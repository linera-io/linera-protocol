// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::{Context, HasherOutput, MIN_VIEW_TAG},
    views::{HashableView, Hasher, View, ViewError},
};
use async_lock::Mutex;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::{vec_deque::IterMut, VecDeque},
    fmt::Debug,
    ops::Range,
};

/// Key tags to create the sub-keys of a QueueView on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the storing of the variable stored_count
    Store = MIN_VIEW_TAG,
    /// Prefix for the indices of the log
    Index,
    /// Prefix for the hash
    Hash,
}

/// A view that supports a FIFO queue for values of type `T`.
#[derive(Debug)]
pub struct QueueView<C, T> {
    context: C,
    stored_indices: Range<usize>,
    front_delete_count: usize,
    new_back_values: VecDeque<T>,
    stored_hash: Option<HasherOutput>,
    hash: Mutex<Option<HasherOutput>>,
}

#[async_trait]
impl<C, T> View<C> for QueueView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Serialize,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let key = context.base_tag(KeyTag::Store as u8);
        let stored_indices = context.read_key(&key).await?.unwrap_or_default();
        let key = context.base_tag(KeyTag::Hash as u8);
        let hash = context.read_key(&key).await?;
        Ok(Self {
            context,
            stored_indices,
            front_delete_count: 0,
            new_back_values: VecDeque::new(),
            stored_hash: hash,
            hash: Mutex::new(hash),
        })
    }

    fn rollback(&mut self) {
        self.front_delete_count = 0;
        self.new_back_values.clear();
        *self.hash.get_mut() = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        let mut save_stored_indices = false;
        if self.front_delete_count > 0 {
            let deletion_range = self.stored_indices.clone().take(self.front_delete_count);
            self.stored_indices.start += self.front_delete_count;
            save_stored_indices = true;
            for index in deletion_range {
                let key = self.context.derive_tag_key(KeyTag::Index as u8, &index)?;
                batch.delete_key(key);
            }
        }
        if !self.new_back_values.is_empty() {
            for value in &self.new_back_values {
                let key = self
                    .context
                    .derive_tag_key(KeyTag::Index as u8, &self.stored_indices.end)?;
                batch.put_key_value(key, value)?;
                self.stored_indices.end += 1;
            }
            save_stored_indices = true;
            self.new_back_values.clear();
        }
        if save_stored_indices {
            let key = self.context.base_tag(KeyTag::Store as u8);
            batch.put_key_value(key, &self.stored_indices)?;
        }
        self.front_delete_count = 0;
        let hash = *self.hash.get_mut();
        if self.stored_hash != hash {
            let key = self.context.base_tag(KeyTag::Hash as u8);
            match hash {
                None => batch.delete_key(key),
                Some(hash) => batch.put_key_value(key, &hash)?,
            }
            self.stored_hash = hash;
        }
        Ok(())
    }

    fn delete(self, batch: &mut Batch) {
        batch.delete_key_prefix(self.context.base_key());
    }

    fn clear(&mut self) {
        self.front_delete_count = self.stored_indices.len();
        self.new_back_values.clear();
        *self.hash.get_mut() = None;
    }
}

impl<'a, C, T> QueueView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    async fn get(&self, index: usize) -> Result<Option<T>, ViewError> {
        let key = self.context.derive_tag_key(KeyTag::Index as u8, &index)?;
        Ok(self.context.read_key(&key).await?)
    }

    /// Read the front value, if any.
    pub async fn front(&self) -> Result<Option<T>, ViewError> {
        let stored_remainder = self.stored_indices.len() - self.front_delete_count;
        let value = if stored_remainder > 0 {
            self.get(self.stored_indices.end - stored_remainder).await?
        } else {
            self.new_back_values.front().cloned()
        };
        Ok(value)
    }

    /// Read the back value, if any.
    pub async fn back(&self) -> Result<Option<T>, ViewError> {
        let value = match self.new_back_values.back() {
            Some(value) => Some(value.clone()),
            None if self.stored_indices.len() > self.front_delete_count => {
                self.get(self.stored_indices.end - 1).await?
            }
            _ => None,
        };
        Ok(value)
    }

    /// Delete the front value, if any.
    pub fn delete_front(&mut self) {
        *self.hash.get_mut() = None;
        if self.front_delete_count < self.stored_indices.len() {
            self.front_delete_count += 1;
        } else {
            self.new_back_values.pop_front();
        }
    }

    /// Push a value to the end of the queue.
    pub fn push_back(&mut self, value: T) {
        *self.hash.get_mut() = None;
        self.new_back_values.push_back(value);
    }

    /// Read the size of the queue.
    pub fn count(&self) -> usize {
        self.stored_indices.len() - self.front_delete_count + self.new_back_values.len()
    }

    /// Obtain the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }

    async fn read_context(&self, range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let mut values = Vec::with_capacity(range.len());
        for index in range {
            let key = self.context.derive_tag_key(KeyTag::Index as u8, &index)?;
            match self.context.read_key(&key).await? {
                None => return Ok(values),
                Some(value) => values.push(value),
            }
        }
        Ok(values)
    }

    /// Read the `count` next values in the queue (including staged ones).
    pub async fn read_front(&self, mut count: usize) -> Result<Vec<T>, ViewError> {
        if count > self.count() {
            count = self.count();
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut values = Vec::new();
        values.reserve(count);
        let stored_remainder = self.stored_indices.len() - self.front_delete_count;
        let start = self.stored_indices.end - stored_remainder;
        if count <= stored_remainder {
            values.extend(self.read_context(start..(start + count)).await?);
        } else {
            values.extend(self.read_context(start..self.stored_indices.end).await?);
            values.extend(
                self.new_back_values
                    .range(0..(count - stored_remainder))
                    .cloned(),
            );
        }
        Ok(values)
    }

    /// Read the `count` last values in the queue (including staged ones).
    pub async fn read_back(&self, mut count: usize) -> Result<Vec<T>, ViewError> {
        if count > self.count() {
            count = self.count();
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut values = Vec::new();
        values.reserve(count);
        let new_back_len = self.new_back_values.len();
        if count <= new_back_len {
            values.extend(
                self.new_back_values
                    .range((new_back_len - count)..new_back_len)
                    .cloned(),
            );
        } else {
            let start = self.stored_indices.end + new_back_len - count;
            values.extend(self.read_context(start..self.stored_indices.end).await?);
            values.extend(self.new_back_values.iter().cloned());
        }
        Ok(values)
    }

    /// Read all the elements
    pub async fn elements(&self) -> Result<Vec<T>, ViewError> {
        let count = self.count();
        self.read_front(count).await
    }

    async fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        let count = self.count();
        let elements = self.read_front(count).await?;
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bcs_bytes(&elements)?;
        Ok(hasher.finalize())
    }

    async fn load_all(&mut self) -> Result<(), ViewError> {
        let stored_remainder = self.stored_indices.len() - self.front_delete_count;
        let start = self.stored_indices.end - stored_remainder;
        let elements = self.read_context(start..self.stored_indices.end).await?;
        let shift = self.stored_indices.end - start;
        for elt in elements {
            self.new_back_values.push_back(elt);
        }
        self.new_back_values.rotate_right(shift);
        self.front_delete_count = self.stored_indices.len();
        Ok(())
    }

    /// Get a mutable iterator on the entries of the queue
    pub async fn iter_mut(&'a mut self) -> Result<IterMut<'a, T>, ViewError> {
        self.load_all().await?;
        Ok(self.new_back_values.iter_mut())
    }
}

#[async_trait]
impl<C, T> HashableView<C> for QueueView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        let hash = *self.hash.get_mut();
        match hash {
            Some(hash) => Ok(hash),
            None => {
                let new_hash = self.compute_hash().await?;
                let hash = self.hash.get_mut();
                *hash = Some(new_hash);
                Ok(new_hash)
            }
        }
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        let mut hash = self.hash.lock().await;
        match *hash {
            Some(hash) => Ok(hash),
            None => {
                let new_hash = self.compute_hash().await?;
                *hash = Some(new_hash);
                Ok(new_hash)
            }
        }
    }
}
