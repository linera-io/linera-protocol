// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::{from_bytes_opt, Context, HasherOutput, MIN_VIEW_TAG},
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
    /// Prefix for the storing of the variable stored_indices.
    Store = MIN_VIEW_TAG,
    /// Prefix for the indices of the log.
    Index,
    /// Prefix for the hash.
    Hash,
}

/// A view that supports a FIFO queue for values of type `T`.
#[derive(Debug)]
pub struct QueueView<C, T> {
    context: C,
    stored_indices: Range<usize>,
    front_delete_count: usize,
    was_cleared: bool,
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
        let key1 = context.base_tag(KeyTag::Store as u8);
        let key2 = context.base_tag(KeyTag::Hash as u8);
        let keys = vec![key1, key2];
        let values_bytes = context.read_multi_key_bytes(keys).await?;
        let stored_indices = from_bytes_opt(values_bytes[0].clone())?.unwrap_or_default();
        let hash = from_bytes_opt(values_bytes[1].clone())?;
        Ok(Self {
            context,
            stored_indices,
            front_delete_count: 0,
            was_cleared: false,
            new_back_values: VecDeque::new(),
            stored_hash: hash,
            hash: Mutex::new(hash),
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.front_delete_count = 0;
        self.new_back_values.clear();
        *self.hash.get_mut() = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        let mut save_stored_indices = false;
        if self.stored_count() == 0 {
            let key_prefix = self.context.base_tag(KeyTag::Index as u8);
            batch.delete_key_prefix(key_prefix);
            self.stored_indices = Range::default();
            save_stored_indices = true;
        } else if self.front_delete_count > 0 {
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
        self.was_cleared = false;
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
        self.was_cleared = true;
        self.new_back_values.clear();
        *self.hash.get_mut() = None;
    }
}

impl<C, T> QueueView<C, T> {
    fn stored_count(&self) -> usize {
        if self.was_cleared {
            0
        } else {
            self.stored_indices.len() - self.front_delete_count
        }
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

    /// Reads the front value, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::queue_view::QueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut queue = QueueView::load(context).await.unwrap();
    ///   queue.push_back(34);
    ///   queue.push_back(42);
    ///   assert_eq!(queue.front().await.unwrap(), Some(34));
    /// # })
    /// ```
    pub async fn front(&self) -> Result<Option<T>, ViewError> {
        let stored_remainder = self.stored_count();
        let value = if stored_remainder > 0 {
            self.get(self.stored_indices.end - stored_remainder).await?
        } else {
            self.new_back_values.front().cloned()
        };
        Ok(value)
    }

    /// Reads the back value, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::queue_view::QueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut queue = QueueView::load(context).await.unwrap();
    ///   queue.push_back(34);
    ///   queue.push_back(42);
    ///   assert_eq!(queue.back().await.unwrap(), Some(42));
    /// # })
    /// ```
    pub async fn back(&self) -> Result<Option<T>, ViewError> {
        Ok(match self.new_back_values.back() {
            Some(value) => Some(value.clone()),
            None if self.stored_count() > 0 => self.get(self.stored_indices.end - 1).await?,
            _ => None,
        })
    }

    /// Deletes the front value, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::queue_view::QueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut queue = QueueView::load(context).await.unwrap();
    ///   queue.push_back(34 as u128);
    ///   queue.delete_front();
    ///   assert_eq!(queue.elements().await.unwrap(), Vec::<u128>::new());
    /// # })
    /// ```
    pub fn delete_front(&mut self) {
        *self.hash.get_mut() = None;
        if self.stored_count() > 0 {
            self.front_delete_count += 1;
        } else {
            self.new_back_values.pop_front();
        }
    }

    /// Pushes a value to the end of the queue.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::queue_view::QueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut queue = QueueView::load(context).await.unwrap();
    ///   queue.push_back(34);
    ///   queue.push_back(37);
    ///   assert_eq!(queue.elements().await.unwrap(), vec![34,37]);
    /// # })
    /// ```
    pub fn push_back(&mut self, value: T) {
        *self.hash.get_mut() = None;
        self.new_back_values.push_back(value);
    }

    /// Reads the size of the queue.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::queue_view::QueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut queue = QueueView::load(context).await.unwrap();
    ///   queue.push_back(34);
    ///   assert_eq!(queue.count(), 1);
    /// # })
    /// ```
    pub fn count(&self) -> usize {
        self.stored_count() + self.new_back_values.len()
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }

    async fn read_context(&self, range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let count = range.len();
        let mut keys = Vec::with_capacity(count);
        for index in range {
            let key = self.context.derive_tag_key(KeyTag::Index as u8, &index)?;
            keys.push(key)
        }
        let mut values = Vec::with_capacity(count);
        for entry in self.context.read_multi_key(keys).await? {
            match entry {
                None => {
                    return Err(ViewError::MissingEntries);
                }
                Some(value) => values.push(value),
            }
        }
        Ok(values)
    }

    /// Reads the `count` next values in the queue (including staged ones).
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::queue_view::QueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut queue = QueueView::load(context).await.unwrap();
    ///   queue.push_back(34);
    ///   queue.push_back(42);
    ///   assert_eq!(queue.read_front(1).await.unwrap(), vec![34]);
    /// # })
    /// ```
    pub async fn read_front(&self, mut count: usize) -> Result<Vec<T>, ViewError> {
        if count > self.count() {
            count = self.count();
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut values = Vec::new();
        values.reserve(count);
        if !self.was_cleared {
            let stored_remainder = self.stored_count();
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
        } else {
            values.extend(self.new_back_values.range(0..count).cloned());
        }
        Ok(values)
    }

    /// Reads the `count` last values in the queue (including staged ones).
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::queue_view::QueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut queue = QueueView::load(context).await.unwrap();
    ///   queue.push_back(34);
    ///   queue.push_back(42);
    ///   assert_eq!(queue.read_back(1).await.unwrap(), vec![42]);
    /// # })
    /// ```
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
        if count <= new_back_len || self.was_cleared {
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

    /// Reads all the elements
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::queue_view::QueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut queue = QueueView::load(context).await.unwrap();
    ///   queue.push_back(34);
    ///   queue.push_back(37);
    ///   assert_eq!(queue.elements().await.unwrap(), vec![34,37]);
    /// # })
    /// ```
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
        if !self.was_cleared {
            let stored_remainder = self.stored_count();
            let start = self.stored_indices.end - stored_remainder;
            let elements = self.read_context(start..self.stored_indices.end).await?;
            let shift = self.stored_indices.end - start;
            for elt in elements {
                self.new_back_values.push_back(elt);
            }
            self.new_back_values.rotate_right(shift);
            // All indices are being deleted at the next flush. This is because they are deleted either:
            // * Because a self.front_delete_count forces them to be removed
            // * Or because loading them means that their value can be changed which invalidates
            //   the entries on storage
            self.was_cleared = true;
        }
        Ok(())
    }

    /// Gets a mutable iterator on the entries of the queue
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::queue_view::QueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut queue = QueueView::load(context).await.unwrap();
    ///   queue.push_back(34);
    ///   let mut iter = queue.iter_mut().await.unwrap();
    ///   let value = iter.next().unwrap();
    ///   *value = 42;
    ///   assert_eq!(queue.elements().await.unwrap(), vec![42]);
    /// # })
    /// ```
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
