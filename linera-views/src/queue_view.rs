use crate::{
    common::{concatenate_base_flag, Batch, Context, HashOutput},
    views::{HashView, Hasher, View, ViewError},
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::VecDeque, fmt::Debug, ops::Range};

/// prefix used.
///
/// 0 : for the storing of the variable stored_count
/// 1 : for the indices of the log
/// 2 : for the hash
const FLAG_STORE: u8 = 0;
const FLAG_INDEX: u8 = 1;
const FLAG_HASH: u8 = 2;

/// A view that supports a FIFO queue for values of type `T`.
#[derive(Debug, Clone)]
pub struct QueueView<C, T> {
    context: C,
    stored_indices: Range<usize>,
    front_delete_count: usize,
    new_back_values: VecDeque<T>,
    hash: Option<HashOutput>,
}

#[async_trait]
impl<C, T> View<C> for QueueView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let key = concatenate_base_flag(context.base_key(), FLAG_STORE);
        let stored_indices = context.read_key(&key).await?.unwrap_or_default();
        let key = concatenate_base_flag(context.base_key(), FLAG_HASH);
        let hash = context.read_key(&key).await?;
        Ok(Self {
            context,
            stored_indices,
            front_delete_count: 0,
            new_back_values: VecDeque::new(),
            hash,
        })
    }

    fn rollback(&mut self) {
        self.front_delete_count = 0;
        self.new_back_values.clear();
        self.hash = None;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.front_delete_count > 0 {
            let deletion_range = self.stored_indices.clone().take(self.front_delete_count);
            self.stored_indices.start += self.front_delete_count;
            let key = concatenate_base_flag(self.context.base_key(), FLAG_STORE);
            batch.put_key_value(key, &self.stored_indices)?;
            for index in deletion_range {
                let key = self.context.derive_flag_key(FLAG_INDEX, &index)?;
                batch.delete_key(key);
            }
        }
        if !self.new_back_values.is_empty() {
            for value in &self.new_back_values {
                let key = self
                    .context
                    .derive_flag_key(FLAG_INDEX, &self.stored_indices.end)?;
                batch.put_key_value(key, value)?;
                self.stored_indices.end += 1;
            }
            let key = concatenate_base_flag(self.context.base_key(), FLAG_STORE);
            batch.put_key_value(key, &self.stored_indices)?;
            self.new_back_values.clear();
        }
        self.front_delete_count = 0;
        Ok(())
    }

    fn delete(self, batch: &mut Batch) {
        batch.delete_key_prefix(self.context.base_key());
    }

    fn clear(&mut self) {
        self.front_delete_count = self.stored_indices.len();
        self.new_back_values.clear();
        self.hash = None;
    }
}

impl<C, T> QueueView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    async fn get(&self, index: usize) -> Result<Option<T>, ViewError> {
        let key = self.context.derive_flag_key(FLAG_INDEX, &index)?;
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
        self.hash = None;
        if self.front_delete_count < self.stored_indices.len() {
            self.front_delete_count += 1;
        } else {
            self.new_back_values.pop_front();
        }
    }

    /// Push a value to the end of the queue.
    pub fn push_back(&mut self, value: T) {
        self.hash = None;
        self.new_back_values.push_back(value);
    }

    /// Read the size of the queue.
    pub fn count(&self) -> usize {
        self.stored_indices.len() - self.front_delete_count + self.new_back_values.len()
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }

    async fn read_context(&self, range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let mut values = Vec::with_capacity(range.len());
        for index in range {
            let key = self.context.derive_flag_key(FLAG_INDEX, &index)?;
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
}

#[async_trait]
impl<C, T> HashView<C> for QueueView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    type Hasher = sha2::Sha512;

    async fn hash(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        match self.hash {
            Some(hash) => Ok(hash),
            None => {
                let count = self.count();
                let elements = self.read_front(count).await?;
                let mut hasher = Self::Hasher::default();
                hasher.update_with_bcs_bytes(&elements)?;
                let hash = hasher.finalize();
                self.hash = Some(hash);
                Ok(hash)
            }
        }
    }
}
