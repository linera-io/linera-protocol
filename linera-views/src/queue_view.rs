use crate::{
    common::{Batch, Context},
    views::{HashView, Hasher, HashingContext, View, ViewError},
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::VecDeque, fmt::Debug, mem, ops::Range};

/// A view that supports a FIFO queue for values of type `T`.
#[derive(Debug, Clone)]
pub struct QueueView<C, T> {
    context: C,
    stored_indices: Range<usize>,
    front_delete_count: usize,
    new_back_values: VecDeque<T>,
}

/// The context operations supporting [`QueueView`].
#[async_trait]
pub trait QueueOperations<T>: Context {
    /// Obtain the range of indices in the stored queue.
    async fn indices(&mut self) -> Result<Range<usize>, Self::Error>;

    /// Obtain the value at the given index.
    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error>;

    /// Obtain the values in the given range.
    async fn read(&self, range: Range<usize>) -> Result<Vec<T>, Self::Error>;

    /// Delete `count` values from the front of the queue. Crash-resistant implementations
    /// should only write to `batch`.
    fn delete_front(
        &mut self,
        stored_indices: &mut Range<usize>,
        batch: &mut Batch,
        count: usize,
    ) -> Result<(), Self::Error>;

    /// Append the given values from the back of the queue. Crash-resistant
    /// implementations should only write to `batch`.
    fn append_back(
        &self,
        stored_indices: &mut Range<usize>,
        batch: &mut Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error>;

    /// Delete the queue from storage. Crash-resistant implementations should only write to `batch`.
    fn delete(&self, stored_indices: Range<usize>, batch: &mut Batch) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T, C: Context + Send + Sync> QueueOperations<T> for C
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn indices(&mut self) -> Result<Range<usize>, Self::Error> {
        let base = self.base_key();
        let range = self.read_key(&base).await?.unwrap_or_default();
        Ok(range)
    }

    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error> {
        let key = self.derive_key(&index)?;
        Ok(self.read_key(&key).await?)
    }

    async fn read(&self, range: Range<usize>) -> Result<Vec<T>, Self::Error> {
        let mut values = Vec::with_capacity(range.len());
        for index in range {
            let key = self.derive_key(&index)?;
            match self.read_key(&key).await? {
                None => return Ok(values),
                Some(value) => values.push(value),
            }
        }
        Ok(values)
    }

    fn delete_front(
        &mut self,
        stored_indices: &mut Range<usize>,
        batch: &mut Batch,
        count: usize,
    ) -> Result<(), Self::Error> {
        if count == 0 {
            return Ok(());
        }
        let deletion_range = stored_indices.clone().take(count);
        stored_indices.start += count;
        batch.put_key_value(self.base_key(), &stored_indices)?;
        for index in deletion_range {
            let key = self.derive_key(&index)?;
            batch.delete_key(key);
        }
        Ok(())
    }

    fn append_back(
        &self,
        stored_indices: &mut Range<usize>,
        batch: &mut Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error> {
        if values.is_empty() {
            return Ok(());
        }
        for value in values {
            let key = self.derive_key(&stored_indices.end)?;
            batch.put_key_value(key, &value)?;
            stored_indices.end += 1;
        }
        let base = self.base_key();
        batch.put_key_value(base, &stored_indices)?;
        Ok(())
    }

    fn delete(&self, stored_indices: Range<usize>, batch: &mut Batch) -> Result<(), Self::Error> {
        batch.delete_key_prefix(self.base_key());
        Ok(())
    }
}

#[async_trait]
impl<C, T> View<C> for QueueView<C, T>
where
    C: QueueOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(mut context: C) -> Result<Self, ViewError> {
        let stored_indices = context.indices().await?;
        Ok(Self {
            context,
            stored_indices,
            front_delete_count: 0,
            new_back_values: VecDeque::new(),
        })
    }

    fn rollback(&mut self) {
        self.front_delete_count = 0;
        self.new_back_values.clear();
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.front_delete_count > 0 {
            self.context
                .delete_front(&mut self.stored_indices, batch, self.front_delete_count)?;
        }
        if !self.new_back_values.is_empty() {
            self.context.append_back(
                &mut self.stored_indices,
                batch,
                mem::take(&mut self.new_back_values).into_iter().collect(),
            )?;
        }
        self.front_delete_count = 0;
        Ok(())
    }

    fn delete(mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.context.delete(self.stored_indices, batch)?;
        Ok(())
    }

    fn clear(&mut self) {
        self.front_delete_count = self.stored_indices.len();
        self.new_back_values.clear();
    }
}

impl<C, T> QueueView<C, T>
where
    C: QueueOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone,
{
    /// Read the front value, if any.
    pub async fn front(&mut self) -> Result<Option<T>, ViewError> {
        let stored_remainder = self.stored_indices.len() - self.front_delete_count;
        let value = if stored_remainder > 0 {
            self.context
                .get(self.stored_indices.end - stored_remainder)
                .await?
        } else {
            self.new_back_values.front().cloned()
        };
        Ok(value)
    }

    /// Read the back value, if any.
    pub async fn back(&mut self) -> Result<Option<T>, ViewError> {
        let value = match self.new_back_values.back() {
            Some(value) => Some(value.clone()),
            None if self.stored_indices.len() > self.front_delete_count => {
                self.context.get(self.stored_indices.end - 1).await?
            }
            _ => None,
        };
        Ok(value)
    }

    /// Delete the front value, if any.
    pub fn delete_front(&mut self) {
        if self.front_delete_count < self.stored_indices.len() {
            self.front_delete_count += 1;
        } else {
            self.new_back_values.pop_front();
        }
    }

    /// Push a value to the end of the queue.
    pub fn push_back(&mut self, value: T) {
        self.new_back_values.push_back(value);
    }

    /// Read the size of the queue.
    pub fn count(&self) -> usize {
        self.stored_indices.len() - self.front_delete_count + self.new_back_values.len()
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }

    /// Read the `count` next values in the queue (including staged ones).
    pub async fn read_front(&mut self, mut count: usize) -> Result<Vec<T>, ViewError> {
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
            values.extend(self.context.read(start..(start + count)).await?);
        } else {
            values.extend(self.context.read(start..self.stored_indices.end).await?);
            values.extend(
                self.new_back_values
                    .range(0..(count - stored_remainder))
                    .cloned(),
            );
        }
        Ok(values)
    }

    /// Read the `count` last values in the queue (including staged ones).
    pub async fn read_back(&mut self, mut count: usize) -> Result<Vec<T>, ViewError> {
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
            values.extend(self.context.read(start..self.stored_indices.end).await?);
            values.extend(self.new_back_values.iter().cloned());
        }
        Ok(values)
    }
}

#[async_trait]
impl<C, T> HashView<C> for QueueView<C, T>
where
    C: HashingContext + QueueOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize,
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, ViewError> {
        let count = self.count();
        let elements = self.read_front(count).await?;
        let mut hasher = C::Hasher::default();
        hasher.update_with_bcs_bytes(&elements)?;
        Ok(hasher.finalize())
    }
}
