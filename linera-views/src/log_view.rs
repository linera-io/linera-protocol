use crate::{
    common::{Batch, Context},
    views::{View, ViewError},
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, mem, ops::Range};

/// A view that supports logging values of type `T`.
#[derive(Debug, Clone)]
pub struct LogView<C, T> {
    context: C,
    was_reset_to_default: bool,
    stored_count: usize,
    new_values: Vec<T>,
}

/// The context operations supporting [`LogView`].
#[async_trait]
pub trait LogOperations<T>: Context {
    /// Return the size of the log in storage.
    async fn count(&self) -> Result<usize, Self::Error>;

    /// Obtain the value at the given index.
    async fn get(&self, index: usize) -> Result<Option<T>, Self::Error>;

    /// Obtain the values in the given range of indices.
    async fn read(&self, range: Range<usize>) -> Result<Vec<T>, Self::Error>;

    /// Append values to the logs. Crash-resistant implementations should only write to `batch`.
    fn append(
        &self,
        stored_count: usize,
        batch: &mut Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error>;

    /// Delete the log. Crash-resistant implementations should only write to `batch`.
    /// The stored_count is an invariant of the structure. It is a leaky abstraction
    /// but allows a priori better performance.
    fn delete(&self, stored_count: usize, batch: &mut Batch) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T, C: Context + Send + Sync> LogOperations<T> for C
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn count(&self) -> Result<usize, Self::Error> {
        let base = self.base_key();
        let count = self.read_key(&base).await?.unwrap_or_default();
        Ok(count)
    }

    async fn get(&self, index: usize) -> Result<Option<T>, Self::Error> {
        let key = self.derive_key(&index)?;
        self.read_key(&key).await
    }

    async fn read(&self, range: Range<usize>) -> Result<Vec<T>, Self::Error> {
        let mut values = Vec::with_capacity(range.len());
        for index in range {
            let key = self.derive_key(&index)?;
            match self.read_key(&key).await? {
                None => return Ok(values),
                Some(value) => values.push(value),
            };
        }
        Ok(values)
    }

    fn append(
        &self,
        stored_count: usize,
        batch: &mut Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error> {
        if values.is_empty() {
            return Ok(());
        }
        let mut count = stored_count;
        for value in values {
            batch.put_key_value(self.derive_key(&count)?, &value)?;
            count += 1;
        }
        batch.put_key_value(self.base_key(), &count)?;
        Ok(())
    }

    fn delete(&self, stored_count: usize, batch: &mut Batch) -> Result<(), Self::Error> {
        batch.delete_key(self.base_key());
        for index in 0..stored_count {
            batch.delete_key(self.derive_key(&index)?);
        }
        Ok(())
    }
}

#[async_trait]
impl<C, T> View<C> for LogView<C, T>
where
    C: LogOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let stored_count = context.count().await?;
        Ok(Self {
            context,
            was_reset_to_default: false,
            stored_count,
            new_values: Vec::new(),
        })
    }

    fn rollback(&mut self) {
        self.was_reset_to_default = false;
        self.new_values.clear();
    }

    async fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_reset_to_default {
            self.was_reset_to_default = false;
            if self.stored_count > 0 {
                self.context.delete(self.stored_count, batch)?;
                self.stored_count = 0;
            }
        }
        if !self.new_values.is_empty() {
            let count = self.new_values.len();
            self.context
                .append(self.stored_count, batch, mem::take(&mut self.new_values))?;
            self.stored_count += count;
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.context.delete(self.stored_count, batch)?;
        Ok(())
    }

    fn clear(&mut self) {
        self.was_reset_to_default = true;
        self.new_values.clear();
    }
}

impl<C, T> LogView<C, T>
where
    C: Context,
{
    /// Push a value to the end of the log.
    pub fn push(&mut self, value: T) {
        self.new_values.push(value);
    }

    /// Read the size of the log.
    pub fn count(&self) -> usize {
        if self.was_reset_to_default {
            self.new_values.len()
        } else {
            self.stored_count + self.new_values.len()
        }
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, T> LogView<C, T>
where
    C: LogOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone,
{
    /// Read the logged values in the given range (including staged ones).
    pub async fn get(&mut self, index: usize) -> Result<Option<T>, ViewError> {
        let value = if self.was_reset_to_default {
            self.new_values.get(index).cloned()
        } else if index < self.stored_count {
            self.context.get(index).await?
        } else {
            self.new_values.get(index - self.stored_count).cloned()
        };
        Ok(value)
    }

    /// Read the logged values in the given range (including staged ones).
    pub async fn read(&self, mut range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let effective_stored_count = if self.was_reset_to_default {
            0
        } else {
            self.stored_count
        };
        if range.end > self.count() {
            range.end = self.count();
        }
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let mut values = Vec::new();
        values.reserve(range.end - range.start);
        if range.start < effective_stored_count {
            if range.end <= effective_stored_count {
                values.extend(self.context.read(range.start..range.end).await?);
            } else {
                values.extend(
                    self.context
                        .read(range.start..effective_stored_count)
                        .await?,
                );
                values.extend(self.new_values[0..(range.end - effective_stored_count)].to_vec());
            }
        } else {
            values.extend(
                self.new_values
                    [(range.start - effective_stored_count)..(range.end - effective_stored_count)]
                    .to_vec(),
            );
        }
        Ok(values)
    }
}
