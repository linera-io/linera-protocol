use crate::{
    common::{Batch, Context},
    views::{HashView, Hasher, HashingContext, View, ViewError},
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, ops::Range};

/// A view that supports logging values of type `T`.
#[derive(Debug, Clone)]
pub struct LogView<C, T> {
    context: C,
    was_cleared: bool,
    stored_count: usize,
    new_values: Vec<T>,
}

/// The context operations supporting [`LogView`].
#[async_trait]
pub trait LogOperations<T>: Context {
}

#[async_trait]
impl<T, C: Context + Send + Sync> LogOperations<T> for C
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
}

#[async_trait]
impl<C, T> View<C> for LogView<C, T>
where
    C: LogOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let base = context.base_key();
        let stored_count = context.read_key(&base).await?.unwrap_or_default();
        Ok(Self {
            context,
            was_cleared: false,
            stored_count,
            new_values: Vec::new(),
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.new_values.clear();
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_cleared {
            self.was_cleared = false;
            if self.stored_count > 0 {
                batch.delete_key_prefix(self.context.base_key());
                self.stored_count = 0;
            }
        }
        if !self.new_values.is_empty() {
            let count = self.new_values.len();
            if count > 0 {
                for value in &self.new_values {
                    batch.put_key_value(self.context.derive_key(&self.stored_count)?, value)?;
                    self.stored_count += 1;
                }
                batch.put_key_value(self.context.base_key(), &self.stored_count)?;
                self.new_values.clear();
            }
        }
        Ok(())
    }

    fn delete(self, batch: &mut Batch) {
        batch.delete_key_prefix(self.context.base_key());
    }

    fn clear(&mut self) {
        self.was_cleared = true;
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
        if self.was_cleared {
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
    T: Send + Sync + Clone + DeserializeOwned,
{
    /// Read the logged values in the given range (including staged ones).
    pub async fn get(&mut self, index: usize) -> Result<Option<T>, ViewError> {
        let value = if self.was_cleared {
            self.new_values.get(index).cloned()
        } else if index < self.stored_count {
            let key = self.context.derive_key(&index)?;
            self.context.read_key(&key).await?
        } else {
            self.new_values.get(index - self.stored_count).cloned()
        };
        Ok(value)
    }

    async fn read_context(&self, range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let mut values = Vec::with_capacity(range.len());
        for index in range {
            let key = self.context.derive_key(&index)?;
            match self.context.read_key(&key).await? {
                None => return Ok(values),
                Some(value) => values.push(value),
            };
        }
        Ok(values)
    }
    /// Read the logged values in the given range (including staged ones).
    pub async fn read(&self, mut range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let effective_stored_count = if self.was_cleared {
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
                values.extend(self.read_context(range.start..range.end).await?);
            } else {
                values.extend(
                    self.read_context(range.start..effective_stored_count)
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

#[async_trait]
impl<C, T> HashView<C> for LogView<C, T>
where
    C: HashingContext + LogOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, ViewError> {
        let count = self.count();
        let elements = self.read(0..count).await?;
        let mut hasher = C::Hasher::default();
        hasher.update_with_bcs_bytes(&elements)?;
        Ok(hasher.finalize())
    }
}
