use crate::{
    common::{Batch, Context, HashOutput},
    views::{HashView, Hasher, View, ViewError},
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, ops::Range};

/// prefix used.
///
/// 0 : for the storing of the variable stored_count
/// 1 : for the indices of the log
/// 2 : for the hash
enum KeyTag {
    Store = 0,
    Index = 1,
    Hash = 2,
}

/// A view that supports logging values of type `T`.
#[derive(Debug)]
pub struct LogView<C, T> {
    context: C,
    was_cleared: bool,
    stored_count: usize,
    new_values: Vec<T>,
    stored_hash: Option<HashOutput>,
    hash: Option<HashOutput>,
}

#[async_trait]
impl<C, T> View<C> for LogView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let key = context.base_tag(KeyTag::Store as u8);
        let stored_count = context.read_key(&key).await?.unwrap_or_default();
        let key = context.base_tag(KeyTag::Hash as u8);
        let hash = context.read_key(&key).await?;
        Ok(Self {
            context,
            was_cleared: false,
            stored_count,
            new_values: Vec::new(),
            stored_hash: hash,
            hash,
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.new_values.clear();
        self.hash = self.stored_hash;
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
            for value in &self.new_values {
                let key = self
                    .context
                    .derive_tag_key(KeyTag::Index as u8, &self.stored_count)?;
                batch.put_key_value(key, value)?;
                self.stored_count += 1;
            }
            let key = self.context.base_tag(KeyTag::Store as u8);
            batch.put_key_value(key, &self.stored_count)?;
            self.new_values.clear();
        }
        if self.stored_hash != self.hash {
            let key = self.context.base_tag(KeyTag::Hash as u8);
            match self.hash {
                None => batch.delete_key(key),
                Some(hash) => batch.put_key_value(key, &hash)?,
            }
            self.stored_hash = self.hash;
        }
        Ok(())
    }

    fn delete(self, batch: &mut Batch) {
        batch.delete_key_prefix(self.context.base_key());
    }

    fn clear(&mut self) {
        self.was_cleared = true;
        self.new_values.clear();
        self.hash = None;
    }
}

impl<C, T> LogView<C, T>
where
    C: Context,
{
    /// Push a value to the end of the log.
    pub fn push(&mut self, value: T) {
        self.new_values.push(value);
        self.hash = None;
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
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + DeserializeOwned,
{
    /// Read the logged values in the given range (including staged ones).
    pub async fn get(&self, index: usize) -> Result<Option<T>, ViewError> {
        let value = if self.was_cleared {
            self.new_values.get(index).cloned()
        } else if index < self.stored_count {
            let key = self.context.derive_tag_key(KeyTag::Index as u8, &index)?;
            self.context.read_key(&key).await?
        } else {
            self.new_values.get(index - self.stored_count).cloned()
        };
        Ok(value)
    }

    async fn read_context(&self, range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let mut values = Vec::with_capacity(range.len());
        for index in range {
            let key = self.context.derive_tag_key(KeyTag::Index as u8, &index)?;
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
                let elements = self.read(0..count).await?;
                let mut hasher = Self::Hasher::default();
                hasher.update_with_bcs_bytes(&elements)?;
                let hash = hasher.finalize();
                self.hash = Some(hash);
                Ok(hash)
            }
        }
    }
}
