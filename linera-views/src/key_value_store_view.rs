use crate::{
    common::{Batch, Context, KeyValueOperations},
    views::{View, ViewError},
};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::{collections::BTreeMap, fmt::Debug, mem};

use crate::common::ContextFromDb;
use crate::memory::MemoryContext;
use tokio::sync::OwnedMutexGuard;
use crate::memory::MemoryStoreMap;

/// A view that represents the KeyValueOperations
#[derive(Debug, Clone)]
pub struct KeyValueStoreView<C> {
    context: C,
    was_reset_to_default: bool,
    updates: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

#[async_trait]
impl<C> View<C> for KeyValueStoreView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Ok(Self {
            context,
            was_reset_to_default: false,
            updates: BTreeMap::new(),
        })
    }

    fn rollback(&mut self) {
        self.was_reset_to_default = false;
        self.updates.clear();
    }

    async fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_reset_to_default {
            self.was_reset_to_default = false;
            self.delete_entries(batch).await?;
            for (index, update) in mem::take(&mut self.updates) {
                if let Some(value) = update {
                    batch.put_key_value_u8(self.context.derive_key_u8(&index), value);
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                match update {
                    None => batch.delete_key(self.context.derive_key_u8(&index)),
                    Some(value) => batch.put_key_value_u8(self.context.derive_key_u8(&index), value),
                }
            }
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.delete_entries(batch).await?;
        Ok(())
    }

    fn clear(&mut self) {
        self.was_reset_to_default = true;
        self.updates.clear();
    }
}

impl<C> KeyValueStoreView<C>
where
    C: Send + Context,
    ViewError: From<C::Error>,
{
    async fn delete_entries(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        let base = self.context.base_key();
        for key in self.context.find_keys_with_prefix(&base).await? {
            batch.delete_key(key);
        }
        Ok(())
    }

    pub fn new(context: C) -> Self {
        Self {
            context,
            was_reset_to_default: false,
            updates: BTreeMap::new(),
        }
    }
}

#[async_trait]
impl<C> KeyValueOperations for KeyValueStoreView<C>
where
    C: Context + Sync + Send,
    ViewError: std::convert::From<C::Error>,
{
    type Error = ViewError;
    async fn read_key<V: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<V>, ViewError> {
        if let Some(update) = self.updates.get(key) {
            match update.as_ref() {
                Some(val) => {
                    let val = bcs::from_bytes(val)?;
                    return Ok(Some(val))
                },
                None => { return Ok(None) },
            }
        }
	if self.was_reset_to_default {
            return Ok(None);
        }
        let val = self.context.read_key(&key.to_vec()).await?;
        Ok(val)
    }

    async fn find_keys_with_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, ViewError> {
        let len = self.context.base_key().len();
        let key_prefix = self.context.derive_key_u8(key_prefix);
        let mut keys = Vec::new();
        if !self.was_reset_to_default {
            for key in self.context.find_keys_with_prefix(&key_prefix).await? {
                if !self.updates.contains_key(&key) {
                    let key = &key[len..];
                    keys.push(key.to_vec())
                }
            }
        }
        for (key, value) in &self.updates {
            if value.is_some() {
                keys.push(key.to_vec())
            }
        }
        Ok(keys)
    }

    async fn get_sub_keys<Key: DeserializeOwned + Send>(
        &mut self,
        key_prefix: &[u8],
    ) -> Result<Vec<Key>, ViewError> {
        let len1 = key_prefix.len();
        let key_prefix = self.context.derive_key_u8(key_prefix);
        let len2 = key_prefix.len();
        let mut keys = Vec::new();
        if !self.was_reset_to_default {
            for key in self.context.find_keys_with_prefix(&key_prefix).await? {
                if !self.updates.contains_key(&key) {
                    keys.push(bcs::from_bytes(&key[len2..])?);
                }
            }
        }
        for (key, value) in &self.updates {
            if value.is_some() {
                keys.push(bcs::from_bytes(&key[len1..])?);
            }
        }
        Ok(keys)
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), ViewError> {
        self.context.write_batch(batch).await?;
        Ok(())
    }
}




/// A context that stores all values in memory.
pub type KeyValueStoreContext = ContextFromDb<usize, KeyValueStoreView<MemoryContext<usize>>>;

impl KeyValueStoreContext {
    pub fn new(guard: OwnedMutexGuard<MemoryStoreMap>, base_key: Vec<u8>, extra: usize) -> Self {
        let context = MemoryContext::new(guard, extra);
        let key_value_store_view = KeyValueStoreView::new(context);
        Self { db: key_value_store_view, base_key, extra }
    }
}

