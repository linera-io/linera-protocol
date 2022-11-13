use crate::{
    common::{Batch, Context},
    views::{View, ViewError},
};
use async_trait::async_trait;
//use serde::{de::DeserializeOwned, Serialize};
use std::{collections::BTreeMap, fmt::Debug, mem};

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
}
