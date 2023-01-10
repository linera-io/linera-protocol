// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    common::{
        get_upper_bound, Batch, Context, ContextFromDb, HashOutput, KeyValueOperations,
        SimpleTypeIterator, WriteOperation,
    },
    memory::{MemoryContext, MemoryStoreMap},
    views::{HashableView, Hasher, View, ViewError},
};
use async_trait::async_trait;
use std::{collections::BTreeMap, fmt::Debug, mem, ops::Bound::Included};
use tokio::sync::OwnedMutexGuard;

/// Key tags to create the sub-keys of a KeyValueStoreView on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the indices of the mapview
    Index = 0,
    /// Prefix for the hash
    Hash = 1,
}

/// A view that represents the KeyValueOperations
#[derive(Debug, Clone)]
pub struct KeyValueStoreView<C> {
    context: C,
    was_cleared: bool,
    updates: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    stored_hash: Option<HashOutput>,
    hash: Option<HashOutput>,
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
        let key = context.base_tag(KeyTag::Hash as u8);
        let hash = context.read_key(&key).await?;
        Ok(Self {
            context,
            was_cleared: false,
            updates: BTreeMap::new(),
            stored_hash: hash,
            hash,
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.updates.clear();
        self.hash = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_cleared {
            self.was_cleared = false;
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(&mut self.updates) {
                if let Some(value) = update {
                    let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                    batch.put_key_value_bytes(key, value);
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                match update {
                    None => batch.delete_key(key),
                    Some(value) => batch.put_key_value_bytes(key, value),
                }
            }
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
        self.updates.clear();
        self.hash = None;
    }
}

impl<C> KeyValueStoreView<C>
where
    C: Send + Context,
    ViewError: From<C::Error>,
{
    pub fn new(context: C) -> Self {
        Self {
            context,
            was_cleared: false,
            updates: BTreeMap::new(),
            stored_hash: None,
            hash: None,
        }
    }

    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(Vec<u8>) -> Result<(), ViewError> + Send,
    {
        let key_prefix = self.context.base_tag(KeyTag::Index as u8);
        let mut iter = self.updates.iter();
        let mut pair = iter.next();
        if !self.was_cleared {
            for index in self
                .context
                .find_stripped_keys_by_prefix(&key_prefix)
                .await?
            {
                loop {
                    match pair {
                        Some((key, value)) => {
                            let key = key.clone();
                            if key < index {
                                if value.is_some() {
                                    f(key)?;
                                }
                                pair = iter.next();
                            } else {
                                if key != index {
                                    f(index)?;
                                } else if value.is_some() {
                                    f(key)?;
                                    pair = iter.next();
                                }
                                break;
                            }
                        }
                        None => {
                            f(index)?;
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = pair {
            let key = key.clone();
            if value.is_some() {
                f(key)?;
            }
            pair = iter.next();
        }
        Ok(())
    }

    pub async fn for_each_index_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(Vec<u8>, Vec<u8>) -> Result<(), ViewError> + Send,
    {
        let key_prefix = self.context.base_tag(KeyTag::Index as u8);
        let mut iter = self.updates.iter();
        let mut pair = iter.next();
        if !self.was_cleared {
            for (index, index_val) in self
                .context
                .find_stripped_key_values_by_prefix(&key_prefix)
                .await?
            {
                loop {
                    match pair {
                        Some((key, value)) => {
                            let key = key.clone();
                            if key < index {
                                if let Some(value) = value {
                                    f(key, value.to_vec())?;
                                }
                                pair = iter.next();
                            } else {
                                if key != index {
                                    f(index, index_val)?;
                                } else if let Some(value) = value {
                                    f(key, value.to_vec())?;
                                    pair = iter.next();
                                }
                                break;
                            }
                        }
                        None => {
                            f(index, index_val)?;
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = pair {
            let key = key.clone();
            if let Some(value) = value {
                f(key, value.to_vec())?;
            }
            pair = iter.next();
        }
        Ok(())
    }

    pub async fn indices(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index: Vec<u8>| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    pub async fn get(&self, index: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        if let Some(update) = self.updates.get(index) {
            return Ok(update.as_ref().cloned());
        }
        if self.was_cleared {
            return Ok(None);
        }
        let key = self.context.base_tag_index(KeyTag::Index as u8, index);
        let value = self.context.read_key_bytes(&key).await?;
        Ok(value)
    }

    /// Set or insert a value.
    pub fn insert(&mut self, index: Vec<u8>, value: Vec<u8>) {
        self.updates.insert(index, Some(value));
        self.hash = None;
    }

    /// Remove a value.
    pub fn remove(&mut self, index: Vec<u8>) {
        self.hash = None;
        if self.was_cleared {
            self.updates.remove(&index);
        } else {
            self.updates.insert(index, None);
        }
    }
}

#[async_trait]
impl<C> KeyValueOperations for KeyValueStoreView<C>
where
    C: Context + Sync + Send,
    ViewError: From<C::Error>,
{
    type Error = ViewError;
    type KeyIterator = SimpleTypeIterator<Vec<u8>, ViewError>;
    type KeyValueIterator = SimpleTypeIterator<(Vec<u8>, Vec<u8>), ViewError>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        if let Some(update) = self.updates.get(key) {
            return Ok(update.clone());
        }
        if self.was_cleared {
            return Ok(None);
        }
        let key = self.context.base_tag_index(KeyTag::Index as u8, key);
        let val = self.context.read_key_bytes(&key).await?;
        Ok(val)
    }

    async fn find_stripped_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyIterator, ViewError> {
        let len = key_prefix.len();
        let key_prefix_full = self.context.base_tag_index(KeyTag::Index as u8, key_prefix);
        let mut keys = Vec::new();
        let key_prefix_upper = get_upper_bound(key_prefix);
        let mut iter = self
            .updates
            .range((Included(key_prefix.to_vec()), key_prefix_upper));
        let mut pair = iter.next();
        if !self.was_cleared {
            for stripped_key in self
                .context
                .find_stripped_keys_by_prefix(&key_prefix_full)
                .await?
            {
                loop {
                    match pair {
                        Some((key_update, value_update)) => {
                            let stripped_key_update = key_update[len..].to_vec();
                            if stripped_key_update < stripped_key {
                                if value_update.is_some() {
                                    keys.push(stripped_key_update);
                                }
                                pair = iter.next();
                            } else {
                                if stripped_key != stripped_key_update {
                                    keys.push(stripped_key.to_vec());
                                } else if value_update.is_some() {
                                    keys.push(stripped_key.to_vec());
                                    pair = iter.next();
                                }
                                break;
                            }
                        }
                        None => {
                            keys.push(stripped_key.to_vec());
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key_update, value_update)) = pair {
            if value_update.is_some() {
                let stripped_key_update = key_update[len..].to_vec();
                keys.push(stripped_key_update);
            }
            pair = iter.next();
        }
        Ok(Self::KeyIterator::new(keys))
    }

    async fn find_stripped_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValueIterator, ViewError> {
        let len = key_prefix.len();
        let key_prefix_full = self.context.base_tag_index(KeyTag::Index as u8, key_prefix);
        let mut key_values = Vec::new();
        let key_prefix_upper = get_upper_bound(key_prefix);
        let mut iter = self
            .updates
            .range((Included(key_prefix.to_vec()), key_prefix_upper));
        let mut pair = iter.next();
        if !self.was_cleared {
            for (stripped_key, value) in self
                .context
                .find_stripped_key_values_by_prefix(&key_prefix_full)
                .await?
            {
                loop {
                    match pair {
                        Some((key_update, value_update)) => {
                            let stripped_key_update = key_update[len..].to_vec();
                            if stripped_key_update < stripped_key {
                                if let Some(value_update) = value_update {
                                    let key_value = (stripped_key_update, value_update.to_vec());
                                    key_values.push(key_value);
                                }
                                pair = iter.next();
                            } else {
                                if stripped_key != stripped_key_update {
                                    key_values.push((stripped_key.to_vec(), value.clone()));
                                } else if let Some(value_update) = value_update {
                                    key_values.push((stripped_key.to_vec(), value_update.to_vec()));
                                    pair = iter.next();
                                }
                                break;
                            }
                        }
                        None => {
                            key_values.push((stripped_key.to_vec(), value.clone()));
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key_update, value_update)) = pair {
            if let Some(value_update) = value_update {
                let key_value = (key_update[len..].to_vec(), value_update.to_vec());
                key_values.push(key_value);
            }
            pair = iter.next();
        }
        Ok(Self::KeyValueIterator::new(key_values))
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), ViewError> {
        let mut batch_new = Batch::default();
        for op in batch.operations {
            match op {
                WriteOperation::Delete { key } => {
                    let key = self.context.base_tag_index(KeyTag::Index as u8, &key);
                    batch_new.delete_key(key);
                }
                WriteOperation::Put { key, value } => {
                    let key = self.context.base_tag_index(KeyTag::Index as u8, &key);
                    batch_new.put_key_value_bytes(key, value);
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    let key_prefix = self
                        .context
                        .base_tag_index(KeyTag::Index as u8, &key_prefix);
                    batch_new.delete_key_prefix(key_prefix);
                }
            }
        }
        self.context.write_batch(batch_new).await?;
        Ok(())
    }
}

#[async_trait]
impl<C> HashableView<C> for KeyValueStoreView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    type Hasher = sha2::Sha512;

    async fn hash(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        match self.hash {
            Some(hash) => Ok(hash),
            None => {
                let mut hasher = Self::Hasher::default();
                let mut count = 0;
                self.for_each_index_value(
                    |index: Vec<u8>, value: Vec<u8>| -> Result<(), ViewError> {
                        count += 1;
                        hasher.update_with_bytes(&index)?;
                        hasher.update_with_bytes(&value)?;
                        Ok(())
                    },
                )
                .await?;
                hasher.update_with_bcs_bytes(&count)?;
                let hash = hasher.finalize();
                self.hash = Some(hash);
                Ok(hash)
            }
        }
    }
}

/// A context that stores all values in memory.
#[cfg(any(test, feature = "test"))]
pub type KeyValueStoreMemoryContext<E> = ContextFromDb<E, KeyValueStoreView<MemoryContext<()>>>;

#[cfg(any(test, feature = "test"))]
impl<E> KeyValueStoreMemoryContext<E> {
    pub fn new(guard: OwnedMutexGuard<MemoryStoreMap>, base_key: Vec<u8>, extra: E) -> Self {
        let context = MemoryContext::new(guard, ());
        let key_value_store_view = KeyValueStoreView::new(context);
        Self {
            db: key_value_store_view,
            base_key,
            extra,
        }
    }
}
