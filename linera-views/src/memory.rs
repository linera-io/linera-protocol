// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{
    common::{Batch, ContextFromDb, KeyValueOperations, WriteOperation},
    views::ViewError,
};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};
use thiserror::Error;
use tokio::sync::{OwnedMutexGuard, RwLock};

/// The data is serialized in memory just like for rocksdb / dynamodb
/// The analogue of the database is the BTreeMap
pub type MemoryStoreMap = BTreeMap<Vec<u8>, Vec<u8>>;

pub type MemoryContainer = Arc<RwLock<OwnedMutexGuard<MemoryStoreMap>>>;

/// A context that stores all values in memory.
pub type MemoryContext<E> = ContextFromDb<E, MemoryContainer>;

impl<E> MemoryContext<E> {
    pub fn new(guard: OwnedMutexGuard<MemoryStoreMap>, extra: E) -> Self {
        Self {
            db: Arc::new(RwLock::new(guard)),
            base_key: Vec::new(),
            extra,
        }
    }
}

#[async_trait]
impl KeyValueOperations for MemoryContainer {
    type Error = MemoryContextError;
    async fn read_key<V: DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<V>, MemoryContextError> {
        let map = self.read().await;
        match map.get(key) {
            None => Ok(None),
            Some(bytes) => Ok(Some(bcs::from_bytes(bytes)?)),
        }
    }

    async fn find_keys_with_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, MemoryContextError> {
        let map = self.read().await;
        let mut vals = Vec::new();
        for key in map.keys() {
            if key.starts_with(key_prefix) {
                vals.push(key.clone())
            }
        }
        Ok(vals)
    }

    async fn get_sub_keys<Key>(&self, key_prefix: &[u8]) -> Result<Vec<Key>, MemoryContextError>
    where
        Key: DeserializeOwned + Send,
    {
        let map = self.read().await;
        let mut keys = Vec::new();
        let len = key_prefix.len();
        for key in map.keys() {
            if key.starts_with(key_prefix) {
                keys.push(bcs::from_bytes(&key[len..])?);
            }
        }
        Ok(keys)
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), MemoryContextError> {
        let mut map = self.write().await;
        for ent in batch.operations {
            match ent {
                WriteOperation::Put { key, value } => map.insert(key, value),
                WriteOperation::Delete { key } => map.remove(&key),
            };
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum MemoryContextError {
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),
}

impl From<MemoryContextError> for ViewError {
    fn from(error: MemoryContextError) -> Self {
        Self::ContextError {
            backend: "memory".to_string(),
            error: error.to_string(),
        }
    }
}
