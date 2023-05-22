// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::{Batch, WriteOperation},
    common::{get_interval, ContextFromDb, KeyValueStoreClient},
    views::ViewError,
};
use async_lock::{Mutex, MutexGuardArc, RwLock};
use async_trait::async_trait;
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};
use thiserror::Error;

/// The data is serialized in memory just like for rocksdb / dynamodb
/// The analogue of the database is the BTreeMap
pub type MemoryStoreMap = BTreeMap<Vec<u8>, Vec<u8>>;

/// A virtual DB client where data are persisted in memory.
pub type MemoryClient = Arc<RwLock<MutexGuardArc<MemoryStoreMap>>>;

/// An implementation of [`crate::common::Context`] that stores all values in memory.
pub type MemoryContext<E> = ContextFromDb<E, MemoryClient>;

impl<E> MemoryContext<E> {
    /// Creates a [`MemoryContext`].
    pub fn new(guard: MutexGuardArc<MemoryStoreMap>, extra: E) -> Self {
        Self {
            db: Arc::new(RwLock::new(guard)),
            base_key: Vec::new(),
            extra,
        }
    }
}

/// Provides a `MemoryContext<()>` that can be used for tests.
pub fn create_test_context() -> MemoryContext<()> {
    let state = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = state
        .try_lock_arc()
        .expect("We should acquire the lock just after creating the object");
    MemoryContext::new(guard, ())
}

#[async_trait]
impl KeyValueStoreClient for MemoryClient {
    const MAX_CONNECTIONS: usize = 1;
    type Error = MemoryContextError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MemoryContextError> {
        let map = self.read().await;
        Ok(map.get(key).cloned())
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, MemoryContextError> {
        let map = self.read().await;
        let mut result = Vec::new();
        for key in keys {
            result.push(map.get(&key).cloned());
        }
        Ok(result)
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, MemoryContextError> {
        let map = self.read().await;
        let mut values = Vec::new();
        let len = key_prefix.len();
        for (key, _value) in map.range(get_interval(key_prefix.to_vec())) {
            values.push(key[len..].to_vec())
        }
        Ok(values)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, MemoryContextError> {
        let map = self.read().await;
        let mut key_values = Vec::new();
        let len = key_prefix.len();
        for (key, value) in map.range(get_interval(key_prefix.to_vec())) {
            let key_value = (key[len..].to_vec(), value.to_vec());
            key_values.push(key_value);
        }
        Ok(key_values)
    }

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), MemoryContextError> {
        let mut map = self.write().await;
        for ent in batch.operations {
            match ent {
                WriteOperation::Put { key, value } => {
                    map.insert(key, value);
                }
                WriteOperation::Delete { key } => {
                    map.remove(&key);
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    let key_list: Vec<Vec<u8>> = map
                        .range(get_interval(key_prefix))
                        .map(|x| x.0.to_vec())
                        .collect();
                    for key in key_list {
                        map.remove(&key);
                    }
                }
            }
        }
        Ok(())
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// The error type for [`MemoryContext`].
#[derive(Error, Debug)]
pub enum MemoryContextError {
    /// Serialization error with BCS.
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
