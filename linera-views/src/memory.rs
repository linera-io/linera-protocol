// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::{Batch, WriteOperation},
    common::{get_interval, ContextFromDb, KeyValueStoreClient},
    value_splitting::DatabaseConsistencyError,
    views::ViewError,
};
use async_lock::{Mutex, MutexGuardArc, RwLock};
use async_trait::async_trait;
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};
use thiserror::Error;

/// The data is serialized in memory just like for RocksDB / DynamoDB
/// The analog of the database is the BTreeMap
pub type MemoryStoreMap = BTreeMap<Vec<u8>, Vec<u8>>;

/// A virtual DB client where data are persisted in memory.
#[derive(Clone)]
pub struct MemoryClient {
    map: Arc<RwLock<MutexGuardArc<MemoryStoreMap>>>,
}

#[async_trait]
impl KeyValueStoreClient for MemoryClient {
    const MAX_CONNECTIONS: usize = 1;
    const MAX_VALUE_SIZE: usize = usize::MAX;
    type Error = MemoryContextError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MemoryContextError> {
        let map = self.map.read().await;
        Ok(map.get(key).cloned())
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, MemoryContextError> {
        let map = self.map.read().await;
        let mut result = Vec::new();
        for key in keys {
            result.push(map.get(&key).cloned());
        }
        Ok(result)
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, MemoryContextError> {
        let map = self.map.read().await;
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
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, MemoryContextError> {
        let map = self.map.read().await;
        let mut key_values = Vec::new();
        let len = key_prefix.len();
        for (key, value) in map.range(get_interval(key_prefix.to_vec())) {
            let key_value = (key[len..].to_vec(), value.to_vec());
            key_values.push(key_value);
        }
        Ok(key_values)
    }

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), MemoryContextError> {
        let mut map = self.map.write().await;
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

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), MemoryContextError> {
        Ok(())
    }
}

impl MemoryClient {
    /// constructor of MemoryClient
    pub fn new(guard: MutexGuardArc<MemoryStoreMap>) -> Self {
        let map = Arc::new(RwLock::new(guard));
        MemoryClient { map }
    }
}

/// An implementation of [`crate::common::Context`] that stores all values in memory.
pub type MemoryContext<E> = ContextFromDb<E, MemoryClient>;

impl<E> MemoryContext<E> {
    /// Creates a [`MemoryContext`].
    pub fn new(guard: MutexGuardArc<MemoryStoreMap>, extra: E) -> Self {
        let db = MemoryClient::new(guard);
        let base_key = Vec::new();
        Self {
            db,
            base_key,
            extra,
        }
    }
}

/// Provides a `MemoryContext<()>` that can be used for tests.
/// It is not named create_memory_test_context because it is massively
/// used and so we want to have a short name.
pub fn create_memory_context() -> MemoryContext<()> {
    let state = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = state
        .try_lock_arc()
        .expect("We should acquire the lock just after creating the object");
    MemoryContext::new(guard, ())
}

/// Creates a test memory client for working.
pub fn create_memory_client() -> MemoryClient {
    let state = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = state
        .try_lock_arc()
        .expect("We should acquire the lock just after creating the object");
    MemoryClient::new(guard)
}

/// The error type for [`MemoryContext`].
#[derive(Error, Debug)]
pub enum MemoryContextError {
    /// Serialization error with BCS.
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),

    /// The value is too large for the MemoryClient
    #[error("The value is too large for the MemoryClient")]
    TooLargeValue,

    /// The database is not consistent
    #[error(transparent)]
    DatabaseConsistencyError(#[from] DatabaseConsistencyError),
}

impl From<MemoryContextError> for ViewError {
    fn from(error: MemoryContextError) -> Self {
        Self::ContextError {
            backend: "memory".to_string(),
            error: error.to_string(),
        }
    }
}
