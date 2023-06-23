// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::{Batch, WriteOperation},
    common::{
        get_interval, ContextFromDb, DatabaseConsistencyError, KeyValueStoreClient,
        KeyValueStoreClientBigValue,
    },
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
pub type MemoryClientInternal = Arc<RwLock<MutexGuardArc<MemoryStoreMap>>>;

#[async_trait]
impl KeyValueStoreClient for MemoryClientInternal {
    const MAX_CONNECTIONS: usize = 1;
    // For the memory container, memory is the limit and so usize::MAX
    // could be used. But when memory is used for tests we want to exercise
    // the splitting mechanism and so we set it artificially to 100.
    #[cfg(any(test, feature = "test"))]
    const MAX_VALUE_SIZE: usize = 100;
    #[cfg(not(any(test, feature = "test")))]
    const MAX_VALUE_SIZE: usize = usize::MAX;
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

/// Implementation of the MemoryClient from the existing one.
#[derive(Clone)]
pub struct MemoryClient {
    client: KeyValueStoreClientBigValue<MemoryClientInternal>,
}

#[async_trait]
impl KeyValueStoreClient for MemoryClient {
    const MAX_CONNECTIONS: usize = MemoryClientInternal::MAX_CONNECTIONS;
    const MAX_VALUE_SIZE: usize = MemoryClientInternal::MAX_VALUE_SIZE;
    type Error = MemoryContextError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MemoryContextError> {
        self.client.read_key_bytes(key).await
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, MemoryContextError> {
        self.client.read_multi_key_bytes(keys).await
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, MemoryContextError> {
        self.client.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, MemoryContextError> {
        self.client.find_key_values_by_prefix(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), MemoryContextError> {
        self.client.write_batch(batch, base_key).await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Self::Error> {
        self.client.clear_journal(base_key).await
    }
}

impl MemoryClient {
    /// Create a MemoryClient from the guard
    pub fn new(guard: MutexGuardArc<MemoryStoreMap>) -> Self {
        let client = Arc::new(RwLock::new(guard));
        MemoryClient {
            client: KeyValueStoreClientBigValue::new(client),
        }
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
/// used
pub fn create_test_context() -> MemoryContext<()> {
    let state = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = state
        .try_lock_arc()
        .expect("We should acquire the lock just after creating the object");
    MemoryContext::new(guard, ())
}

/// create a test memory client for working.
pub fn create_memory_test_client() -> MemoryClient {
    let state = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = state
        .try_lock_arc()
        .expect("We should acquire the lock just after creating the object");
    MemoryClient::new(guard)
}

/// An implementation of [`crate::common::Context`] that stores all values in memory.
pub type MemoryContextInternal<E> = ContextFromDb<E, MemoryClientInternal>;

impl<E> MemoryContextInternal<E> {
    /// Creates a [`MemoryContext`].
    pub fn new(guard: MutexGuardArc<MemoryStoreMap>, extra: E) -> Self {
        let db = Arc::new(RwLock::new(guard));
        let base_key = Vec::new();
        Self {
            db,
            base_key,
            extra,
        }
    }
}

/// Provides a `MemoryContext<()>` that can be used for tests.
pub fn create_test_context_internal() -> MemoryContextInternal<()> {
    let state = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = state
        .try_lock_arc()
        .expect("We should acquire the lock just after creating the object");
    MemoryContextInternal::new(guard, ())
}

/// The error type for [`MemoryContext`].
#[derive(Error, Debug)]
pub enum MemoryContextError {
    /// Serialization error with BCS.
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),

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
