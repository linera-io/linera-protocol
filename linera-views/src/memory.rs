// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

use async_lock::{Mutex, MutexGuardArc, RwLock};
use futures::FutureExt as _;
use thiserror::Error;

use crate::{
    batch::{Batch, DeletePrefixExpander, WriteOperation},
    common::{
        get_interval, AdminKeyValueStore, CommonStoreConfig, Context, ContextFromStore,
        KeyIterable, KeyValueStore, ReadableKeyValueStore, WritableKeyValueStore,
    },
    value_splitting::DatabaseConsistencyError,
    views::ViewError,
};

/// The initial configuration of the system
#[derive(Debug)]
pub struct MemoryStoreConfig {
    /// The common configuration of the key value store
    pub common_config: CommonStoreConfig,
}

impl MemoryStoreConfig {
    /// Creates a `MemoryStoreConfig`. `max_concurrent_queries` and `cache_size` are not used.
    pub fn new(max_stream_queries: usize) -> Self {
        let common_config = CommonStoreConfig {
            max_concurrent_queries: None,
            max_stream_queries,
            cache_size: 1000,
        };
        Self { common_config }
    }
}

/// The number of streams for the test
pub const TEST_MEMORY_MAX_STREAM_QUERIES: usize = 10;

/// The data is serialized in memory just like for RocksDB / DynamoDB
/// The analog of the database is the BTreeMap
pub type MemoryStoreMap = BTreeMap<Vec<u8>, Vec<u8>>;

/// A virtual DB client where data are persisted in memory.
#[derive(Clone)]
pub struct MemoryStore {
    /// The map used for storing the data.
    pub map: Arc<RwLock<MutexGuardArc<MemoryStoreMap>>>,
    /// The maximum number of queries used for the stream.
    pub max_stream_queries: usize,
}

impl ReadableKeyValueStore<MemoryStoreError> for MemoryStore {
    const MAX_KEY_SIZE: usize = usize::MAX;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MemoryStoreError> {
        let map = self.map.read().await;
        Ok(map.get(key).cloned())
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, MemoryStoreError> {
        let map = self.map.read().await;
        Ok(map.contains_key(key))
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, MemoryStoreError> {
        let map = self.map.read().await;
        Ok(keys
            .into_iter()
            .map(|key| map.contains_key(&key))
            .collect::<Vec<_>>())
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, MemoryStoreError> {
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
    ) -> Result<Vec<Vec<u8>>, MemoryStoreError> {
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
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, MemoryStoreError> {
        let map = self.map.read().await;
        let mut key_values = Vec::new();
        let len = key_prefix.len();
        for (key, value) in map.range(get_interval(key_prefix.to_vec())) {
            let key_value = (key[len..].to_vec(), value.to_vec());
            key_values.push(key_value);
        }
        Ok(key_values)
    }
}

impl WritableKeyValueStore<MemoryStoreError> for MemoryStore {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), MemoryStoreError> {
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
                    let key_list = map
                        .range(get_interval(key_prefix))
                        .map(|x| x.0.to_vec())
                        .collect::<Vec<_>>();
                    for key in key_list {
                        map.remove(&key);
                    }
                }
            }
        }
        Ok(())
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), MemoryStoreError> {
        Ok(())
    }
}

impl AdminKeyValueStore for MemoryStore {
    type Error = MemoryStoreError;
    type Config = MemoryStoreConfig;

    async fn connect(config: &Self::Config, _namespace: &str) -> Result<Self, MemoryStoreError> {
        let state = Arc::new(Mutex::new(BTreeMap::new()));
        let guard = state
            .try_lock_arc()
            .expect("We should acquire the lock just after creating the object");
        let max_stream_queries = config.common_config.max_stream_queries;
        let map = Arc::new(RwLock::new(guard));
        Ok(MemoryStore {
            map,
            max_stream_queries,
        })
    }

    async fn list_all(_config: &Self::Config) -> Result<Vec<String>, MemoryStoreError> {
        Ok(Vec::new())
    }

    async fn exists(_config: &Self::Config, _namespace: &str) -> Result<bool, MemoryStoreError> {
        Ok(false)
    }

    async fn create(_config: &Self::Config, _namespace: &str) -> Result<(), MemoryStoreError> {
        Ok(())
    }

    async fn delete(_config: &Self::Config, _namespace: &str) -> Result<(), MemoryStoreError> {
        Ok(())
    }
}

impl KeyValueStore for MemoryStore {
    type Error = MemoryStoreError;
}

/// An implementation of [`crate::common::Context`] that stores all values in memory.
pub type MemoryContext<E> = ContextFromStore<E, MemoryStore>;

impl<E> MemoryContext<E> {
    /// Creates a [`MemoryContext`].
    pub fn new(max_stream_queries: usize, extra: E) -> Self {
        let common_config = CommonStoreConfig {
            max_concurrent_queries: None,
            max_stream_queries,
            cache_size: 1000,
        };
        let config = MemoryStoreConfig { common_config };
        let namespace = "linera";
        let store = MemoryStore::connect(&config, namespace)
            .now_or_never()
            .unwrap()
            .unwrap();
        let base_key = Vec::new();
        Self {
            store,
            base_key,
            extra,
        }
    }
}

/// Provides a `MemoryContext<()>` that can be used for tests.
/// It is not named create_memory_test_context because it is massively
/// used and so we want to have a short name.
pub fn create_memory_context() -> MemoryContext<()> {
    MemoryContext::new(TEST_MEMORY_MAX_STREAM_QUERIES, ())
}

/// Creates a test memory client for working.
pub fn create_memory_store_stream_queries(max_stream_queries: usize) -> MemoryStore {
    let common_config = CommonStoreConfig {
        max_concurrent_queries: None,
        max_stream_queries,
        cache_size: 1000,
    };
    let config = MemoryStoreConfig { common_config };
    let namespace = "linera";
    MemoryStore::connect(&config, namespace)
        .now_or_never()
        .unwrap()
        .unwrap()
}

/// Creates a test memory store for working.
pub fn create_memory_store() -> MemoryStore {
    create_memory_store_stream_queries(TEST_MEMORY_MAX_STREAM_QUERIES)
}

/// The error type for [`MemoryContext`].
#[derive(Error, Debug)]
pub enum MemoryStoreError {
    /// Serialization error with BCS.
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),

    /// The value is too large for the MemoryStore
    #[error("The value is too large for the MemoryStore")]
    TooLargeValue,

    /// The database is not consistent
    #[error(transparent)]
    DatabaseConsistencyError(#[from] DatabaseConsistencyError),
}

impl From<MemoryStoreError> for ViewError {
    fn from(error: MemoryStoreError) -> Self {
        Self::StoreError {
            backend: "memory".to_string(),
            error: error.to_string(),
        }
    }
}

impl DeletePrefixExpander for MemoryContext<()> {
    type Error = MemoryStoreError;

    async fn expand_delete_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let mut vector_list = Vec::new();
        for key in <Vec<Vec<u8>> as KeyIterable<Self::Error>>::iterator(
            &self.find_keys_by_prefix(key_prefix).await?,
        ) {
            vector_list.push(key?.to_vec());
        }
        Ok(vector_list)
    }
}
