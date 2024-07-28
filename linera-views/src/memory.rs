// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, LazyLock, Mutex, RwLock},
};

use thiserror::Error;

#[cfg(with_testing)]
use crate::test_utils::generate_test_namespace;
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
type MemoryStoreMap = BTreeMap<Vec<u8>, Vec<u8>>;

/// The container for the MemoryStopMap according to the Namespace.
type NamespaceMemoryStore = BTreeMap<String, Arc<RwLock<MemoryStoreMap>>>;

/// The global variables of the Namespace memory stores
static MEMORY_STORES: LazyLock<Mutex<NamespaceMemoryStore>> =
    LazyLock::new(|| Mutex::new(NamespaceMemoryStore::new()));

/// A virtual DB client where data are persisted in memory.
#[derive(Clone)]
pub struct MemoryStore {
    /// The map used for storing the data.
    map: Arc<RwLock<MemoryStoreMap>>,
    /// The maximum number of queries used for the stream.
    max_stream_queries: usize,
}

impl ReadableKeyValueStore<MemoryStoreError> for MemoryStore {
    const MAX_KEY_SIZE: usize = usize::MAX;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MemoryStoreError> {
        let map = self
            .map
            .read()
            .expect("MemoryStore lock should not be poisoned");
        Ok(map.get(key).cloned())
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, MemoryStoreError> {
        let map = self
            .map
            .read()
            .expect("MemoryStore lock should not be poisoned");
        Ok(map.contains_key(key))
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, MemoryStoreError> {
        let map = self
            .map
            .read()
            .expect("MemoryStore lock should not be poisoned");
        Ok(keys
            .into_iter()
            .map(|key| map.contains_key(&key))
            .collect::<Vec<_>>())
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, MemoryStoreError> {
        let map = self
            .map
            .read()
            .expect("MemoryStore lock should not be poisoned");
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
        let map = self
            .map
            .read()
            .expect("MemoryStore lock should not be poisoned");
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
        let map = self
            .map
            .read()
            .expect("MemoryStore lock should not be poisoned");
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
        let mut map = self
            .map
            .write()
            .expect("MemoryStore lock should not be poisoned");
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

impl MemoryStore {
    fn sync_connect(
        namespace_memory_store: &NamespaceMemoryStore,
        config: &MemoryStoreConfig,
        namespace: &str,
    ) -> Result<Self, MemoryStoreError> {
        let max_stream_queries = config.common_config.max_stream_queries;
        let namespace = namespace.to_string();
        let store = namespace_memory_store
            .get(&namespace)
            .ok_or(MemoryStoreError::NotExistentNamespace)?;
        let map = store.clone();
        Ok(MemoryStore {
            map,
            max_stream_queries,
        })
    }

    fn sync_list_all(
        namespace_memory_store: &NamespaceMemoryStore,
    ) -> Result<Vec<String>, MemoryStoreError> {
        let namespaces = namespace_memory_store.keys().cloned().collect::<Vec<_>>();
        Ok(namespaces)
    }

    fn sync_exists(
        namespace_memory_store: &NamespaceMemoryStore,
        namespace: &str,
    ) -> Result<bool, MemoryStoreError> {
        let namespace = namespace.to_string();
        Ok(namespace_memory_store.contains_key(&namespace))
    }

    fn sync_create(
        namespace_memory_store: &mut NamespaceMemoryStore,
        namespace: &str,
    ) -> Result<(), MemoryStoreError> {
        let namespace = namespace.to_string();
        let map = MemoryStoreMap::new();
        let map = Arc::new(RwLock::new(map));
        namespace_memory_store.insert(namespace, map);
        Ok(())
    }

    fn sync_delete(
        namespace_memory_store: &mut NamespaceMemoryStore,
        namespace: &str,
    ) -> Result<(), MemoryStoreError> {
        let namespace = namespace.to_string();
        namespace_memory_store.remove(&namespace);
        Ok(())
    }

    /// Create a memory store if one is missing and otherwise connect with the existing one
    pub fn sync_maybe_create_and_connect(
        config: &MemoryStoreConfig,
        namespace: &str,
    ) -> Result<Self, MemoryStoreError> {
        let mut namespace_memory_store = MEMORY_STORES.lock().expect("lock should not be poisoned");
        if !MemoryStore::sync_exists(&namespace_memory_store, namespace)? {
            MemoryStore::sync_create(&mut namespace_memory_store, namespace)?;
        }
        MemoryStore::sync_connect(&namespace_memory_store, config, namespace)
    }

    /// Creates a `MemoryStore` from a number of queries and a namespace.
    pub fn new(max_stream_queries: usize, namespace: &str) -> Result<Self, MemoryStoreError> {
        let common_config = CommonStoreConfig {
            max_concurrent_queries: None,
            max_stream_queries,
            cache_size: 1000,
        };
        let config = MemoryStoreConfig { common_config };
        MemoryStore::sync_maybe_create_and_connect(&config, namespace)
    }
}

impl AdminKeyValueStore for MemoryStore {
    type Error = MemoryStoreError;
    type Config = MemoryStoreConfig;

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, MemoryStoreError> {
        let namespace_memory_store = MEMORY_STORES
            .lock()
            .expect("MEMORY_STORES lock should not be poisoned");
        Self::sync_connect(&namespace_memory_store, config, namespace)
    }

    async fn list_all(_config: &Self::Config) -> Result<Vec<String>, MemoryStoreError> {
        let namespace_memory_store = MEMORY_STORES
            .lock()
            .expect("MEMORY_STORES lock should not be poisoned");
        Self::sync_list_all(&namespace_memory_store)
    }

    async fn exists(_config: &Self::Config, namespace: &str) -> Result<bool, MemoryStoreError> {
        let namespace_memory_store = MEMORY_STORES
            .lock()
            .expect("MEMORY_STORES lock should not be poisoned");
        Self::sync_exists(&namespace_memory_store, namespace)
    }

    async fn create(_config: &Self::Config, namespace: &str) -> Result<(), MemoryStoreError> {
        let mut namespace_memory_store = MEMORY_STORES
            .lock()
            .expect("MEMORY_STORES lock should not be poisoned");
        Self::sync_create(&mut namespace_memory_store, namespace)
    }

    async fn delete(_config: &Self::Config, namespace: &str) -> Result<(), MemoryStoreError> {
        let mut namespace_memory_store = MEMORY_STORES
            .lock()
            .expect("MEMORY_STORES lock should not be poisoned");
        Self::sync_delete(&mut namespace_memory_store, namespace)
    }
}

impl KeyValueStore for MemoryStore {
    type Error = MemoryStoreError;
}

/// An implementation of [`crate::common::Context`] that stores all values in memory.
pub type MemoryContext<E> = ContextFromStore<E, MemoryStore>;

/// Creates a default memory test config
pub fn create_memory_store_test_config() -> MemoryStoreConfig {
    let max_stream_queries = TEST_MEMORY_MAX_STREAM_QUERIES;
    let common_config = CommonStoreConfig {
        max_concurrent_queries: None,
        max_stream_queries,
        cache_size: 1000,
    };
    MemoryStoreConfig { common_config }
}

impl<E> MemoryContext<E> {
    /// Creates a [`MemoryContext`].
    pub fn new(max_stream_queries: usize, namespace: &str, extra: E) -> Self {
        let common_config = CommonStoreConfig {
            max_concurrent_queries: None,
            max_stream_queries,
            cache_size: 1000,
        };
        let config = MemoryStoreConfig { common_config };
        let store = MemoryStore::sync_maybe_create_and_connect(&config, namespace).unwrap();
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
#[cfg(with_testing)]
pub fn create_test_memory_context() -> MemoryContext<()> {
    let namespace = generate_test_namespace();
    MemoryContext::new(TEST_MEMORY_MAX_STREAM_QUERIES, &namespace, ())
}

/// Creates a test memory store for working.
#[cfg(with_testing)]
pub fn create_test_memory_store() -> MemoryStore {
    let namespace = generate_test_namespace();
    MemoryStore::new(TEST_MEMORY_MAX_STREAM_QUERIES, &namespace).unwrap()
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

    /// The namespace is not existent
    #[error("The namespace is not existent")]
    NotExistentNamespace,

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
