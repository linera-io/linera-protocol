// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::store::KeyValueStore`] in memory.

use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock, Mutex, RwLock},
};

use thiserror::Error;

#[cfg(with_testing)]
use crate::random::generate_test_namespace;
#[cfg(with_testing)]
use crate::store::TestKeyValueStore;
use crate::{
    batch::{Batch, WriteOperation},
    common::get_interval,
    store::{
        AdminKeyValueStore, CommonStoreInternalConfig, KeyValueStoreError, ReadableKeyValueStore,
        WithError, WritableKeyValueStore,
    },
};

/// The initial configuration of the system
#[derive(Debug)]
pub struct MemoryStoreConfig {
    /// The common configuration of the key value store
    pub common_config: CommonStoreInternalConfig,
}

impl MemoryStoreConfig {
    /// Creates a `MemoryStoreConfig`. `max_concurrent_queries` and `cache_size` are not used.
    pub fn new(max_stream_queries: usize) -> Self {
        let common_config = CommonStoreInternalConfig {
            max_concurrent_queries: None,
            max_stream_queries,
        };
        Self { common_config }
    }
}

/// The number of streams for the test
pub const TEST_MEMORY_MAX_STREAM_QUERIES: usize = 10;

/// The data is serialized in memory just like for RocksDB / DynamoDB
/// The analog of the database is the BTreeMap
type MemoryStoreMap = BTreeMap<Vec<u8>, Vec<u8>>;

/// The container for the `MemoryStoreMap`s by namespace and then root key
#[derive(Default)]
struct MemoryStores {
    stores: BTreeMap<String, BTreeMap<Vec<u8>, Arc<RwLock<MemoryStoreMap>>>>,
}

impl MemoryStores {
    fn sync_connect(
        &mut self,
        config: &MemoryStoreConfig,
        namespace: &str,
        root_key: &[u8],
        kill_on_drop: bool,
    ) -> Result<MemoryStore, MemoryStoreError> {
        let max_stream_queries = config.common_config.max_stream_queries;
        let Some(stores) = self.stores.get_mut(namespace) else {
            return Err(MemoryStoreError::NamespaceNotFound);
        };
        let store = stores.entry(root_key.to_vec()).or_insert_with(|| {
            let map = MemoryStoreMap::new();
            Arc::new(RwLock::new(map))
        });
        let map = store.clone();
        let namespace = namespace.to_string();
        let root_key = root_key.to_vec();
        Ok(MemoryStore {
            map,
            max_stream_queries,
            namespace,
            root_key,
            kill_on_drop,
        })
    }

    fn sync_list_all(&self) -> Vec<String> {
        self.stores.keys().cloned().collect::<Vec<_>>()
    }

    fn sync_exists(&self, namespace: &str) -> bool {
        self.stores.contains_key(namespace)
    }

    fn sync_create(&mut self, namespace: &str) {
        self.stores.insert(namespace.to_string(), BTreeMap::new());
    }

    fn sync_delete(&mut self, namespace: &str) {
        self.stores.remove(namespace);
    }
}

/// The global variables of the Namespace memory stores
static MEMORY_STORES: LazyLock<Mutex<MemoryStores>> =
    LazyLock::new(|| Mutex::new(MemoryStores::default()));

/// A virtual DB client where data are persisted in memory.
#[derive(Clone)]
pub struct MemoryStore {
    /// The map used for storing the data.
    map: Arc<RwLock<MemoryStoreMap>>,
    /// The maximum number of queries used for the stream.
    max_stream_queries: usize,
    /// The namespace of the store
    namespace: String,
    /// The root key of the store
    root_key: Vec<u8>,
    /// Whether to kill on drop or not the
    kill_on_drop: bool,
}

impl Drop for MemoryStore {
    fn drop(&mut self) {
        if self.kill_on_drop {
            let mut memory_stores = MEMORY_STORES
                .lock()
                .expect("MEMORY_STORES lock should not be poisoned");
            let stores = memory_stores.stores.get_mut(&self.namespace).unwrap();
            stores.remove(&self.root_key);
        }
    }
}

impl WithError for MemoryStore {
    type Error = MemoryStoreError;
}

impl ReadableKeyValueStore for MemoryStore {
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

impl WritableKeyValueStore for MemoryStore {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch) -> Result<(), MemoryStoreError> {
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

    async fn clear_journal(&self) -> Result<(), MemoryStoreError> {
        Ok(())
    }
}

impl MemoryStore {
    /// Connects to a memory store. Creates it if it does not exist yet
    fn sync_maybe_create_and_connect(
        config: &MemoryStoreConfig,
        namespace: &str,
        root_key: &[u8],
        kill_on_drop: bool,
    ) -> Result<Self, MemoryStoreError> {
        let mut memory_stores = MEMORY_STORES.lock().expect("lock should not be poisoned");
        if !memory_stores.sync_exists(namespace) {
            memory_stores.sync_create(namespace);
        }
        memory_stores.sync_connect(config, namespace, root_key, kill_on_drop)
    }

    /// Creates a `MemoryStore` from a number of queries and a namespace.
    pub fn new(
        max_stream_queries: usize,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, MemoryStoreError> {
        let common_config = CommonStoreInternalConfig {
            max_concurrent_queries: None,
            max_stream_queries,
        };
        let config = MemoryStoreConfig { common_config };
        let kill_on_drop = false;
        MemoryStore::sync_maybe_create_and_connect(&config, namespace, root_key, kill_on_drop)
    }

    /// Creates a `MemoryStore` from a number of queries and a namespace for testing.
    #[cfg(with_testing)]
    pub fn new_for_testing(
        max_stream_queries: usize,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, MemoryStoreError> {
        let common_config = CommonStoreInternalConfig {
            max_concurrent_queries: None,
            max_stream_queries,
        };
        let config = MemoryStoreConfig { common_config };
        let kill_on_drop = true;
        MemoryStore::sync_maybe_create_and_connect(&config, namespace, root_key, kill_on_drop)
    }
}

impl AdminKeyValueStore for MemoryStore {
    type Config = MemoryStoreConfig;

    fn get_name() -> String {
        "memory".to_string()
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, MemoryStoreError> {
        let mut memory_stores = MEMORY_STORES
            .lock()
            .expect("MEMORY_STORES lock should not be poisoned");
        let kill_on_drop = false;
        memory_stores.sync_connect(config, namespace, root_key, kill_on_drop)
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, MemoryStoreError> {
        let max_stream_queries = self.max_stream_queries;
        let common_config = CommonStoreInternalConfig {
            max_concurrent_queries: None,
            max_stream_queries,
        };
        let config = MemoryStoreConfig { common_config };
        let mut memory_stores = MEMORY_STORES
            .lock()
            .expect("MEMORY_STORES lock should not be poisoned");
        let kill_on_drop = self.kill_on_drop;
        let namespace = &self.namespace;
        memory_stores.sync_connect(&config, namespace, root_key, kill_on_drop)
    }

    async fn list_all(_config: &Self::Config) -> Result<Vec<String>, MemoryStoreError> {
        let memory_stores = MEMORY_STORES
            .lock()
            .expect("MEMORY_STORES lock should not be poisoned");
        Ok(memory_stores.sync_list_all())
    }

    async fn exists(_config: &Self::Config, namespace: &str) -> Result<bool, MemoryStoreError> {
        let memory_stores = MEMORY_STORES
            .lock()
            .expect("MEMORY_STORES lock should not be poisoned");
        Ok(memory_stores.sync_exists(namespace))
    }

    async fn create(_config: &Self::Config, namespace: &str) -> Result<(), MemoryStoreError> {
        let mut memory_stores = MEMORY_STORES
            .lock()
            .expect("MEMORY_STORES lock should not be poisoned");
        memory_stores.sync_create(namespace);
        Ok(())
    }

    async fn delete(_config: &Self::Config, namespace: &str) -> Result<(), MemoryStoreError> {
        let mut memory_stores = MEMORY_STORES
            .lock()
            .expect("MEMORY_STORES lock should not be poisoned");
        memory_stores.sync_delete(namespace);
        Ok(())
    }
}

#[cfg(with_testing)]
impl TestKeyValueStore for MemoryStore {
    async fn new_test_config() -> Result<MemoryStoreConfig, MemoryStoreError> {
        let max_stream_queries = TEST_MEMORY_MAX_STREAM_QUERIES;
        let common_config = CommonStoreInternalConfig {
            max_concurrent_queries: None,
            max_stream_queries,
        };
        Ok(MemoryStoreConfig { common_config })
    }
}

/// Creates a test memory store for working.
#[cfg(with_testing)]
pub fn create_test_memory_store() -> MemoryStore {
    let namespace = generate_test_namespace();
    let root_key = &[];
    MemoryStore::new_for_testing(TEST_MEMORY_MAX_STREAM_QUERIES, &namespace, root_key).unwrap()
}

/// The error type for [`MemoryStore`].
#[derive(Error, Debug)]
pub enum MemoryStoreError {
    /// Serialization error with BCS.
    #[error(transparent)]
    BcsError(#[from] bcs::Error),

    /// The value is too large for the MemoryStore
    #[error("The value is too large for the MemoryStore")]
    TooLargeValue,

    /// The namespace does not exist
    #[error("The namespace does not exist")]
    NamespaceNotFound,
}

impl KeyValueStoreError for MemoryStoreError {
    const BACKEND: &'static str = "memory";
}
