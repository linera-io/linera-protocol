// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::store::KeyValueDatabase`] in memory.

use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock, Mutex, RwLock},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(with_testing)]
use crate::store::TestKeyValueDatabase;
use crate::{
    batch::{Batch, WriteOperation},
    common::get_interval,
    store::{
        KeyValueDatabase, KeyValueStoreError, ReadableKeyValueStore, WithError,
        WritableKeyValueStore,
    },
};

/// The initial configuration of the system
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MemoryStoreConfig {
    /// Preferred buffer size for async streams.
    pub max_stream_queries: usize,
    /// Whether a namespace should be immediately cleaned up from memory when the
    /// connection object is dropped.
    pub kill_on_drop: bool,
}

/// The number of streams for the test
#[cfg(with_testing)]
const TEST_MEMORY_MAX_STREAM_QUERIES: usize = 10;

/// The values in a partition.
type MemoryStoreMap = BTreeMap<Vec<u8>, Vec<u8>>;

/// The container for the `MemoryStoreMap`s by namespace and then root key
#[derive(Default)]
struct MemoryDatabases {
    databases: BTreeMap<String, BTreeMap<Vec<u8>, Arc<RwLock<MemoryStoreMap>>>>,
}

/// A connection to a namespace of key-values in memory.
#[derive(Clone, Debug)]
pub struct MemoryDatabase {
    /// The current namespace.
    namespace: String,
    /// The maximum number of queries used for a stream.
    max_stream_queries: usize,
    /// Whether to remove the namespace on drop.
    kill_on_drop: bool,
}

impl MemoryDatabases {
    fn sync_open(
        &mut self,
        namespace: &str,
        max_stream_queries: usize,
        root_key: &[u8],
    ) -> Result<MemoryStore, MemoryStoreError> {
        let Some(stores) = self.databases.get_mut(namespace) else {
            return Err(MemoryStoreError::NamespaceNotFound);
        };
        let store = stores.entry(root_key.to_vec()).or_insert_with(|| {
            let map = MemoryStoreMap::new();
            Arc::new(RwLock::new(map))
        });
        let map = store.clone();
        Ok(MemoryStore {
            map,
            max_stream_queries,
        })
    }

    fn sync_list_all(&self) -> Vec<String> {
        self.databases.keys().cloned().collect::<Vec<_>>()
    }

    fn sync_list_root_keys(&self, namespace: &str) -> Vec<Vec<u8>> {
        match self.databases.get(namespace) {
            None => Vec::new(),
            Some(map) => map.keys().cloned().collect::<Vec<_>>(),
        }
    }

    fn sync_exists(&self, namespace: &str) -> bool {
        self.databases.contains_key(namespace)
    }

    fn sync_create(&mut self, namespace: &str) {
        self.databases
            .insert(namespace.to_string(), BTreeMap::new());
    }

    fn sync_delete(&mut self, namespace: &str) {
        self.databases.remove(namespace);
    }
}

/// The global table of namespaces.
static MEMORY_DATABASES: LazyLock<Mutex<MemoryDatabases>> =
    LazyLock::new(|| Mutex::new(MemoryDatabases::default()));

/// A virtual DB client where data are persisted in memory.
#[derive(Clone)]
pub struct MemoryStore {
    /// The map used for storing the data.
    map: Arc<RwLock<MemoryStoreMap>>,
    /// The maximum number of queries used for a stream.
    max_stream_queries: usize,
}

impl WithError for MemoryDatabase {
    type Error = MemoryStoreError;
}

impl WithError for MemoryStore {
    type Error = MemoryStoreError;
}

impl ReadableKeyValueStore for MemoryStore {
    const MAX_KEY_SIZE: usize = usize::MAX;

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
    /// Creates a `MemoryStore` that doesn't belong to any registered namespace.
    #[cfg(with_testing)]
    pub fn new_for_testing() -> Self {
        Self {
            map: Arc::default(),
            max_stream_queries: TEST_MEMORY_MAX_STREAM_QUERIES,
        }
    }
}

impl Drop for MemoryDatabase {
    fn drop(&mut self) {
        if self.kill_on_drop {
            let mut databases = MEMORY_DATABASES
                .lock()
                .expect("MEMORY_DATABASES lock should not be poisoned");
            databases.databases.remove(&self.namespace);
        }
    }
}

impl KeyValueDatabase for MemoryDatabase {
    type Config = MemoryStoreConfig;

    type Store = MemoryStore;

    fn get_name() -> String {
        "memory".to_string()
    }

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, MemoryStoreError> {
        let databases = MEMORY_DATABASES
            .lock()
            .expect("MEMORY_DATABASES lock should not be poisoned");
        if !databases.sync_exists(namespace) {
            return Err(MemoryStoreError::NamespaceNotFound);
        };
        Ok(MemoryDatabase {
            namespace: namespace.to_string(),
            max_stream_queries: config.max_stream_queries,
            kill_on_drop: config.kill_on_drop,
        })
    }

    fn open_shared(&self, root_key: &[u8]) -> Result<Self::Store, MemoryStoreError> {
        let mut databases = MEMORY_DATABASES
            .lock()
            .expect("MEMORY_DATABASES lock should not be poisoned");
        databases.sync_open(&self.namespace, self.max_stream_queries, root_key)
    }

    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self::Store, MemoryStoreError> {
        self.open_shared(root_key)
    }

    async fn list_all(_config: &Self::Config) -> Result<Vec<String>, MemoryStoreError> {
        let databases = MEMORY_DATABASES
            .lock()
            .expect("MEMORY_DATABASES lock should not be poisoned");
        Ok(databases.sync_list_all())
    }

    async fn list_root_keys(
        _config: &Self::Config,
        namespace: &str,
    ) -> Result<Vec<Vec<u8>>, MemoryStoreError> {
        let databases = MEMORY_DATABASES
            .lock()
            .expect("MEMORY_DATABASES lock should not be poisoned");
        Ok(databases.sync_list_root_keys(namespace))
    }

    async fn exists(_config: &Self::Config, namespace: &str) -> Result<bool, MemoryStoreError> {
        let databases = MEMORY_DATABASES
            .lock()
            .expect("MEMORY_DATABASES lock should not be poisoned");
        Ok(databases.sync_exists(namespace))
    }

    async fn create(_config: &Self::Config, namespace: &str) -> Result<(), MemoryStoreError> {
        let mut databases = MEMORY_DATABASES
            .lock()
            .expect("MEMORY_DATABASES lock should not be poisoned");
        if databases.sync_exists(namespace) {
            return Err(MemoryStoreError::StoreAlreadyExists);
        }
        databases.sync_create(namespace);
        Ok(())
    }

    async fn delete(_config: &Self::Config, namespace: &str) -> Result<(), MemoryStoreError> {
        let mut databases = MEMORY_DATABASES
            .lock()
            .expect("MEMORY_DATABASES lock should not be poisoned");
        databases.sync_delete(namespace);
        Ok(())
    }
}

#[cfg(with_testing)]
impl TestKeyValueDatabase for MemoryDatabase {
    async fn new_test_config() -> Result<MemoryStoreConfig, MemoryStoreError> {
        Ok(MemoryStoreConfig {
            max_stream_queries: TEST_MEMORY_MAX_STREAM_QUERIES,
            kill_on_drop: false,
        })
    }
}

/// The error type for [`MemoryStore`].
#[derive(Error, Debug)]
pub enum MemoryStoreError {
    /// Store already exists during a create operation
    #[error("Store already exists during a create operation")]
    StoreAlreadyExists,

    /// Serialization error with BCS.
    #[error(transparent)]
    BcsError(#[from] bcs::Error),

    /// The namespace does not exist
    #[error("The namespace does not exist")]
    NamespaceNotFound,
}

impl KeyValueStoreError for MemoryStoreError {
    const BACKEND: &'static str = "memory";
}
