// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::common::KeyValueStore`] for the RocksDB database.

use std::{
    ffi::OsString,
    ops::{Bound, Bound::Excluded},
    path::PathBuf,
    sync::Arc,
};

use linera_base::ensure;
use tempfile::TempDir;
use thiserror::Error;

#[cfg(with_metrics)]
use crate::metering::{
    MeteredStore, LRU_CACHING_METRICS, ROCKS_DB_METRICS, VALUE_SPLITTING_METRICS,
};
#[cfg(with_testing)]
use crate::test_utils::generate_test_namespace;
use crate::{
    batch::{Batch, WriteOperation},
    common::{
        get_upper_bound, AdminKeyValueStore, CommonStoreConfig, KeyValueStoreError,
        ReadableKeyValueStore, WithError, WritableKeyValueStore,
    },
    lru_caching::{LruCachingStore, TEST_CACHE_SIZE},
    value_splitting::{DatabaseConsistencyError, ValueSplittingStore},
};

/// The number of streams for the test
const TEST_ROCKS_DB_MAX_STREAM_QUERIES: usize = 10;

// The maximum size of values in RocksDB is 3 GB
// That is 3221225472 and so for offset reason we decrease by 400
const MAX_VALUE_SIZE: usize = 3221225072;

// The maximum size of keys in RocksDB is 8 MB
// 8388608 and so for offset reason we decrease by 400
const MAX_KEY_SIZE: usize = 8388208;

/// The RocksDB client that we use.
pub type DB = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;

/// The choice of the spawning mode.
/// The SpawnBlocking always works and is the safest.
/// The BlockInPlace can only be used in multi-threaded environment.
/// One way to select that is to select BlockInPlace when
/// tokio::runtime::Handle::current().metrics().num_workers() > 1
/// The BlockInPlace is documented in <https://docs.rs/tokio/latest/tokio/task/fn.block_in_place.html>
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RocksDbSpawnMode {
    /// This uses the `spawn_blocking` function of tokio.
    SpawnBlocking,
    /// This uses the `block_in_place` function of tokio.
    BlockInPlace,
}

impl RocksDbSpawnMode {
    /// Obtains the spawning mode from runtime
    pub fn get_spawn_mode_from_runtime() -> Self {
        if tokio::runtime::Handle::current().metrics().num_workers() > 1 {
            RocksDbSpawnMode::BlockInPlace
        } else {
            RocksDbSpawnMode::SpawnBlocking
        }
    }

    /// Runs the computation for a function according to the selected policy.
    #[inline]
    pub async fn spawn<F, I, O>(&self, f: F, input: I) -> Result<O, RocksDbStoreError>
    where
        F: FnOnce(I) -> Result<O, RocksDbStoreError> + Send + 'static,
        I: Send + 'static,
        O: Send + 'static,
    {
        Ok(match self {
            RocksDbSpawnMode::BlockInPlace => {
                tokio::task::block_in_place(move || f(input))?
            },
            RocksDbSpawnMode::SpawnBlocking => {
                tokio::task::spawn_blocking(move || f(input)).await??
            },
        })
    }

}




#[derive(Clone)]
struct RocksDbStoreExecutor {
    db: Arc<DB>,
    root_key: Vec<u8>,
}

impl RocksDbStoreExecutor {
    pub fn contains_keys_internal(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<bool>, RocksDbStoreError> {
        let size = keys.len();
        let mut results = vec![false; size];
        let mut indices = Vec::new();
        let mut keys_red = Vec::new();
        for (i, key) in keys.into_iter().enumerate() {
            ensure!(key.len() <= MAX_KEY_SIZE, RocksDbStoreError::KeyTooLong);
            let mut full_key = self.root_key.to_vec();
            full_key.extend(key);
            let key_may_exist = self.db.key_may_exist(&full_key);
            if key_may_exist {
                indices.push(i);
                keys_red.push(full_key);
            }
        }
        let values_red = self.db.multi_get(keys_red);
        for (index, value) in indices.into_iter().zip(values_red) {
            results[index] = value?.is_some();
        }
        Ok(results)
    }

    fn read_multi_values_bytes_internal(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, RocksDbStoreError> {
        for key in &keys {
            ensure!(key.len() <= MAX_KEY_SIZE, RocksDbStoreError::KeyTooLong);
        }
        let full_keys = keys
            .into_iter()
            .map(|key| {
                let mut full_key = self.root_key.to_vec();
                full_key.extend(key);
                full_key
            })
            .collect::<Vec<_>>();
        let entries = self.db.multi_get(&full_keys);
        Ok(entries.into_iter().collect::<Result<_, _>>()?)
    }

    fn find_keys_by_prefix_internal(
        &self,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, RocksDbStoreError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            RocksDbStoreError::KeyTooLong
        );
        let mut prefix = self.root_key.clone();
        prefix.extend(key_prefix);
        let len = prefix.len();
        let mut iter = self.db.raw_iterator();
        let mut keys = Vec::new();
        iter.seek(&prefix);
        let mut next_key = iter.key();
        while let Some(key) = next_key {
            if !key.starts_with(&prefix) {
                break;
            }
            keys.push(key[len..].to_vec());
            iter.next();
            next_key = iter.key();
        }
        Ok(keys)
    }

    #[allow(clippy::type_complexity)]
    fn find_key_values_by_prefix_internal(
        &self,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RocksDbStoreError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            RocksDbStoreError::KeyTooLong
        );
        let mut prefix = self.root_key.clone();
        prefix.extend(key_prefix);
        let len = prefix.len();
        let mut iter = self.db.raw_iterator();
        let mut key_values = Vec::new();
        iter.seek(&prefix);
        let mut next_key = iter.key();
        while let Some(key) = next_key {
            if !key.starts_with(&prefix) {
                break;
            }
            if let Some(value) = iter.value() {
                let key_value = (key[len..].to_vec(), value.to_vec());
                key_values.push(key_value);
            }
            iter.next();
            next_key = iter.key();
        }
        Ok(key_values)
    }

    fn write_batch_internal(&self, mut batch: Batch) -> Result<(), RocksDbStoreError> {
        // NOTE: The delete_range functionality of RocksDB needs to have an upper bound in order to work.
        // Thus in order to have the system working, we need to handle the unlikely case of having to
        // delete a key starting with [255, ...., 255]
        let len = batch.operations.len();
        let mut keys = Vec::new();
        for i in 0..len {
            let op = batch.operations.get(i).unwrap();
            if let WriteOperation::DeletePrefix { key_prefix } = op {
                if get_upper_bound(key_prefix) == Bound::Unbounded {
                    for short_key in self.find_keys_by_prefix_internal(key_prefix.to_vec())? {
                        let mut full_key = self.root_key.clone();
                        full_key.extend(key_prefix);
                        full_key.extend(short_key);
                        keys.push(full_key);
                    }
                }
            }
        }
        for key in keys {
            batch.operations.push(WriteOperation::Delete { key });
        }
        let mut inner_batch = rocksdb::WriteBatchWithTransaction::default();
        for operation in batch.operations {
            match operation {
                WriteOperation::Delete { key } => {
                    ensure!(key.len() <= MAX_KEY_SIZE, RocksDbStoreError::KeyTooLong);
                    let mut full_key = self.root_key.to_vec();
                    full_key.extend(key);
                    inner_batch.delete(&full_key)
                }
                WriteOperation::Put { key, value } => {
                    ensure!(key.len() <= MAX_KEY_SIZE, RocksDbStoreError::KeyTooLong);
                    let mut full_key = self.root_key.to_vec();
                    full_key.extend(key);
                    inner_batch.put(&full_key, value)
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    ensure!(
                        key_prefix.len() <= MAX_KEY_SIZE,
                        RocksDbStoreError::KeyTooLong
                    );
                    if let Excluded(upper_bound) = get_upper_bound(&key_prefix) {
                        let mut full_key1 = self.root_key.to_vec();
                        full_key1.extend(key_prefix);
                        let mut full_key2 = self.root_key.to_vec();
                        full_key2.extend(upper_bound);
                        inner_batch.delete_range(&full_key1, &full_key2);
                    }
                }
            }
        }
        self.db.write(inner_batch)?;
        Ok(())
    }
}

/// The inner client
#[derive(Clone)]
struct RocksDbStoreInternal {
    executor: RocksDbStoreExecutor,
    _path_with_guard: PathWithGuard,
    max_stream_queries: usize,
    cache_size: usize,
    spawn_mode: RocksDbSpawnMode,
}

/// The initial configuration of the system
#[derive(Clone, Debug)]
pub struct RocksDbStoreConfig {
    /// The path to the storage containing the namespaces..
    pub path_with_guard: PathWithGuard,
    /// The spawn_mode that is chosen
    pub spawn_mode: RocksDbSpawnMode,
    /// The common configuration of the key value store
    pub common_config: CommonStoreConfig,
}

impl RocksDbStoreInternal {
    fn check_namespace(namespace: &str) -> Result<(), RocksDbStoreError> {
        if !namespace
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return Err(RocksDbStoreError::InvalidNamespace);
        }
        Ok(())
    }

    fn build(
        path_with_guard: PathWithGuard,
        spawn_mode: RocksDbSpawnMode,
        max_stream_queries: usize,
        cache_size: usize,
        root_key: &[u8],
    ) -> Result<RocksDbStoreInternal, RocksDbStoreError> {
        let path = path_with_guard.path_buf.clone();
        if !std::path::Path::exists(&path) {
            std::fs::create_dir(path.clone())?;
        }
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, path)?;
        let root_key = root_key.to_vec();
        let executor = RocksDbStoreExecutor {
            db: Arc::new(db),
            root_key,
        };
        Ok(RocksDbStoreInternal {
            executor,
            _path_with_guard: path_with_guard,
            max_stream_queries,
            cache_size,
            spawn_mode,
        })
    }
}

impl WithError for RocksDbStoreInternal {
    type Error = RocksDbStoreError;
}

impl ReadableKeyValueStore for RocksDbStoreInternal {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksDbStoreError> {
        ensure!(key.len() <= MAX_KEY_SIZE, RocksDbStoreError::KeyTooLong);
        let db = self.executor.db.clone();
        let mut full_key = self.executor.root_key.to_vec();
        full_key.extend(key);
        self.spawn_mode.spawn(move |x| Ok(db.get(&x)?), full_key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, RocksDbStoreError> {
        ensure!(key.len() <= MAX_KEY_SIZE, RocksDbStoreError::KeyTooLong);
        let db = self.executor.db.clone();
        let mut full_key = self.executor.root_key.to_vec();
        full_key.extend(key);
        self.spawn_mode.spawn(move |x| {
            let key_may_exist = db.key_may_exist(&x);
            if !key_may_exist {
                return Ok(false);
            }
            Ok(db.get(&x)?.is_some())
        }, full_key).await
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, RocksDbStoreError> {
        let executor = self.executor.clone();
        self.spawn_mode.spawn(move |x| executor.contains_keys_internal(x), keys).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, RocksDbStoreError> {
        let executor = self.executor.clone();
        self.spawn_mode.spawn(move |x| executor.read_multi_values_bytes_internal(x), keys).await
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, RocksDbStoreError> {
        let executor = self.executor.clone();
        let key_prefix = key_prefix.to_vec();
        self.spawn_mode.spawn(move |x| executor.find_keys_by_prefix_internal(x), key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, RocksDbStoreError> {
        let executor = self.executor.clone();
        let key_prefix = key_prefix.to_vec();
        self.spawn_mode.spawn(move |x| executor.find_key_values_by_prefix_internal(x), key_prefix).await
    }
}

impl WritableKeyValueStore for RocksDbStoreInternal {
    const MAX_VALUE_SIZE: usize = MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch) -> Result<(), RocksDbStoreError> {
        let executor = self.executor.clone();
        self.spawn_mode.spawn(move |x| executor.write_batch_internal(x), batch).await
    }

    async fn clear_journal(&self) -> Result<(), RocksDbStoreError> {
        Ok(())
    }
}

impl AdminKeyValueStore for RocksDbStoreInternal {
    type Config = RocksDbStoreConfig;

    async fn new_test_config() -> Result<RocksDbStoreConfig, RocksDbStoreError> {
        let path_with_guard = create_rocks_db_test_path();
        let common_config = create_rocks_db_common_config();
        let spawn_mode = RocksDbSpawnMode::SpawnBlocking;
        Ok(RocksDbStoreConfig {
            path_with_guard,
            spawn_mode,
            common_config,
        })
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, RocksDbStoreError> {
        Self::check_namespace(namespace)?;
        let mut path_buf = config.path_with_guard.path_buf.clone();
        let mut path_with_guard = config.path_with_guard.clone();
        path_buf.push(namespace);
        path_with_guard.path_buf = path_buf;
        let max_stream_queries = config.common_config.max_stream_queries;
        let cache_size = config.common_config.cache_size;
        let spawn_mode = config.spawn_mode;
        RocksDbStoreInternal::build(
            path_with_guard,
            spawn_mode,
            max_stream_queries,
            cache_size,
            root_key,
        )
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, RocksDbStoreError> {
        let mut store = self.clone();
        store.executor.root_key = root_key.to_vec();
        Ok(store)
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, RocksDbStoreError> {
        let entries = std::fs::read_dir(config.path_with_guard.path_buf.clone())?;
        let mut namespaces = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                return Err(RocksDbStoreError::NonDirectoryNamespace);
            }
            let namespace = match entry.file_name().into_string() {
                Err(error) => {
                    return Err(RocksDbStoreError::IntoStringError(error));
                }
                Ok(namespace) => namespace,
            };
            namespaces.push(namespace);
        }
        Ok(namespaces)
    }

    async fn delete_all(config: &Self::Config) -> Result<(), RocksDbStoreError> {
        let namespaces = RocksDbStoreInternal::list_all(config).await?;
        for namespace in namespaces {
            let mut path_buf = config.path_with_guard.path_buf.clone();
            path_buf.push(&namespace);
            std::fs::remove_dir_all(path_buf.as_path())?;
        }
        Ok(())
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, RocksDbStoreError> {
        Self::check_namespace(namespace)?;
        let mut path_buf = config.path_with_guard.path_buf.clone();
        path_buf.push(namespace);
        let test = std::path::Path::exists(&path_buf);
        Ok(test)
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), RocksDbStoreError> {
        Self::check_namespace(namespace)?;
        let mut path_buf = config.path_with_guard.path_buf.clone();
        path_buf.push(namespace);
        std::fs::create_dir_all(path_buf)?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), RocksDbStoreError> {
        Self::check_namespace(namespace)?;
        let mut path_buf = config.path_with_guard.path_buf.clone();
        path_buf.push(namespace);
        let path = path_buf.as_path();
        std::fs::remove_dir_all(path)?;
        Ok(())
    }
}

/// A shared DB client for RocksDB implementing LruCaching
#[derive(Clone)]
pub struct RocksDbStore {
    #[cfg(with_metrics)]
    store: MeteredStore<
        LruCachingStore<MeteredStore<ValueSplittingStore<MeteredStore<RocksDbStoreInternal>>>>,
    >,
    #[cfg(not(with_metrics))]
    store: LruCachingStore<ValueSplittingStore<RocksDbStoreInternal>>,
}

/// Creates the common initialization for RocksDB
pub fn create_rocks_db_common_config() -> CommonStoreConfig {
    CommonStoreConfig {
        max_concurrent_queries: None,
        max_stream_queries: TEST_ROCKS_DB_MAX_STREAM_QUERIES,
        cache_size: TEST_CACHE_SIZE,
    }
}

/// A path and the guard for the temporary directory if needed
#[derive(Clone, Debug)]
pub struct PathWithGuard {
    /// The path to the data
    pub path_buf: PathBuf,
    /// The guard for the directory if one is needed
    _dir: Option<Arc<TempDir>>,
}

impl PathWithGuard {
    /// Create a PathWithGuard from an existing path.
    pub fn new(path_buf: PathBuf) -> Self {
        Self {
            path_buf,
            _dir: None,
        }
    }
}

/// Returns the test path for RocksDB without common config.
pub fn create_rocks_db_test_path() -> PathWithGuard {
    let dir = TempDir::new().unwrap();
    let path_buf = dir.path().to_path_buf();
    let _dir = Some(Arc::new(dir));
    PathWithGuard { path_buf, _dir }
}

/// Creates a RocksDB database client to be used for tests.
/// The temporary directory has to be carried because if it goes
/// out of scope then the RocksDB client can become unstable.
#[cfg(with_testing)]
pub async fn create_rocks_db_test_store() -> RocksDbStore {
    let config = RocksDbStore::new_test_config().await.expect("config");
    let namespace = generate_test_namespace();
    let root_key = &[];
    RocksDbStore::recreate_and_connect(&config, &namespace, root_key)
        .await
        .expect("store")
}

/// Creates a RocksDB database client to be used for benchmarks.
/// The temporary directory has to be carried because if it goes
/// out of scope then the RocksDB client can become unstable.
#[cfg(with_testing)]
pub async fn create_rocks_db_benchmark_store() -> RocksDbStore {
    let mut config = RocksDbStore::new_test_config().await.expect("config");
    config.spawn_mode = RocksDbSpawnMode::BlockInPlace;
    let namespace = generate_test_namespace();
    let root_key = &[];
    RocksDbStore::recreate_and_connect(&config, &namespace, root_key)
        .await
        .expect("store")
}

impl RocksDbStore {
    #[cfg(with_metrics)]
    fn inner(&self) -> &RocksDbStoreInternal {
        &self.store.store.store.store.store.store
    }

    #[cfg(not(with_metrics))]
    fn inner(&self) -> &RocksDbStoreInternal {
        &self.store.store.store
    }

    fn from_inner(store: RocksDbStoreInternal, cache_size: usize) -> RocksDbStore {
        #[cfg(with_metrics)]
        let store = MeteredStore::new(&ROCKS_DB_METRICS, store);
        let store = ValueSplittingStore::new(store);
        #[cfg(with_metrics)]
        let store = MeteredStore::new(&VALUE_SPLITTING_METRICS, store);
        let store = LruCachingStore::new(store, cache_size);
        #[cfg(with_metrics)]
        let store = MeteredStore::new(&LRU_CACHING_METRICS, store);
        Self { store }
    }
}

impl WithError for RocksDbStore {
    type Error = RocksDbStoreError;
}

impl ReadableKeyValueStore for RocksDbStore {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksDbStoreError> {
        self.store.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, RocksDbStoreError> {
        self.store.contains_key(key).await
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, RocksDbStoreError> {
        self.store.contains_keys(keys).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, RocksDbStoreError> {
        self.store.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, RocksDbStoreError> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, RocksDbStoreError> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

impl WritableKeyValueStore for RocksDbStore {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch) -> Result<(), RocksDbStoreError> {
        self.store.write_batch(batch).await
    }

    async fn clear_journal(&self) -> Result<(), RocksDbStoreError> {
        self.store.clear_journal().await
    }
}

impl AdminKeyValueStore for RocksDbStore {
    type Config = RocksDbStoreConfig;

    async fn new_test_config() -> Result<RocksDbStoreConfig, RocksDbStoreError> {
        RocksDbStoreInternal::new_test_config().await
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, RocksDbStoreError> {
        let store = RocksDbStoreInternal::connect(config, namespace, root_key).await?;
        let cache_size = config.common_config.cache_size;
        Ok(Self::from_inner(store, cache_size))
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, RocksDbStoreError> {
        let store = self.inner().clone_with_root_key(root_key)?;
        let cache_size = self.inner().cache_size;
        Ok(Self::from_inner(store, cache_size))
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, RocksDbStoreError> {
        RocksDbStoreInternal::list_all(config).await
    }

    async fn delete_all(config: &Self::Config) -> Result<(), RocksDbStoreError> {
        RocksDbStoreInternal::delete_all(config).await
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, RocksDbStoreError> {
        RocksDbStoreInternal::exists(config, namespace).await
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), RocksDbStoreError> {
        RocksDbStoreInternal::create(config, namespace).await
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), RocksDbStoreError> {
        RocksDbStoreInternal::delete(config, namespace).await
    }
}

/// The error type for [`RocksDbStore`]
#[derive(Error, Debug)]
pub enum RocksDbStoreError {
    /// Tokio join error in RocksDb.
    #[error("tokio join error: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

    /// RocksDB error.
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    /// The database contains a file which is not a directory
    #[error("Namespaces should be directories")]
    NonDirectoryNamespace,

    /// Error converting `OsString` to `String`
    #[error("error in the conversion from OsString")]
    IntoStringError(OsString),

    /// The key must have at most 8M
    #[error("The key must have at most 8M")]
    KeyTooLong,

    /// Missing database
    #[error("Missing database")]
    MissingDatabase(String),

    /// Invalid namespace
    #[error("Invalid namespace")]
    InvalidNamespace,

    /// Already existing database
    #[error("Already existing database")]
    AlreadyExistingDatabase,

    /// Filesystem error
    #[error("Filesystem error")]
    FsError(#[from] std::io::Error),

    /// BCS serialization error.
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),

    /// The database is not coherent
    #[error(transparent)]
    DatabaseConsistencyError(#[from] DatabaseConsistencyError),
}

impl KeyValueStoreError for RocksDbStoreError {
    const BACKEND: &'static str = "rocks_db";
}
