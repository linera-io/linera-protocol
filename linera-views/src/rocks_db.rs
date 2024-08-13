// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    ffi::OsString,
    ops::{Bound, Bound::Excluded},
    path::PathBuf,
    sync::Arc,
};

use futures::future::join_all;
use linera_base::ensure;
use thiserror::Error;
#[cfg(with_testing)]
use {
    crate::{lru_caching::TEST_CACHE_SIZE, test_utils::generate_test_namespace},
    tempfile::TempDir,
};

#[cfg(with_metrics)]
use crate::metering::{
    MeteredStore, LRU_CACHING_METRICS, ROCKS_DB_METRICS, VALUE_SPLITTING_METRICS,
};
use crate::{
    batch::{Batch, WriteOperation},
    common::{
        get_upper_bound, AdminKeyValueStore, CommonStoreConfig, ContextFromStore, KeyValueStore,
        ReadableKeyValueStore, WritableKeyValueStore,
    },
    lru_caching::LruCachingStore,
    value_splitting::{DatabaseConsistencyError, ValueSplittingStore},
};

/// The number of streams for the test
#[cfg(with_testing)]
const TEST_ROCKS_DB_MAX_STREAM_QUERIES: usize = 10;

// The maximum size of values in RocksDB is 3 GB
// That is 3221225472 and so for offset reason we decrease by 400
const MAX_VALUE_SIZE: usize = 3221225072;

// The maximum size of keys in RocksDB is 8 MB
// 8388608 and so for offset reason we decrease by 400
const MAX_KEY_SIZE: usize = 8388208;

/// The RocksDB client that we use.
pub type DB = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;

/// The internal client
#[derive(Clone)]
pub struct RocksDbStoreInternal {
    db: Arc<DB>,
    max_stream_queries: usize,
}

/// The initial configuration of the system
#[derive(Clone, Debug)]
pub struct RocksDbStoreConfig {
    /// The AWS configuration
    pub path_buf: PathBuf,
    /// The common configuration of the key value store
    pub common_config: CommonStoreConfig,
}

impl RocksDbStoreInternal {
    fn check_namespace(namespace: &str) -> Result<(), RocksDbStoreError> {
        if !namespace
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return Err(RocksDbStoreError::InvalidTableName);
        }
        Ok(())
    }
}

impl ReadableKeyValueStore<RocksDbStoreError> for RocksDbStoreInternal {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksDbStoreError> {
        ensure!(key.len() <= MAX_KEY_SIZE, RocksDbStoreError::KeyTooLong);
        let client = self.clone();
        let key = key.to_vec();
        Ok(tokio::task::spawn_blocking(move || client.db.get(&key)).await??)
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, RocksDbStoreError> {
        ensure!(key.len() <= MAX_KEY_SIZE, RocksDbStoreError::KeyTooLong);
        let client = self.clone();
        let key_may_exist = {
            let key = key.to_vec();
            tokio::task::spawn_blocking(move || client.db.key_may_exist(&key)).await?
        };
        if !key_may_exist {
            return Ok(false);
        }
        Ok(self.read_value_bytes(key).await?.is_some())
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, RocksDbStoreError> {
        let size = keys.len();
        let mut results = vec![false; size];
        let mut handles = Vec::with_capacity(size);
        for key in keys.clone() {
            ensure!(key.len() <= MAX_KEY_SIZE, RocksDbStoreError::KeyTooLong);
            let client = self.clone();
            let handle = tokio::task::spawn_blocking(move || client.db.key_may_exist(&key));
            handles.push(handle);
        }
        let may_results: Vec<_> = join_all(handles)
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        let count_true = may_results.iter().filter(|&&x| x).count();
        let mut indices = Vec::with_capacity(count_true);
        let mut keys_red = Vec::with_capacity(count_true);
        for (i, key) in keys.into_iter().enumerate() {
            if may_results[i] {
                indices.push(i);
                keys_red.push(key);
            }
        }
        let values_red = self.read_multi_values_bytes(keys_red).await?;
        for (index, value) in indices.into_iter().zip(values_red) {
            results[index] = value.is_some();
        }
        Ok(results)
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, RocksDbStoreError> {
        for key in &keys {
            ensure!(key.len() <= MAX_KEY_SIZE, RocksDbStoreError::KeyTooLong);
        }
        let client = self.clone();
        let entries = tokio::task::spawn_blocking(move || client.db.multi_get(&keys)).await?;
        Ok(entries.into_iter().collect::<Result<_, _>>()?)
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, RocksDbStoreError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            RocksDbStoreError::KeyTooLong
        );
        let client = self.clone();
        let prefix = key_prefix.to_vec();
        let len = prefix.len();
        let keys = tokio::task::spawn_blocking(move || {
            let mut iter = client.db.raw_iterator();
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
            keys
        })
        .await?;
        Ok(keys)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, RocksDbStoreError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            RocksDbStoreError::KeyTooLong
        );
        let client = self.clone();
        let prefix = key_prefix.to_vec();
        let len = prefix.len();
        let key_values = tokio::task::spawn_blocking(move || {
            let mut iter = client.db.raw_iterator();
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
            key_values
        })
        .await?;
        Ok(key_values)
    }
}

impl WritableKeyValueStore<RocksDbStoreError> for RocksDbStoreInternal {
    const MAX_VALUE_SIZE: usize = MAX_VALUE_SIZE;

    async fn write_batch(
        &self,
        mut batch: Batch,
        _base_key: &[u8],
    ) -> Result<(), RocksDbStoreError> {
        let client = self.clone();
        // NOTE: The delete_range functionality of RocksDB needs to have an upper bound in order to work.
        // Thus in order to have the system working, we need to handle the unlikely case of having to
        // delete a key starting with [255, ...., 255]
        let len = batch.operations.len();
        let mut keys = Vec::new();
        for i in 0..len {
            let op = batch.operations.get(i).unwrap();
            if let WriteOperation::DeletePrefix { key_prefix } = op {
                if get_upper_bound(key_prefix) == Bound::Unbounded {
                    for short_key in self.find_keys_by_prefix(key_prefix).await? {
                        let mut key = key_prefix.clone();
                        key.extend(short_key);
                        keys.push(key);
                    }
                }
            }
        }
        for key in keys {
            batch.operations.push(WriteOperation::Delete { key });
        }
        tokio::task::spawn_blocking(move || -> Result<(), RocksDbStoreError> {
            let mut inner_batch = rocksdb::WriteBatchWithTransaction::default();
            for operation in batch.operations {
                match operation {
                    WriteOperation::Delete { key } => {
                        ensure!(key.len() <= MAX_KEY_SIZE, RocksDbStoreError::KeyTooLong);
                        inner_batch.delete(&key)
                    }
                    WriteOperation::Put { key, value } => {
                        ensure!(key.len() <= MAX_KEY_SIZE, RocksDbStoreError::KeyTooLong);
                        inner_batch.put(&key, value)
                    }
                    WriteOperation::DeletePrefix { key_prefix } => {
                        ensure!(
                            key_prefix.len() <= MAX_KEY_SIZE,
                            RocksDbStoreError::KeyTooLong
                        );
                        if let Excluded(upper_bound) = get_upper_bound(&key_prefix) {
                            inner_batch.delete_range(key_prefix, upper_bound);
                        }
                    }
                }
            }
            client.db.write(inner_batch)?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), RocksDbStoreError> {
        Ok(())
    }
}

impl AdminKeyValueStore for RocksDbStoreInternal {
    type Error = RocksDbStoreError;
    type Config = RocksDbStoreConfig;

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, RocksDbStoreError> {
        Self::check_namespace(namespace)?;
        let options = rocksdb::Options::default();
        let mut path_buf = config.path_buf.clone();
        path_buf.push(namespace);
        let db = DB::open(&options, path_buf)?;
        let max_stream_queries = config.common_config.max_stream_queries;
        Ok(RocksDbStoreInternal {
            db: Arc::new(db),
            max_stream_queries,
        })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, RocksDbStoreError> {
        let mut entries = std::fs::read_dir(config.path_buf.clone())?;
        let mut namespaces = Vec::with_capacity(entries.by_ref().count());
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
            let mut path_buf = config.path_buf.clone();
            path_buf.push(&namespace);
            std::fs::remove_dir_all(path_buf.as_path())?;
        }
        Ok(())
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, RocksDbStoreError> {
        Self::check_namespace(namespace)?;
        let options = rocksdb::Options::default();
        let mut path_buf = config.path_buf.clone();
        path_buf.push(namespace);
        let result = DB::open(&options, path_buf.clone());
        match result {
            Ok(_) => Ok(true),
            Err(error) => match error.kind() {
                rocksdb::ErrorKind::InvalidArgument => {
                    std::fs::remove_dir_all(path_buf)?;
                    Ok(false)
                }
                _ => Err(RocksDbStoreError::RocksDb(error)),
            },
        }
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), RocksDbStoreError> {
        Self::check_namespace(namespace)?;
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let mut path_buf = config.path_buf.clone();
        path_buf.push(namespace);
        let _db = DB::open(&options, path_buf)?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), RocksDbStoreError> {
        Self::check_namespace(namespace)?;
        let mut path_buf = config.path_buf.clone();
        path_buf.push(namespace);
        let path = path_buf.as_path();
        std::fs::remove_dir_all(path)?;
        Ok(())
    }
}

impl KeyValueStore for RocksDbStoreInternal {
    type Error = RocksDbStoreError;
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
#[cfg(with_testing)]
pub fn create_rocks_db_common_config() -> CommonStoreConfig {
    CommonStoreConfig {
        max_concurrent_queries: None,
        max_stream_queries: TEST_ROCKS_DB_MAX_STREAM_QUERIES,
        cache_size: TEST_CACHE_SIZE,
    }
}

/// Returns the test path for RocksDB without common config.
#[cfg(with_testing)]
pub fn create_rocks_db_test_path() -> (PathBuf, TempDir) {
    let dir = TempDir::new().unwrap();
    let path_buf = dir.path().to_path_buf();
    (path_buf, dir)
}

/// Returns the test config and a guard for the temporary directory
#[cfg(with_testing)]
pub async fn create_rocks_db_test_config() -> (RocksDbStoreConfig, TempDir) {
    let (path_buf, tmp_dir) = create_rocks_db_test_path();
    let common_config = create_rocks_db_common_config();
    let store_config = RocksDbStoreConfig {
        path_buf,
        common_config,
    };
    (store_config, tmp_dir)
}

/// Creates a RocksDB database client to be used for tests.
/// The temporary directory has to be carried because if it goes
/// out of scope then the RocksDB client can become unstable.
#[cfg(with_testing)]
pub async fn create_rocks_db_test_store() -> (RocksDbStore, TempDir) {
    let (store_config, dir) = create_rocks_db_test_config().await;
    let namespace = generate_test_namespace();
    let store = RocksDbStore::recreate_and_connect(&store_config, &namespace)
        .await
        .expect("client");
    (store, dir)
}

/// An implementation of [`crate::common::Context`] based on RocksDB
pub type RocksDbContext<E> = ContextFromStore<E, RocksDbStore>;

impl ReadableKeyValueStore<RocksDbStoreError> for RocksDbStore {
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

impl WritableKeyValueStore<RocksDbStoreError> for RocksDbStore {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), RocksDbStoreError> {
        self.store.write_batch(batch, base_key).await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), RocksDbStoreError> {
        self.store.clear_journal(base_key).await
    }
}

impl AdminKeyValueStore for RocksDbStore {
    type Error = RocksDbStoreError;
    type Config = RocksDbStoreConfig;

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, RocksDbStoreError> {
        let store = RocksDbStoreInternal::connect(config, namespace).await?;
        let cache_size = config.common_config.cache_size;
        #[cfg(with_metrics)]
        let store = MeteredStore::new(&ROCKS_DB_METRICS, store);
        let store = ValueSplittingStore::new(store);
        #[cfg(with_metrics)]
        let store = MeteredStore::new(&VALUE_SPLITTING_METRICS, store);
        let store = LruCachingStore::new(store, cache_size);
        #[cfg(with_metrics)]
        let store = MeteredStore::new(&LRU_CACHING_METRICS, store);
        Ok(Self { store })
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

impl KeyValueStore for RocksDbStore {
    type Error = RocksDbStoreError;
}

impl<E: Clone + Send + Sync> RocksDbContext<E> {
    /// Creates a [`RocksDbContext`].
    pub fn new(store: RocksDbStore, base_key: Vec<u8>, extra: E) -> Self {
        Self {
            store,
            base_key,
            extra,
        }
    }
}

/// The error type for [`RocksDbContext`]
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

    /// Invalid table name
    #[error("Invalid table name")]
    InvalidTableName,

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

impl From<RocksDbStoreError> for crate::views::ViewError {
    fn from(error: RocksDbStoreError) -> Self {
        Self::StoreError {
            backend: "rocks_db".to_string(),
            error: error.to_string(),
        }
    }
}
