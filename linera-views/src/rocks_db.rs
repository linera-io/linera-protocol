// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::{Batch, WriteOperation},
    common::{
        get_upper_bound, AdminKeyValueStore, CommonStoreConfig, ContextFromStore, KeyValueStore,
        ReadableKeyValueStore, WritableKeyValueStore,
    },
    lru_caching::LruCachingStore,
    value_splitting::{DatabaseConsistencyError, ValueSplittingStore},
};
use async_trait::async_trait;
use linera_base::ensure;
use std::{
    ffi::OsString,
    ops::{Bound, Bound::Excluded},
    path::PathBuf,
    sync::Arc,
};
use thiserror::Error;

#[cfg(feature = "metrics")]
use crate::metering::{
    MeteredStore, LRU_CACHING_METRICS, ROCKS_DB_METRICS, VALUE_SPLITTING_METRICS,
};

#[cfg(any(test, feature = "test"))]
use {
    crate::{lru_caching::TEST_CACHE_SIZE, test_utils::generate_test_namespace},
    tempfile::TempDir,
};

/// The number of streams for the test
#[cfg(any(test, feature = "test"))]
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
    fn check_namespace(namespace: &str) -> Result<(), RocksDbContextError> {
        if !namespace
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return Err(RocksDbContextError::InvalidTableName);
        }
        Ok(())
    }
}

#[async_trait]
impl ReadableKeyValueStore<RocksDbContextError> for RocksDbStoreInternal {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksDbContextError> {
        ensure!(key.len() <= MAX_KEY_SIZE, RocksDbContextError::KeyTooLong);
        let client = self.clone();
        let key = key.to_vec();
        Ok(tokio::task::spawn_blocking(move || client.db.get(&key)).await??)
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, RocksDbContextError> {
        ensure!(key.len() <= MAX_KEY_SIZE, RocksDbContextError::KeyTooLong);
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

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, RocksDbContextError> {
        for key in &keys {
            ensure!(key.len() <= MAX_KEY_SIZE, RocksDbContextError::KeyTooLong);
        }
        let client = self.clone();
        let entries = tokio::task::spawn_blocking(move || client.db.multi_get(&keys)).await?;
        Ok(entries.into_iter().collect::<Result<_, _>>()?)
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, RocksDbContextError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            RocksDbContextError::KeyTooLong
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
    ) -> Result<Self::KeyValues, RocksDbContextError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            RocksDbContextError::KeyTooLong
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

#[async_trait]
impl WritableKeyValueStore<RocksDbContextError> for RocksDbStoreInternal {
    const MAX_VALUE_SIZE: usize = MAX_VALUE_SIZE;

    async fn write_batch(
        &self,
        mut batch: Batch,
        _base_key: &[u8],
    ) -> Result<(), RocksDbContextError> {
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
        tokio::task::spawn_blocking(move || -> Result<(), RocksDbContextError> {
            let mut inner_batch = rocksdb::WriteBatchWithTransaction::default();
            for operation in batch.operations {
                match operation {
                    WriteOperation::Delete { key } => {
                        ensure!(key.len() <= MAX_KEY_SIZE, RocksDbContextError::KeyTooLong);
                        inner_batch.delete(&key)
                    }
                    WriteOperation::Put { key, value } => {
                        ensure!(key.len() <= MAX_KEY_SIZE, RocksDbContextError::KeyTooLong);
                        inner_batch.put(&key, value)
                    }
                    WriteOperation::DeletePrefix { key_prefix } => {
                        ensure!(
                            key_prefix.len() <= MAX_KEY_SIZE,
                            RocksDbContextError::KeyTooLong
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

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), RocksDbContextError> {
        Ok(())
    }
}

#[async_trait]
impl AdminKeyValueStore<RocksDbContextError> for RocksDbStoreInternal {
    type Config = RocksDbStoreConfig;

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, RocksDbContextError> {
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

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, RocksDbContextError> {
        let entries = std::fs::read_dir(config.path_buf.clone())?;
        let mut namespaces = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                return Err(RocksDbContextError::NonDirectoryNamespace);
            }
            let namespace = match entry.file_name().into_string() {
                Err(error) => {
                    return Err(RocksDbContextError::IntoStringError(error));
                }
                Ok(namespace) => namespace,
            };
            namespaces.push(namespace);
        }
        Ok(namespaces)
    }

    async fn delete_all(config: &Self::Config) -> Result<(), RocksDbContextError> {
        let namespaces = RocksDbStoreInternal::list_all(config).await?;
        for namespace in namespaces {
            let mut path_buf = config.path_buf.clone();
            path_buf.push(&namespace);
            std::fs::remove_dir_all(path_buf.as_path())?;
        }
        Ok(())
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, RocksDbContextError> {
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
                _ => Err(RocksDbContextError::RocksDb(error)),
            },
        }
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), RocksDbContextError> {
        Self::check_namespace(namespace)?;
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let mut path_buf = config.path_buf.clone();
        path_buf.push(namespace);
        let _db = DB::open(&options, path_buf)?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), RocksDbContextError> {
        Self::check_namespace(namespace)?;
        let mut path_buf = config.path_buf.clone();
        path_buf.push(namespace);
        let path = path_buf.as_path();
        std::fs::remove_dir_all(path)?;
        Ok(())
    }
}

impl KeyValueStore for RocksDbStoreInternal {
    type Error = RocksDbContextError;
}

/// A shared DB client for RocksDB implementing LruCaching
#[derive(Clone)]
pub struct RocksDbStore {
    #[cfg(feature = "metrics")]
    store: MeteredStore<
        LruCachingStore<MeteredStore<ValueSplittingStore<MeteredStore<RocksDbStoreInternal>>>>,
    >,
    #[cfg(not(feature = "metrics"))]
    store: LruCachingStore<ValueSplittingStore<RocksDbStoreInternal>>,
}

/// Creates the common initialization for RocksDB
#[cfg(any(test, feature = "test"))]
pub fn create_rocks_db_common_config() -> CommonStoreConfig {
    CommonStoreConfig {
        max_concurrent_queries: None,
        max_stream_queries: TEST_ROCKS_DB_MAX_STREAM_QUERIES,
        cache_size: TEST_CACHE_SIZE,
    }
}

/// Returns the test config and a guard for the temporary directory
#[cfg(any(test, feature = "test"))]
pub async fn create_rocks_db_test_config() -> (RocksDbStoreConfig, TempDir) {
    let dir = TempDir::new().unwrap();
    let path_buf = dir.path().to_path_buf();
    let common_config = create_rocks_db_common_config();
    let store_config = RocksDbStoreConfig {
        path_buf,
        common_config,
    };
    (store_config, dir)
}

/// Creates a RocksDB database client to be used for tests.
/// The temporary directory has to be carried because if it goes
/// out of scope then the RocksDB client can become unstable.
#[cfg(any(test, feature = "test"))]
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

#[async_trait]
impl ReadableKeyValueStore<RocksDbContextError> for RocksDbStore {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksDbContextError> {
        self.store.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, RocksDbContextError> {
        self.store.contains_key(key).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, RocksDbContextError> {
        self.store.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, RocksDbContextError> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, RocksDbContextError> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

#[async_trait]
impl WritableKeyValueStore<RocksDbContextError> for RocksDbStore {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), RocksDbContextError> {
        self.store.write_batch(batch, base_key).await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), RocksDbContextError> {
        self.store.clear_journal(base_key).await
    }
}

#[async_trait]
impl AdminKeyValueStore<RocksDbContextError> for RocksDbStore {
    type Config = RocksDbStoreConfig;

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, RocksDbContextError> {
        let store = RocksDbStoreInternal::connect(config, namespace).await?;
        let cache_size = config.common_config.cache_size;
        #[cfg(feature = "metrics")]
        let store = MeteredStore::new(&ROCKS_DB_METRICS, store);
        let store = ValueSplittingStore::new(store);
        #[cfg(feature = "metrics")]
        let store = MeteredStore::new(&VALUE_SPLITTING_METRICS, store);
        let store = LruCachingStore::new(store, cache_size);
        #[cfg(feature = "metrics")]
        let store = MeteredStore::new(&LRU_CACHING_METRICS, store);
        Ok(Self { store })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, RocksDbContextError> {
        RocksDbStoreInternal::list_all(config).await
    }

    async fn delete_all(config: &Self::Config) -> Result<(), RocksDbContextError> {
        RocksDbStoreInternal::delete_all(config).await
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, RocksDbContextError> {
        RocksDbStoreInternal::exists(config, namespace).await
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), RocksDbContextError> {
        RocksDbStoreInternal::create(config, namespace).await
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), RocksDbContextError> {
        RocksDbStoreInternal::delete(config, namespace).await
    }
}

#[async_trait]
impl KeyValueStore for RocksDbStore {
    type Error = RocksDbContextError;
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
pub enum RocksDbContextError {
    /// Tokio join error in RocksDb.
    #[error("tokio join error: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

    /// RocksDb error.
    #[error("RocksDb error: {0}")]
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

impl From<RocksDbContextError> for crate::views::ViewError {
    fn from(error: RocksDbContextError) -> Self {
        Self::ContextError {
            backend: "rocks_db".to_string(),
            error: error.to_string(),
        }
    }
}
