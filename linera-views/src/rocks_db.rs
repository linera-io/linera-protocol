// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::{Batch, WriteOperation},
    common::{get_upper_bound, CommonStoreConfig, ContextFromDb, KeyValueStoreClient, TableStatus},
    lru_caching::LruCachingKeyValueClient,
    value_splitting::{DatabaseConsistencyError, ValueSplittingKeyValueStoreClient},
};
use async_trait::async_trait;
use linera_base::ensure;
use std::{
    fs,
    ops::{Bound, Bound::Excluded},
    path::PathBuf,
    sync::Arc,
};
use thiserror::Error;
#[cfg(any(test, feature = "test"))]
use {crate::lru_caching::TEST_CACHE_SIZE, tempfile::TempDir};

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
pub struct RocksDbClientInternal {
    db: Arc<DB>,
    max_stream_queries: usize,
}

/// The initial configuration of the system
#[derive(Clone, Debug)]
pub struct RocksDbKvStoreConfig {
    /// The AWS configuration
    pub path_buf: PathBuf,
    /// The common configuration of the key value store
    pub common_config: CommonStoreConfig,
}

#[async_trait]
impl KeyValueStoreClient for RocksDbClientInternal {
    const MAX_VALUE_SIZE: usize = MAX_VALUE_SIZE;
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Error = RocksDbContextError;
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

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// A shared DB client for RocksDB implementing LruCaching
#[derive(Clone)]
pub struct RocksDbClient {
    client: LruCachingKeyValueClient<ValueSplittingKeyValueStoreClient<RocksDbClientInternal>>,
}

impl RocksDbClient {
    /// Returns whether a table already exists.
    pub async fn test_existence(
        store_config: RocksDbKvStoreConfig,
    ) -> Result<bool, RocksDbContextError> {
        let options = rocksdb::Options::default();
        let result = DB::open(&options, store_config.path_buf.clone());
        match result {
            Ok(_) => Ok(true),
            Err(error) => match error.kind() {
                rocksdb::ErrorKind::InvalidArgument => Ok(false),
                _ => Err(RocksDbContextError::RocksDb(error)),
            },
        }
    }

    /// Creates a RocksDB database for unit tests from a specified path.
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        store_config: RocksDbKvStoreConfig,
    ) -> Result<(RocksDbClient, TableStatus), RocksDbContextError> {
        let path = store_config.path_buf.as_path();
        fs::remove_dir_all(path)?;
        let create_if_missing = true;
        Self::new_internal(store_config, create_if_missing).await
    }

    /// Creates all RocksDB databases
    pub async fn delete_all(store_config: RocksDbKvStoreConfig) -> Result<(), RocksDbContextError> {
        let path = store_config.path_buf.as_path();
        fs::remove_dir_all(path)?;
        Ok(())
    }

    /// Creates all RocksDB databases
    pub async fn delete_single(
        store_config: RocksDbKvStoreConfig,
    ) -> Result<(), RocksDbContextError> {
        let path = store_config.path_buf.as_path();
        fs::remove_dir_all(path)?;
        Ok(())
    }

    /// Creates a RocksDB database from a specified path.
    pub async fn new(
        store_config: RocksDbKvStoreConfig,
    ) -> Result<(RocksDbClient, TableStatus), RocksDbContextError> {
        let create_if_missing = false;
        Self::new_internal(store_config, create_if_missing).await
    }

    /// Initializes a RocksDB database from a specified path.
    pub async fn initialize(
        store_config: RocksDbKvStoreConfig,
    ) -> Result<Self, RocksDbContextError> {
        let create_if_missing = true;
        let (client, table_status) = Self::new_internal(store_config, create_if_missing).await?;
        if table_status == TableStatus::Existing {
            return Err(RocksDbContextError::AlreadyExistingDatabase);
        }
        Ok(client)
    }

    /// Creates a RocksDB database from a specified path.
    async fn new_internal(
        store_config: RocksDbKvStoreConfig,
        create_if_missing: bool,
    ) -> Result<(RocksDbClient, TableStatus), RocksDbContextError> {
        let kv_name = format!(
            "store_config={:?} create_if_missing={:?}",
            store_config, create_if_missing
        );
        let path = store_config.path_buf.as_path();
        let cache_size = store_config.common_config.cache_size;
        let max_stream_queries = store_config.common_config.max_stream_queries;
        let mut options = rocksdb::Options::default();
        let test = Self::test_existence(store_config.clone()).await?;
        let table_status = if test {
            TableStatus::Existing
        } else {
            TableStatus::New
        };
        let db = if create_if_missing {
            options.create_if_missing(true);
            DB::open(&options, path)?
        } else {
            if !test {
                tracing::info!("RocksDb: Missing database for kv_name={}", kv_name);
                return Err(RocksDbContextError::MissingDatabase(kv_name));
            }
            DB::open(&options, path)?
        };
        let client = RocksDbClientInternal {
            db: Arc::new(db),
            max_stream_queries,
        };
        let client = ValueSplittingKeyValueStoreClient::new(client);
        let client = Self {
            client: LruCachingKeyValueClient::new(client, cache_size),
        };
        Ok((client, table_status))
    }
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

/// Creates a RocksDB database client to be used for tests.
#[cfg(any(test, feature = "test"))]
pub async fn create_rocks_db_test_client() -> RocksDbClient {
    let dir = TempDir::new().unwrap();
    let path_buf = dir.path().to_path_buf();
    let common_config = create_rocks_db_common_config();
    let store_config = RocksDbKvStoreConfig {
        path_buf,
        common_config,
    };
    let (client, _) = RocksDbClient::new_for_testing(store_config)
        .await
        .expect("client");
    client
}

/// An implementation of [`crate::common::Context`] based on RocksDB
pub type RocksDbContext<E> = ContextFromDb<E, RocksDbClient>;

#[async_trait]
impl KeyValueStoreClient for RocksDbClient {
    const MAX_VALUE_SIZE: usize = usize::MAX;
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Error = RocksDbContextError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.client.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksDbContextError> {
        self.client.read_value_bytes(key).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, RocksDbContextError> {
        self.client.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, RocksDbContextError> {
        self.client.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, RocksDbContextError> {
        self.client.find_key_values_by_prefix(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), RocksDbContextError> {
        self.client.write_batch(batch, base_key).await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Self::Error> {
        self.client.clear_journal(base_key).await
    }
}

impl<E: Clone + Send + Sync> RocksDbContext<E> {
    /// Creates a [`RocksDbContext`].
    pub fn new(db: RocksDbClient, base_key: Vec<u8>, extra: E) -> Self {
        Self {
            db,
            base_key,
            extra,
        }
    }
}

/// Create a [`crate::common::Context`] that can be used for tests.
#[cfg(any(test, feature = "test"))]
pub async fn create_rocks_db_test_context() -> RocksDbContext<()> {
    let client = create_rocks_db_test_client().await;
    let base_key = vec![];
    RocksDbContext::new(client, base_key, ())
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

    /// The key must have at most 8M
    #[error("The key must have at most 8M")]
    KeyTooLong,

    /// Missing database
    #[error("Missing database")]
    MissingDatabase(String),

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
