// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::{Batch, WriteOperation},
    common::{get_upper_bound, ContextFromDb, KeyValueStoreClient},
    lru_caching::{LruCachingKeyValueClient, TEST_CACHE_SIZE},
    value_splitting::{DatabaseConsistencyError, ValueSplittingKeyValueStoreClient},
};
use async_trait::async_trait;
use std::{
    ops::{Bound, Bound::Excluded},
    path::Path,
    sync::Arc,
};
use tempfile::TempDir;
use thiserror::Error;

/// The number of streams for the test
pub const TEST_ROCKS_DB_MAX_STREAM_QUERIES: usize = 10;

// The maximum size of values in RocksDB is 3 GB
// That is 3221225472 and so for offset reason we decrease by 400
const MAX_VALUE_SIZE: usize = 3221225072;

/// The RocksDB client that we use.
pub type DB = rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>;

/// The internal client
#[derive(Clone)]
pub struct RocksDbClientInternal {
    db: Arc<DB>,
    max_stream_queries: usize,
}

#[async_trait]
impl KeyValueStoreClient for RocksDbClientInternal {
    const MAX_VALUE_SIZE: usize = MAX_VALUE_SIZE;
    type Error = RocksDbContextError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksDbContextError> {
        let client = self.clone();
        let key = key.to_vec();
        Ok(tokio::task::spawn_blocking(move || client.db.get(&key)).await??)
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, RocksDbContextError> {
        let client = self.clone();
        let entries = tokio::task::spawn_blocking(move || client.db.multi_get(&keys)).await?;
        Ok(entries.into_iter().collect::<Result<_, _>>()?)
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, RocksDbContextError> {
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
                    WriteOperation::Delete { key } => inner_batch.delete(&key),
                    WriteOperation::Put { key, value } => inner_batch.put(&key, value),
                    WriteOperation::DeletePrefix { key_prefix } => {
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
    /// Creates a RocksDB database from a specified path.
    pub fn new<P: AsRef<Path>>(
        path: P,
        max_stream_queries: usize,
        cache_size: usize,
    ) -> RocksDbClient {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, path).unwrap();
        let client = RocksDbClientInternal {
            db: Arc::new(db),
            max_stream_queries,
        };
        let client = ValueSplittingKeyValueStoreClient::new(client);
        Self {
            client: LruCachingKeyValueClient::new(client, cache_size),
        }
    }
}

/// Creates a RocksDB database client to be used for tests.
pub fn create_rocks_db_test_client() -> RocksDbClient {
    let dir = TempDir::new().unwrap();
    RocksDbClient::new(dir, TEST_ROCKS_DB_MAX_STREAM_QUERIES, TEST_CACHE_SIZE)
}

/// An implementation of [`crate::common::Context`] based on RocksDB
pub type RocksDbContext<E> = ContextFromDb<E, RocksDbClient>;

#[async_trait]
impl KeyValueStoreClient for RocksDbClient {
    const MAX_VALUE_SIZE: usize = usize::MAX;
    type Error = RocksDbContextError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.client.max_stream_queries()
    }

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksDbContextError> {
        self.client.read_key_bytes(key).await
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, RocksDbContextError> {
        self.client.read_multi_key_bytes(keys).await
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
pub fn create_rocks_db_test_context() -> RocksDbContext<()> {
    let client = create_rocks_db_test_client();
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
