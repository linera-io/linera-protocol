// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This provides a KeyValueStoreClient for the ScyllaDB database.
//! The code is functional but some aspects are missing.
//!
//! The current connection is done via a Session and a corresponding
//! primary key that we name `table_name`. The maximum number of
//! concurrent queries is controlled by max_concurrent_queries.
//!
//! We thus implement the
//! * [`KeyValueStoreClient`][trait1] for a database access.
//! * [`Context`][trait2] for the context.
//!
//! Support for ScyllaDb is experimental and is still missing important features:
//! TODO(#935): Read several keys at once
//! TODO(#934): Journaling operations
//!
//! [trait1]: common::KeyValueStoreClient
//! [trait2]: common::Context

use crate::{
    batch::{Batch, WriteOperation},
    common::{get_upper_bound_option, ContextFromDb, KeyValueStoreClient},
    lru_caching::{LruCachingKeyValueClient, TEST_CACHE_SIZE},
    value_splitting::DatabaseConsistencyError,
};
use async_lock::{RwLock, Semaphore, SemaphoreGuard};
use async_trait::async_trait;
use lazy_static::lazy_static;
use scylla::{IntoTypedRows, Session, SessionBuilder};
use std::{ops::Deref, sync::Arc};
use thiserror::Error;

lazy_static! {
    static ref TEST_COUNTER: Arc<RwLock<usize>> = Arc::new(RwLock::new(0));
}

/// The creation of a ScyllaDb client that can be used for accessing it.
/// The `Vec<u8>`is a primary key.
type ScyllaDbClientPair = (Session, String);

/// We limit the number of connections that can be done for tests.
pub const TEST_SCYLLA_DB_MAX_CONCURRENT_QUERIES: usize = 10;

/// The number of connection in the stream is limited for tesst.
pub const TEST_SCYLLA_DB_MAX_STREAM_QUERIES: usize = 10;

/// The client itself and the keeping of the count of active connections.
#[derive(Clone)]
pub struct ScyllaDbClientInternal {
    client: Arc<ScyllaDbClientPair>,
    count: Arc<Semaphore>,
    max_concurrent_queries: Option<usize>,
    max_stream_queries: usize,
}

/// The error type for [`ScyllaDbClientInternal`]
#[derive(Error, Debug)]
pub enum ScyllaDbContextError {
    /// BCS serialization error.
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),

    /// A query error in ScyllaDb
    #[error(transparent)]
    ScyllaDbQueryError(#[from] scylla::transport::errors::QueryError),

    /// A query error in ScyllaDb
    #[error(transparent)]
    ScyllaDbNewSessionError(#[from] scylla::transport::errors::NewSessionError),

    /// The database is not coherent
    #[error(transparent)]
    DatabaseConsistencyError(#[from] DatabaseConsistencyError),
}

impl From<ScyllaDbContextError> for crate::views::ViewError {
    fn from(error: ScyllaDbContextError) -> Self {
        Self::ContextError {
            backend: "scylla_db".to_string(),
            error: error.to_string(),
        }
    }
}

#[async_trait]
impl KeyValueStoreClient for ScyllaDbClientInternal {
    const MAX_VALUE_SIZE: usize = usize::MAX;
    type Error = ScyllaDbContextError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let client = self.client.deref();
        let _guard = self.acquire().await;
        Self::read_key_internal(client, key.to_vec()).await
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        // There is probably a better way in ScyllaDB for downloading several keys than this one.
        let client = self.client.deref();
        let _guard = self.acquire().await;
        let mut values = Vec::new();
        for key in keys {
            let value = Self::read_key_internal(client, key.to_vec()).await?;
            values.push(value);
        }
        Ok(values)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        let client = self.client.deref();
        let _guard = self.acquire().await;
        Self::find_keys_by_prefix_internal(client, key_prefix.to_vec()).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        let client = self.client.deref();
        let _guard = self.acquire().await;
        Self::find_key_values_by_prefix_internal(client, key_prefix.to_vec()).await
    }

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), Self::Error> {
        let client = self.client.deref();
        let _guard = self.acquire().await;
        Self::write_batch_internal(client, batch).await
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl ScyllaDbClientInternal {
    /// Obtains the semaphore lock on the database if needed.
    async fn acquire(&self) -> Option<SemaphoreGuard<'_>> {
        match self.max_concurrent_queries {
            None => None,
            Some(_max_concurrent_queries) => Some(self.count.acquire().await),
        }
    }

    async fn read_key_internal(
        client: &ScyllaDbClientPair,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ScyllaDbContextError> {
        let session = &client.0;
        let table_name = &client.1;
        // Read the value of a key
        let values = (table_name.to_string(), key);
        let query = format!(
            "SELECT v FROM kv.pairs_{} WHERE table_name = ? AND k = ?",
            table_name
        );
        let rows = session.query(query, values).await?;
        if let Some(rows) = rows.rows {
            if let Some(row) = rows.into_typed::<(Vec<u8>,)>().next() {
                let value = row.unwrap();
                return Ok(Some(value.0));
            }
        }
        Ok(None)
    }

    async fn insert_key_value_internal(
        client: &ScyllaDbClientPair,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), ScyllaDbContextError> {
        let session = &client.0;
        let table_name = &client.1;
        let query = format!(
            "INSERT INTO kv.pairs_{} (table_name, k, v) VALUES (?, ?, ?)",
            table_name
        );
        let values = (table_name.to_string(), key, value);
        session.query(query, values).await?;
        Ok(())
    }

    async fn delete_key_internal(
        client: &ScyllaDbClientPair,
        key: Vec<u8>,
    ) -> Result<(), ScyllaDbContextError> {
        let session = &client.0;
        let table_name = &client.1;
        let values = (table_name.to_string(), key);
        let query = format!(
            "DELETE FROM kv.pairs_{} WHERE table_name = ? AND k = ?",
            table_name
        );
        session.query(query, values).await?;
        Ok(())
    }

    async fn delete_key_prefix_internal(
        client: &ScyllaDbClientPair,
        key_prefix: Vec<u8>,
    ) -> Result<(), ScyllaDbContextError> {
        let session = &client.0;
        let table_name = &client.1;
        match get_upper_bound_option(&key_prefix) {
            None => {
                let values = (table_name.to_string(), key_prefix);
                let query = format!(
                    "DELETE FROM kv.pairs_{} WHERE table_name = ? AND k >= ?",
                    table_name
                );
                session.query(query, values).await?;
            }
            Some(upper_bound) => {
                let values = (table_name.to_string(), key_prefix, upper_bound);
                let query = format!(
                    "DELETE FROM kv.pairs_{} WHERE table_name = ? AND k >= ? AND k < ?",
                    table_name
                );
                session.query(query, values).await?;
            }
        }
        Ok(())
    }

    async fn write_batch_internal(
        client: &ScyllaDbClientPair,
        batch: Batch,
    ) -> Result<(), ScyllaDbContextError> {
        for ent in batch.operations {
            match ent {
                WriteOperation::Put { key, value } => {
                    Self::insert_key_value_internal(client, key, value).await?;
                }
                WriteOperation::Delete { key } => {
                    Self::delete_key_internal(client, key).await?;
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    Self::delete_key_prefix_internal(client, key_prefix).await?;
                }
            }
        }
        Ok(())
    }

    async fn find_keys_by_prefix_internal(
        client: &ScyllaDbClientPair,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, ScyllaDbContextError> {
        let session = &client.0;
        let table_name = &client.1;
        // Read the value of a key
        let len = key_prefix.len();
        let rows = match get_upper_bound_option(&key_prefix) {
            None => {
                let values = (table_name.to_string(), key_prefix);
                let query = format!(
                    "SELECT k FROM kv.pairs_{} WHERE table_name = ? AND k >= ?",
                    table_name
                );
                session.query(query, values).await?
            }
            Some(upper_bound) => {
                let values = (table_name.to_string(), key_prefix, upper_bound);
                let query = format!(
                    "SELECT k FROM kv.pairs_{} WHERE table_name = ? AND k >= ? AND k < ?",
                    table_name
                );
                session.query(query, values).await?
            }
        };
        let mut keys = Vec::new();
        if let Some(rows) = rows.rows {
            for row in rows.into_typed::<(Vec<u8>,)>() {
                let key = row.unwrap();
                let short_key = key.0[len..].to_vec();
                keys.push(short_key);
            }
        }
        Ok(keys)
    }

    async fn find_key_values_by_prefix_internal(
        client: &ScyllaDbClientPair,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ScyllaDbContextError> {
        let session = &client.0;
        let table_name = &client.1;
        // Read the value of a key
        let len = key_prefix.len();
        let rows = match get_upper_bound_option(&key_prefix) {
            None => {
                let values = (table_name.to_string(), key_prefix);
                let query = format!(
                    "SELECT k FROM kv.pairs_{} WHERE table_name = ? AND k >= ?",
                    table_name
                );
                session.query(query, values).await?
            }
            Some(upper_bound) => {
                let values = (table_name.to_string(), key_prefix, upper_bound);
                let query = format!(
                    "SELECT k,v FROM kv.pairs_{} WHERE table_name = ? AND k >= ? AND k < ?",
                    table_name
                );
                session.query(query, values).await?
            }
        };
        let mut key_values = Vec::new();
        if let Some(rows) = rows.rows {
            for row in rows.into_typed::<(Vec<u8>, Vec<u8>)>() {
                let key = row.unwrap();
                let short_key = key.0[len..].to_vec();
                key_values.push((short_key, key.1));
            }
        }
        Ok(key_values)
    }

    /// Retrieves the table_name from the client.
    pub async fn get_table_name(&self) -> String {
        let client = self.client.deref();
        client.1.clone()
    }

    async fn new(
        restart_database: bool,
        uri: &str,
        table_name: String,
        max_concurrent_queries: Option<usize>,
        max_stream_queries: usize,
    ) -> Result<Self, ScyllaDbContextError> {
        // Create a session builder and specify the ScyllaDB contact points
        let session = SessionBuilder::new().known_node(uri).build().await?;

        if restart_database {
            // Create a keyspace if it doesn't exist
            session
                .query(
                    "CREATE KEYSPACE IF NOT EXISTS kv WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
                    &[],
                )
                .await?;

            // Dropping the table
            let query = format!("DROP TABLE IF EXISTS kv.pairs_{};", table_name);
            session.query(query, &[]).await?;

            // Create a table if it doesn't exist
            let query = format!("CREATE TABLE kv.pairs_{} (table_name text, k blob, v blob, primary key (table_name, k))", table_name);
            session.query(query, &[]).await?;
        }
        let client = (session, table_name);
        let client = Arc::new(client);
        let n = max_concurrent_queries.unwrap_or(1);
        let count = Arc::new(Semaphore::new(n));
        Ok(ScyllaDbClientInternal {
            client,
            count,
            max_concurrent_queries,
            max_stream_queries,
        })
    }
}

/// A shared DB client for ScyllaDb implementing LruCaching
#[derive(Clone)]
pub struct ScyllaDbClient {
    client: LruCachingKeyValueClient<ScyllaDbClientInternal>,
}

#[async_trait]
impl KeyValueStoreClient for ScyllaDbClient {
    const MAX_VALUE_SIZE: usize = ScyllaDbClientInternal::MAX_VALUE_SIZE;
    type Error = <ScyllaDbClientInternal as KeyValueStoreClient>::Error;
    type Keys = <ScyllaDbClientInternal as KeyValueStoreClient>::Keys;
    type KeyValues = <ScyllaDbClientInternal as KeyValueStoreClient>::KeyValues;

    fn max_stream_queries(&self) -> usize {
        self.client.max_stream_queries()
    }

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.client.read_key_bytes(key).await
    }

    async fn read_multi_key_bytes(
        &self,
        key: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.client.read_multi_key_bytes(key).await
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        self.client.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        self.client.find_key_values_by_prefix(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), Self::Error> {
        self.client.write_batch(batch, base_key).await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Self::Error> {
        self.client.clear_journal(base_key).await
    }
}

impl ScyllaDbClient {
    /// Get the table_name of a client
    pub async fn get_table_name(&self) -> String {
        self.client.client.get_table_name().await
    }

    /// Creates a [`ScyllaDbClient`] from the input parameters.
    pub async fn new(
        restart_database: bool,
        uri: &str,
        namespace: String,
        max_concurrent_queries: Option<usize>,
        max_stream_queries: usize,
        cache_size: usize,
    ) -> Result<Self, ScyllaDbContextError> {
        let client = ScyllaDbClientInternal::new(
            restart_database,
            uri,
            namespace,
            max_concurrent_queries,
            max_stream_queries,
        )
        .await?;
        let client = LruCachingKeyValueClient::new(client, cache_size);
        Ok(ScyllaDbClient { client })
    }
}

/// Get a test table_name
pub async fn get_table_name() -> String {
    let mut counter = TEST_COUNTER.write().await;
    *counter += 1;
    format!("test_table_{}", *counter)
}

/// Creates a ScyllaDb test client
pub async fn create_scylla_db_test_client() -> ScyllaDbClient {
    let restart_database = true;
    let uri = "localhost:9042";
    let table_name = get_table_name().await;
    let max_concurrent_queries = Some(TEST_SCYLLA_DB_MAX_CONCURRENT_QUERIES);
    let max_stream_queries = TEST_SCYLLA_DB_MAX_STREAM_QUERIES;
    let cache_size = TEST_CACHE_SIZE;
    ScyllaDbClient::new(
        restart_database,
        uri,
        table_name,
        max_concurrent_queries,
        max_stream_queries,
        cache_size,
    )
    .await
    .expect("client")
}

/// An implementation of [`crate::common::Context`] based on RocksDB
pub type ScyllaDbContext<E> = ContextFromDb<E, ScyllaDbClient>;

impl<E: Clone + Send + Sync> ScyllaDbContext<E> {
    /// Creates a [`ScyllaDbContext`].
    pub fn new(db: ScyllaDbClient, base_key: Vec<u8>, extra: E) -> Self {
        Self {
            db,
            base_key,
            extra,
        }
    }
}

/// Creates a [`crate::common::Context`] for testing purposes.
pub async fn create_scylla_db_test_context() -> ScyllaDbContext<()> {
    let base_key = vec![];
    let db = create_scylla_db_test_client().await;
    ScyllaDbContext::new(db, base_key, ())
}
