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
//! Support for ScyllaDB is experimental and is still missing important features:
//! TODO(#935): Read several keys at once
//! TODO(#934): Journaling operations
//!
//! [trait1]: common::KeyValueStoreClient
//! [trait2]: common::Context

use crate::{
    batch::{Batch, WriteOperation},
    common::{
        get_table_name, get_upper_bound_option, CommonStoreConfig, ContextFromDb,
        KeyValueStoreClient, TableStatus,
    },
    lru_caching::{LruCachingKeyValueClient, TEST_CACHE_SIZE},
    value_splitting::DatabaseConsistencyError,
};
use async_lock::{Semaphore, SemaphoreGuard};
use async_trait::async_trait;
use scylla::{
    transport::errors::{DbError, QueryError},
    IntoTypedRows, Session, SessionBuilder,
};
use std::{ops::Deref, sync::Arc};
use thiserror::Error;

/// The creation of a ScyllaDB client that can be used for accessing it.
/// The `Vec<u8>`is a primary key.
type ScyllaDbClientPair = (Session, String);

/// We limit the number of connections that can be done for tests.
pub const TEST_SCYLLA_DB_MAX_CONCURRENT_QUERIES: usize = 10;

/// The number of connections in the stream is limited for tests.
pub const TEST_SCYLLA_DB_MAX_STREAM_QUERIES: usize = 10;

/// The client itself and the keeping of the count of active connections.
#[derive(Clone)]
pub struct ScyllaDbClientInternal {
    client: Arc<ScyllaDbClientPair>,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
}

/// The error type for [`ScyllaDbClientInternal`]
#[derive(Error, Debug)]
pub enum ScyllaDbContextError {
    /// BCS serialization error.
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),

    /// A query error in ScyllaDB
    #[error(transparent)]
    ScyllaDbQueryError(#[from] scylla::transport::errors::QueryError),

    /// A query error in ScyllaDB
    #[error(transparent)]
    ScyllaDbNewSessionError(#[from] scylla::transport::errors::NewSessionError),

    /// Missing database
    #[error("Missing database")]
    MissingDatabase,

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
        match &self.semaphore {
            None => None,
            Some(count) => Some(count.acquire().await),
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

    /// Tests if a table is present in the database or not
    pub async fn test_table_existence(
        session: &Session,
        table_name: &String,
    ) -> Result<bool, ScyllaDbContextError> {
        // We check the way the test can fail. It can fail in different ways.
        let query = format!("SELECT table_name FROM kv.pairs_{}", table_name);

        // Execute the query
        let result = session.query(query, &[]).await;

        // The missing table translates into a very specific error that we matched
        let mesg_miss1 = format!("unconfigured table pairs_{}", table_name);
        let mesg_miss1 = mesg_miss1.as_str();
        let mesg_miss2 = "Undefined name table_name in selection clause";
        let mesg_miss3 = "Keyspace kv does not exist";
        let Err(error) = result else {
            // If ok, then the table exists
            return Ok(true);
        };
        let test = match &error {
            QueryError::DbError(db_error, mesg) => {
                if *db_error != DbError::Invalid {
                    false
                } else {
                    mesg.as_str() == mesg_miss1
                        || mesg.as_str() == mesg_miss2
                        || mesg.as_str() == mesg_miss3
                }
            }
            _ => false,
        };
        if test {
            Ok(false)
        } else {
            Err(ScyllaDbContextError::ScyllaDbQueryError(error))
        }
    }

    /// Creates a table, the keyspace might or might not be existing.
    ///
    /// For the table itself, we return true if the table already exists
    /// and false otherwise.
    /// If `stop_if_table_exists` is true then the existence of a table
    /// triggers an error.
    ///
    /// This is done since if `new` is called two times, `test_table_existence`
    /// might be called two times which would lead to `create_table` called
    /// two times.
    async fn create_table(
        session: &Session,
        table_name: &String,
        stop_if_table_exists: bool,
    ) -> Result<bool, ScyllaDbContextError> {
        // Create a keyspace if it doesn't exist
        let query = "CREATE KEYSPACE IF NOT EXISTS kv WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";
        session.query(query, &[]).await?;

        // Create a table if it doesn't exist
        let query = format!("CREATE TABLE kv.pairs_{} (table_name text, k blob, v blob, primary key (table_name, k))", table_name);
        let result = session.query(query, &[]).await;

        let Err(error) = result else {
            return Ok(false);
        };
        if stop_if_table_exists {
            return Err(error.into());
        }
        let test = match &error {
            QueryError::DbError(DbError::AlreadyExists { keyspace, table }, mesg) => {
                keyspace == "kv"
                    && *table == format!("table_{}", table_name)
                    && mesg.starts_with("Cannot add already existing table")
            }
            _ => false,
        };
        if test {
            Ok(true)
        } else {
            Err(error.into())
        }
    }

    #[cfg(any(test, feature = "test"))]
    async fn new_for_testing(
        uri: &str,
        table_name: String,
        common_config: CommonStoreConfig,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let session = SessionBuilder::new().known_node(uri).build().await?;
        if Self::test_table_existence(&session, &table_name).await? {
            let query = format!("DROP TABLE kv.pairs_{};", table_name);
            session.query(query, &[]).await?;
        }
        let stop_if_table_exists = true;
        Self::new_internal(session, table_name, common_config, stop_if_table_exists).await
    }

    async fn new(
        uri: &str,
        table_name: String,
        common_config: CommonStoreConfig,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let session = SessionBuilder::new().known_node(uri).build().await?;
        let stop_if_table_exists = false;
        Self::new_internal(session, table_name, common_config, stop_if_table_exists).await
    }

    async fn new_internal(
        session: Session,
        table_name: String,
        common_config: CommonStoreConfig,
        stop_if_table_exists: bool,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        // Create a session builder and specify the ScyllaDB contact points
        let mut existing_table = Self::test_table_existence(&session, &table_name).await?;
        if !existing_table {
            if common_config.create_if_missing {
                existing_table =
                    Self::create_table(&session, &table_name, stop_if_table_exists).await?;
            } else {
                return Err(ScyllaDbContextError::MissingDatabase);
            }
        }
        let table_status = if existing_table {
            TableStatus::Existing
        } else {
            TableStatus::New
        };
        let client = (session, table_name);
        let client = Arc::new(client);
        let semaphore = common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = common_config.max_stream_queries;
        let client = Self {
            client,
            semaphore,
            max_stream_queries,
        };
        Ok((client, table_status))
    }
}

/// A shared DB client for ScyllaDB implementing LruCaching
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
    /// Gets the table name of a client
    pub async fn get_table_name(&self) -> String {
        self.client.client.get_table_name().await
    }

    /// Creates a [`ScyllaDbClient`] from the input parameters.
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        uri: &str,
        namespace: String,
        common_config: CommonStoreConfig,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let (client, table_status) =
            ScyllaDbClientInternal::new_for_testing(uri, namespace, common_config.clone()).await?;
        let client = LruCachingKeyValueClient::new(client, common_config.cache_size);
        let client = ScyllaDbClient { client };
        Ok((client, table_status))
    }

    /// Creates a [`ScyllaDbClient`] from the input parameters.
    pub async fn new(
        uri: &str,
        namespace: String,
        common_config: CommonStoreConfig,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let (client, table_status) =
            ScyllaDbClientInternal::new(uri, namespace, common_config.clone()).await?;
        let client = LruCachingKeyValueClient::new(client, common_config.cache_size);
        let client = ScyllaDbClient { client };
        Ok((client, table_status))
    }
}

/// Creates the common initialization for RocksDB
#[cfg(any(test, feature = "test"))]
pub fn create_scylla_db_common_config() -> CommonStoreConfig {
    CommonStoreConfig {
        max_concurrent_queries: Some(TEST_SCYLLA_DB_MAX_CONCURRENT_QUERIES),
        max_stream_queries: TEST_SCYLLA_DB_MAX_STREAM_QUERIES,
        cache_size: TEST_CACHE_SIZE,
        create_if_missing: true,
    }
}

/// Creates a ScyllaDB test client
#[cfg(any(test, feature = "test"))]
pub async fn create_scylla_db_test_client() -> ScyllaDbClient {
    let uri = "localhost:9042";
    let table_name = get_table_name().await;
    let common_config = create_scylla_db_common_config();
    let (client, _) = ScyllaDbClient::new_for_testing(uri, table_name, common_config)
        .await
        .expect("client");
    client
}

/// An implementation of [`crate::common::Context`] based on ScyllaDB
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
#[cfg(any(test, feature = "test"))]
pub async fn create_scylla_db_test_context() -> ScyllaDbContext<()> {
    let base_key = vec![];
    let db = create_scylla_db_test_client().await;
    ScyllaDbContext::new(db, base_key, ())
}
