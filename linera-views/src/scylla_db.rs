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
//! [trait1]: common::KeyValueStoreClient
//! [trait2]: common::Context

#[cfg(any(test, feature = "test"))]
use crate::{lru_caching::TEST_CACHE_SIZE, test_utils::get_table_name};

use crate::{
    batch::{Batch, DeletePrefixExpander},
    common::{
        get_upper_bound_option, CommonStoreConfig, ContextFromDb, KeyValueStoreClient, TableStatus,
    },
    lru_caching::LruCachingKeyValueClient,
    value_splitting::DatabaseConsistencyError,
};
use async_lock::{Semaphore, SemaphoreGuard};
use async_trait::async_trait;
use futures::future::join_all;
use scylla::{
    frame::request::batch::BatchType,
    query::Query,
    transport::errors::{DbError, QueryError},
    IntoTypedRows, Session, SessionBuilder,
};
use std::{ops::Deref, sync::Arc};
use thiserror::Error;

/// The creation of a ScyllaDB client that can be used for accessing it.
/// The `Vec<u8>`is a primary key.
type ScyllaDbClientPair = (Session, String);

/// We limit the number of connections that can be done for tests.
pub const TEST_SCYLLA_DB_MAX_CONCURRENT_QUERIES: usize = 1;

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

    /// Table name contains forbidden characters
    #[error("Table name contains forbidden characters")]
    InvalidTableName,

    /// Missing database
    #[error("Missing database")]
    MissingDatabase(String),

    /// Already existing database
    #[error("Already existing database")]
    AlreadyExistingDatabase,

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
        let client = self.client.deref();
        let _guard = self.acquire().await;
        let handles = keys
            .into_iter()
            .map(|key| Self::read_key_internal(client, key));
        let result = join_all(handles).await;
        Ok(result.into_iter().collect::<Result<_, _>>()?)
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

#[async_trait]
impl DeletePrefixExpander for ScyllaDbClientPair {
    type Error = ScyllaDbContextError;
    async fn expand_delete_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        ScyllaDbClientInternal::find_keys_by_prefix_internal(self, key_prefix.to_vec()).await
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
        let values = (key,);
        let query = format!(
            "SELECT v FROM kv.{} WHERE dummy = 0 AND k = ? ALLOW FILTERING",
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

    async fn write_batch_internal(
        client: &ScyllaDbClientPair,
        batch: Batch,
    ) -> Result<(), ScyllaDbContextError> {
        // We cannot do direct writing of the batch as deletion of key followed by insertion
        // yield incoherent results.
        // Thus the batch is first simplified and then the DeletePrefix that collides are removed.
        let unordered_batch = batch.simplify();
        let unordered_batch = unordered_batch
            .expand_colliding_prefix_deletions(client)
            .await?;
        let session = &client.0;
        let table_name = &client.1;
        let mut batch_query = scylla::statement::batch::Batch::new(BatchType::Logged);
        let mut batch_values = Vec::new();
        let query1 = format!("DELETE FROM kv.{} WHERE dummy = 0 AND k >= ?", table_name);
        let query2 = format!(
            "DELETE FROM kv.{} WHERE dummy = 0 AND k >= ? AND k < ?",
            table_name
        );
        for key_prefix in unordered_batch.key_prefix_deletions {
            match get_upper_bound_option(&key_prefix) {
                None => {
                    let values = vec![key_prefix];
                    batch_values.push(values);
                    batch_query.append_statement(Query::new(query1.clone()));
                }
                Some(upper_bound) => {
                    let values = vec![key_prefix, upper_bound];
                    batch_values.push(values);
                    batch_query.append_statement(Query::new(query2.clone()));
                }
            }
        }
        let query3 = format!("DELETE FROM kv.{} WHERE dummy = 0 AND k = ?", table_name);
        for key in unordered_batch.simple_unordered_batch.deletions {
            let values = vec![key];
            batch_values.push(values);
            batch_query.append_statement(Query::new(query3.clone()));
        }
        let query4 = format!(
            "INSERT INTO kv.{} (dummy, k, v) VALUES (0, ?, ?)",
            table_name
        );
        for (key, value) in unordered_batch.simple_unordered_batch.insertions {
            let values = vec![key, value];
            batch_values.push(values);
            batch_query.append_statement(Query::new(query4.clone()));
        }
        session.batch(&batch_query, batch_values).await?;
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
                let values = (key_prefix,);
                let query = format!(
                    "SELECT k FROM kv.{} WHERE dummy = 0 AND k >= ? ALLOW FILTERING",
                    table_name
                );
                session.query(query, values).await?
            }
            Some(upper_bound) => {
                let values = (key_prefix, upper_bound);
                let query = format!(
                    "SELECT k FROM kv.{} WHERE dummy = 0 AND k >= ? AND k < ? ALLOW FILTERING",
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
                let values = (key_prefix,);
                let query = format!(
                    "SELECT k FROM kv.{} WHERE dummy = 0 AND k >= ? ALLOW FILTERING",
                    table_name
                );
                session.query(query, values).await?
            }
            Some(upper_bound) => {
                let values = (key_prefix, upper_bound);
                let query = format!(
                    "SELECT k,v FROM kv.{} WHERE dummy = 0 AND k >= ? AND k < ? ALLOW FILTERING",
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
    pub async fn test_existence(
        store_config: ScyllaDbKvStoreConfig,
    ) -> Result<bool, ScyllaDbContextError> {
        let session = SessionBuilder::new()
            .known_node(store_config.uri.as_str())
            .build()
            .await?;
        Self::test_table_existence(&session, &store_config.table_name).await
    }

    /// Tests if a table is present in the database or not
    pub async fn test_table_existence(
        session: &Session,
        table_name: &String,
    ) -> Result<bool, ScyllaDbContextError> {
        // We check the way the test can fail. It can fail in different ways.
        let query = format!("SELECT dummy FROM kv.{} ALLOW FILTERING", table_name);

        // Execute the query
        let result = session.query(query, &[]).await;

        // The missing table translates into a very specific error that we matched
        let mesg_miss1 = format!("unconfigured table {}", table_name);
        let mesg_miss1 = mesg_miss1.as_str();
        let mesg_miss2 = "Undefined name dummy in selection clause";
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

    fn is_allowed_name(table_name: &str) -> bool {
        !table_name.is_empty()
            && table_name.len() <= 48
            && table_name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
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
        if !Self::is_allowed_name(table_name) {
            return Err(ScyllaDbContextError::InvalidTableName);
        }
        // Create a keyspace if it doesn't exist
        let query = "CREATE KEYSPACE IF NOT EXISTS kv WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";
        session.query(query, &[]).await?;

        // Create a table if it doesn't exist
        // The schema appears too complicated for non-trivial reasons.
        // See TODO(#1069).
        let query = format!(
            "CREATE TABLE kv.{} (dummy int, k blob, v blob, primary key (dummy, k))",
            table_name
        );
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
        store_config: ScyllaDbKvStoreConfig,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let session = SessionBuilder::new()
            .known_node(store_config.uri.as_str())
            .build()
            .await?;
        if Self::test_table_existence(&session, &store_config.table_name).await? {
            let query = format!("DROP TABLE kv.{};", store_config.table_name);
            session.query(query, &[]).await?;
        }
        let stop_if_table_exists = true;
        let create_if_missing = true;
        Self::new_internal(
            session,
            store_config,
            stop_if_table_exists,
            create_if_missing,
        )
        .await
    }

    async fn initialize(store_config: ScyllaDbKvStoreConfig) -> Result<Self, ScyllaDbContextError> {
        let session = SessionBuilder::new()
            .known_node(store_config.uri.as_str())
            .build()
            .await?;
        let stop_if_table_exists = false;
        let create_if_missing = true;
        let (client, table_status) = Self::new_internal(
            session,
            store_config,
            stop_if_table_exists,
            create_if_missing,
        )
        .await?;
        if table_status == TableStatus::Existing {
            return Err(ScyllaDbContextError::AlreadyExistingDatabase);
        }
        Ok(client)
    }

    async fn list_tables(
        store_config: ScyllaDbKvStoreConfig,
    ) -> Result<Vec<String>, ScyllaDbContextError> {
        let session = SessionBuilder::new()
            .known_node(store_config.uri.as_str())
            .build()
            .await?;
        let rows = session.query("DESCRIBE KEYSPACE kv", &[]).await?;
        let mut tables = Vec::new();
        if let Some(rows) = rows.rows {
            for row in rows.into_typed::<(String, String, String, String)>() {
                let value = row.unwrap();
                if value.1 == "table" {
                    tables.push(value.2);
                }
            }
        }
        Ok(tables)
    }

    async fn delete_all(store_config: ScyllaDbKvStoreConfig) -> Result<(), ScyllaDbContextError> {
        let session = SessionBuilder::new()
            .known_node(store_config.uri.as_str())
            .build()
            .await?;
        let query = "DROP KEYSPACE IF EXISTS kv;".to_string();
        session.query(query, &[]).await?;
        Ok(())
    }

    async fn delete_single(
        store_config: ScyllaDbKvStoreConfig,
    ) -> Result<(), ScyllaDbContextError> {
        let session = SessionBuilder::new()
            .known_node(store_config.uri.as_str())
            .build()
            .await?;
        let query = format!("DROP TABLE IF EXISTS kv.{};", store_config.table_name);
        session.query(query, &[]).await?;
        Ok(())
    }

    async fn new(
        store_config: ScyllaDbKvStoreConfig,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let session = SessionBuilder::new()
            .known_node(store_config.uri.as_str())
            .build()
            .await?;
        let stop_if_table_exists = false;
        let create_if_missing = false;
        Self::new_internal(
            session,
            store_config,
            stop_if_table_exists,
            create_if_missing,
        )
        .await
    }

    async fn new_internal(
        session: Session,
        store_config: ScyllaDbKvStoreConfig,
        stop_if_table_exists: bool,
        create_if_missing: bool,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        // Create a session builder and specify the ScyllaDB contact points
        let kv_name = format!("{:?}", store_config);
        let mut existing_table =
            Self::test_table_existence(&session, &store_config.table_name).await?;
        if !existing_table {
            if create_if_missing {
                existing_table =
                    Self::create_table(&session, &store_config.table_name, stop_if_table_exists)
                        .await?;
            } else {
                tracing::info!("ScyllaDb: Missing database for kv_name={}", kv_name);
                return Err(ScyllaDbContextError::MissingDatabase(kv_name));
            }
        }
        let table_status = if existing_table {
            TableStatus::Existing
        } else {
            TableStatus::New
        };
        let client = (session, store_config.table_name);
        let client = Arc::new(client);
        let semaphore = store_config
            .common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = store_config.common_config.max_stream_queries;
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

/// The type for building a new ScyllaDb Key Value Store Client
#[derive(Debug)]
pub struct ScyllaDbKvStoreConfig {
    /// The url to which the requests have to be sent
    pub uri: String,
    /// The name of the table that we create
    pub table_name: String,
    /// The common configuration of the key value store
    pub common_config: CommonStoreConfig,
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
        store_config: ScyllaDbKvStoreConfig,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let cache_size = store_config.common_config.cache_size;
        let (client, table_status) = ScyllaDbClientInternal::new_for_testing(store_config).await?;
        let client = LruCachingKeyValueClient::new(client, cache_size);
        let client = ScyllaDbClient { client };
        Ok((client, table_status))
    }

    /// Creates a [`ScyllaDbClient`] from the input parameters.
    pub async fn initialize(
        store_config: ScyllaDbKvStoreConfig,
    ) -> Result<Self, ScyllaDbContextError> {
        let cache_size = store_config.common_config.cache_size;
        let client = ScyllaDbClientInternal::initialize(store_config).await?;
        let client = LruCachingKeyValueClient::new(client, cache_size);
        let client = ScyllaDbClient { client };
        Ok(client)
    }

    /// Delete all the tables of a database
    pub async fn test_existence(
        store_config: ScyllaDbKvStoreConfig,
    ) -> Result<bool, ScyllaDbContextError> {
        ScyllaDbClientInternal::test_existence(store_config).await
    }

    /// Delete all the tables of a database
    pub async fn delete_all(
        store_config: ScyllaDbKvStoreConfig,
    ) -> Result<(), ScyllaDbContextError> {
        ScyllaDbClientInternal::delete_all(store_config).await
    }

    /// Delete all the tables of a database
    pub async fn list_tables(
        store_config: ScyllaDbKvStoreConfig,
    ) -> Result<Vec<String>, ScyllaDbContextError> {
        ScyllaDbClientInternal::list_tables(store_config).await
    }

    /// Deletes a single table from the database
    pub async fn delete_single(
        store_config: ScyllaDbKvStoreConfig,
    ) -> Result<(), ScyllaDbContextError> {
        ScyllaDbClientInternal::delete_single(store_config).await
    }

    /// Creates a [`ScyllaDbClient`] from the input parameters.
    pub async fn new(
        store_config: ScyllaDbKvStoreConfig,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let cache_size = store_config.common_config.cache_size;
        let (client, table_status) = ScyllaDbClientInternal::new(store_config).await?;
        let client = LruCachingKeyValueClient::new(client, cache_size);
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
    }
}

/// Creates a ScyllaDB test client
#[cfg(any(test, feature = "test"))]
pub async fn create_scylla_db_test_client() -> ScyllaDbClient {
    let uri = "localhost:9042".to_string();
    let table_name = get_table_name();
    let common_config = create_scylla_db_common_config();
    let store_config = ScyllaDbKvStoreConfig {
        uri,
        table_name,
        common_config,
    };
    let (client, _) = ScyllaDbClient::new_for_testing(store_config)
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
