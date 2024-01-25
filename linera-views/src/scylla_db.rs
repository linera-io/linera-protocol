// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This provides a KeyValueStore for the ScyllaDB database.
//! The code is functional but some aspects are missing.
//!
//! The current connection is done via a Session and a corresponding
//! primary key that we name `table_name`. The maximum number of
//! concurrent queries is controlled by max_concurrent_queries.
//!
//! We thus implement the
//! * [`KeyValueStore`][trait1] for a database access.
//! * [`Context`][trait2] for the context.
//!
//! [trait1]: common::KeyValueStore
//! [trait2]: common::Context

#[cfg(feature = "metrics")]
use crate::metering::{MeteredStore, LRU_CACHING_METRICS, SCYLLA_DB_METRICS};

#[cfg(any(test, feature = "test"))]
use crate::{lru_caching::TEST_CACHE_SIZE, test_utils::get_table_name};

use crate::{
    batch::{Batch, DeletePrefixExpander, UnorderedBatch},
    common::{
        get_upper_bound_option, CommonStoreConfig, ContextFromStore, KeyValueStore,
        ReadableKeyValueStore, TableStatus, WritableKeyValueStore,
    },
    journaling::{
        DirectKeyValueStore, DirectWritableKeyValueStore, JournalConsistencyError,
        JournalingKeyValueStore,
    },
    lru_caching::LruCachingStore,
    value_splitting::DatabaseConsistencyError,
};
use async_lock::{Semaphore, SemaphoreGuard};
use async_trait::async_trait;
use futures::future::join_all;
use linera_base::ensure;
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
type ScyllaDbStorePair = (Session, String);

/// We limit the number of connections that can be done for tests.
#[cfg(any(test, feature = "test"))]
const TEST_SCYLLA_DB_MAX_CONCURRENT_QUERIES: usize = 10;

/// The number of connections in the stream is limited for tests.
#[cfg(any(test, feature = "test"))]
const TEST_SCYLLA_DB_MAX_STREAM_QUERIES: usize = 10;

/// The maximal size of an operation on ScyllaDB seems to be 16M
/// https://www.scylladb.com/2019/03/27/best-practices-for-scylla-applications/
/// "There is a hard limit at 16MB, and nothing bigger than that can arrive at once
///  at the database at any particular time"
/// So, we set up the maximal size of 15M for the values and 1M for the keys
const MAX_VALUE_SIZE: usize = 15728640;
const MAX_KEY_SIZE: usize = 1048576;

/// The client itself and the keeping of the count of active connections.
#[derive(Clone)]
pub struct ScyllaDbStoreInternal {
    store: Arc<ScyllaDbStorePair>,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
}

/// The error type for [`ScyllaDbStoreInternal`]
#[derive(Error, Debug)]
pub enum ScyllaDbContextError {
    /// BCS serialization error.
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),

    /// The key must have at most 1M bytes
    #[error("The key must have at most 1M")]
    KeyTooLong,

    /// A row error in ScyllaDB
    #[error(transparent)]
    ScyllaDbRowError(#[from] scylla::cql_to_rust::FromRowError),

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

    /// The journal is not coherent
    #[error(transparent)]
    JournalConsistencyError(#[from] JournalConsistencyError),
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
impl ReadableKeyValueStore<ScyllaDbContextError> for ScyllaDbStoreInternal {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ScyllaDbContextError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        Self::read_value_internal(store, key.to_vec()).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ScyllaDbContextError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        Self::contains_key_internal(store, key.to_vec()).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ScyllaDbContextError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        let handles = keys
            .into_iter()
            .map(|key| Self::read_value_internal(store, key));
        let result = join_all(handles).await;
        Ok(result.into_iter().collect::<Result<_, _>>()?)
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, ScyllaDbContextError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        Self::find_keys_by_prefix_internal(store, key_prefix.to_vec()).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ScyllaDbContextError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        Self::find_key_values_by_prefix_internal(store, key_prefix.to_vec()).await
    }
}

#[async_trait]
impl DirectWritableKeyValueStore<ScyllaDbContextError> for ScyllaDbStoreInternal {
    // The constant 14000 is an empirical constant that was found to be necessary
    // to make the ScyllaDb system work. We have not been able to find this or
    // a similar constant in the source code or the documentation.
    const MAX_BATCH_SIZE: usize = 14000;
    /// The total size is 16M
    const MAX_BATCH_TOTAL_SIZE: usize = 16000000;
    const MAX_VALUE_SIZE: usize = MAX_VALUE_SIZE;

    // ScyllaDB cannot take a `crate::batch::Batch` directly. Indeed, if a delete is
    // followed by a write, then the delete takes priority. See the sentence "The first
    // tie-breaking rule when two cells have the same write timestamp is that dead cells
    // win over live cells" from
    // https://github.com/scylladb/scylladb/blob/master/docs/dev/timestamp-conflict-resolution.md
    type Batch = UnorderedBatch;

    async fn write_batch(&self, batch: Self::Batch) -> Result<(), ScyllaDbContextError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        Self::write_batch_internal(store, batch).await
    }
}

impl DirectKeyValueStore for ScyllaDbStoreInternal {
    type Error = ScyllaDbContextError;
}

#[async_trait]
impl DeletePrefixExpander for ScyllaDbStorePair {
    type Error = ScyllaDbContextError;

    async fn expand_delete_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        ScyllaDbStoreInternal::find_keys_by_prefix_internal(self, key_prefix.to_vec()).await
    }
}

impl ScyllaDbStoreInternal {
    /// Obtains the semaphore lock on the database if needed.
    async fn acquire(&self) -> Option<SemaphoreGuard<'_>> {
        match &self.semaphore {
            None => None,
            Some(count) => Some(count.acquire().await),
        }
    }

    async fn read_value_internal(
        store: &ScyllaDbStorePair,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ScyllaDbContextError> {
        ensure!(key.len() <= MAX_KEY_SIZE, ScyllaDbContextError::KeyTooLong);
        let session = &store.0;
        let table_name = &store.1;
        // Read the value of a key
        let values = (key,);
        let query = format!(
            "SELECT v FROM kv.{} WHERE dummy = 0 AND k = ? ALLOW FILTERING",
            table_name
        );
        let result = session.query(query, values).await?;
        if let Some(rows) = result.rows {
            if let Some(row) = rows.into_typed::<(Vec<u8>,)>().next() {
                let value = row?;
                return Ok(Some(value.0));
            }
        }
        Ok(None)
    }

    async fn contains_key_internal(
        store: &ScyllaDbStorePair,
        key: Vec<u8>,
    ) -> Result<bool, ScyllaDbContextError> {
        ensure!(key.len() <= MAX_KEY_SIZE, ScyllaDbContextError::KeyTooLong);
        let session = &store.0;
        let table_name = &store.1;
        // Read the value of a key
        let values = (key,);
        let query = format!(
            "SELECT dummy FROM kv.{} WHERE dummy = 0 AND k = ? ALLOW FILTERING",
            table_name
        );
        let result = session.query(query, values).await?;
        if let Some(rows) = result.rows {
            if let Some(_row) = rows.into_typed::<(Vec<u8>,)>().next() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn write_batch_internal(
        store: &ScyllaDbStorePair,
        batch: UnorderedBatch,
    ) -> Result<(), ScyllaDbContextError> {
        let session = &store.0;
        let table_name = &store.1;
        let mut batch_query = scylla::statement::batch::Batch::new(BatchType::Logged);
        let mut batch_values = Vec::new();
        let query1 = format!("DELETE FROM kv.{} WHERE dummy = 0 AND k >= ?", table_name);
        let query2 = format!(
            "DELETE FROM kv.{} WHERE dummy = 0 AND k >= ? AND k < ?",
            table_name
        );
        for key_prefix in batch.key_prefix_deletions {
            ensure!(
                key_prefix.len() <= MAX_KEY_SIZE,
                ScyllaDbContextError::KeyTooLong
            );
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
        for key in batch.simple_unordered_batch.deletions {
            let values = vec![key];
            batch_values.push(values);
            batch_query.append_statement(Query::new(query3.clone()));
        }
        let query4 = format!(
            "INSERT INTO kv.{} (dummy, k, v) VALUES (0, ?, ?)",
            table_name
        );
        for (key, value) in batch.simple_unordered_batch.insertions {
            ensure!(key.len() <= MAX_KEY_SIZE, ScyllaDbContextError::KeyTooLong);
            let values = vec![key, value];
            batch_values.push(values);
            batch_query.append_statement(Query::new(query4.clone()));
        }
        session.batch(&batch_query, batch_values).await?;
        Ok(())
    }

    async fn find_keys_by_prefix_internal(
        store: &ScyllaDbStorePair,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, ScyllaDbContextError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            ScyllaDbContextError::KeyTooLong
        );
        let session = &store.0;
        let table_name = &store.1;
        // Read the value of a key
        let len = key_prefix.len();
        let mut keys = Vec::new();
        let mut paging_state = None;
        loop {
            let result = match get_upper_bound_option(&key_prefix) {
                None => {
                    let values = (key_prefix.clone(),);
                    let query = format!(
                        "SELECT k FROM kv.{} WHERE dummy = 0 AND k >= ? ALLOW FILTERING",
                        table_name
                    );
                    session.query_paged(query, values, paging_state).await?
                }
                Some(upper_bound) => {
                    let values = (key_prefix.clone(), upper_bound);
                    let query = format!(
                        "SELECT k FROM kv.{} WHERE dummy = 0 AND k >= ? AND k < ? ALLOW FILTERING",
                        table_name
                    );
                    session.query_paged(query, values, paging_state).await?
                }
            };
            if let Some(rows) = result.rows {
                for row in rows.into_typed::<(Vec<u8>,)>() {
                    let key = row?;
                    let short_key = key.0[len..].to_vec();
                    keys.push(short_key);
                }
            }
            if result.paging_state.is_none() {
                return Ok(keys);
            }
            paging_state = result.paging_state;
        }
    }

    async fn find_key_values_by_prefix_internal(
        store: &ScyllaDbStorePair,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ScyllaDbContextError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            ScyllaDbContextError::KeyTooLong
        );
        let session = &store.0;
        let table_name = &store.1;
        // Read the value of a key
        let len = key_prefix.len();
        let mut key_values = Vec::new();
        let mut paging_state = None;
        loop {
            let result = match get_upper_bound_option(&key_prefix) {
                None => {
                    let values = (key_prefix.clone(),);
                    let query = format!(
                        "SELECT k,v FROM kv.{} WHERE dummy = 0 AND k >= ? ALLOW FILTERING",
                        table_name
                    );
                    session.query_paged(query, values, paging_state).await?
                }
                Some(upper_bound) => {
                    let values = (key_prefix.clone(), upper_bound);
                    let query = format!(
                        "SELECT k,v FROM kv.{} WHERE dummy = 0 AND k >= ? AND k < ? ALLOW FILTERING",
                        table_name
                    );
                    session.query_paged(query, values, paging_state).await?
                }
            };
            if let Some(rows) = result.rows {
                for row in rows.into_typed::<(Vec<u8>, Vec<u8>)>() {
                    let key = row?;
                    let short_key = key.0[len..].to_vec();
                    key_values.push((short_key, key.1));
                }
            }
            if result.paging_state.is_none() {
                return Ok(key_values);
            }
            paging_state = result.paging_state;
        }
    }

    /// Retrieves the table_name from the store
    pub async fn get_table_name(&self) -> String {
        let store = self.store.deref();
        store.1.clone()
    }

    /// Tests if a table is present in the database or not
    pub async fn test_existence(
        store_config: ScyllaDbStoreConfig,
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
        store_config: ScyllaDbStoreConfig,
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

    async fn initialize(store_config: ScyllaDbStoreConfig) -> Result<Self, ScyllaDbContextError> {
        let session = SessionBuilder::new()
            .known_node(store_config.uri.as_str())
            .build()
            .await?;
        let stop_if_table_exists = false;
        let create_if_missing = true;
        let (store, table_status) = Self::new_internal(
            session,
            store_config,
            stop_if_table_exists,
            create_if_missing,
        )
        .await?;
        if table_status == TableStatus::Existing {
            return Err(ScyllaDbContextError::AlreadyExistingDatabase);
        }
        Ok(store)
    }

    async fn list_tables(
        store_config: ScyllaDbStoreConfig,
    ) -> Result<Vec<String>, ScyllaDbContextError> {
        let session = SessionBuilder::new()
            .known_node(store_config.uri.as_str())
            .build()
            .await?;
        let result = session.query("DESCRIBE KEYSPACE kv", &[]).await?;
        let mut tables = Vec::new();
        if let Some(rows) = result.rows {
            for row in rows.into_typed::<(String, String, String, String)>() {
                let value = row?;
                if value.1 == "table" {
                    tables.push(value.2);
                }
            }
        }
        Ok(tables)
    }

    async fn delete_all(store_config: ScyllaDbStoreConfig) -> Result<(), ScyllaDbContextError> {
        let session = SessionBuilder::new()
            .known_node(store_config.uri.as_str())
            .build()
            .await?;
        let query = "DROP KEYSPACE IF EXISTS kv;".to_string();
        session.query(query, &[]).await?;
        Ok(())
    }

    async fn delete_single(store_config: ScyllaDbStoreConfig) -> Result<(), ScyllaDbContextError> {
        let session = SessionBuilder::new()
            .known_node(store_config.uri.as_str())
            .build()
            .await?;
        let query = format!("DROP TABLE IF EXISTS kv.{};", store_config.table_name);
        session.query(query, &[]).await?;
        Ok(())
    }

    async fn new(
        store_config: ScyllaDbStoreConfig,
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
        store_config: ScyllaDbStoreConfig,
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
        let store = (session, store_config.table_name);
        let store = Arc::new(store);
        let semaphore = store_config
            .common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = store_config.common_config.max_stream_queries;
        let store = Self {
            store,
            semaphore,
            max_stream_queries,
        };
        Ok((store, table_status))
    }
}

/// A shared DB store for ScyllaDB implementing LruCaching
#[derive(Clone)]
pub struct ScyllaDbStore {
    #[cfg(feature = "metrics")]
    store:
        MeteredStore<LruCachingStore<MeteredStore<JournalingKeyValueStore<ScyllaDbStoreInternal>>>>,
    #[cfg(not(feature = "metrics"))]
    store: LruCachingStore<JournalingKeyValueStore<ScyllaDbStoreInternal>>,
}

/// The type for building a new ScyllaDB Key Value Store
#[derive(Debug)]
pub struct ScyllaDbStoreConfig {
    /// The url to which the requests have to be sent
    pub uri: String,
    /// The name of the table that we create
    pub table_name: String,
    /// The common configuration of the key value store
    pub common_config: CommonStoreConfig,
}

#[async_trait]
impl ReadableKeyValueStore<ScyllaDbContextError> for ScyllaDbStore {
    const MAX_KEY_SIZE: usize = ScyllaDbStoreInternal::MAX_KEY_SIZE;

    type Keys = <ScyllaDbStoreInternal as ReadableKeyValueStore<ScyllaDbContextError>>::Keys;

    type KeyValues =
        <ScyllaDbStoreInternal as ReadableKeyValueStore<ScyllaDbContextError>>::KeyValues;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ScyllaDbContextError> {
        self.store.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ScyllaDbContextError> {
        self.store.contains_key(key).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ScyllaDbContextError> {
        self.store.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, ScyllaDbContextError> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ScyllaDbContextError> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

#[async_trait]
impl WritableKeyValueStore<ScyllaDbContextError> for ScyllaDbStore {
    const MAX_VALUE_SIZE: usize = ScyllaDbStoreInternal::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), ScyllaDbContextError> {
        self.store.write_batch(batch, base_key).await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), ScyllaDbContextError> {
        self.store.clear_journal(base_key).await
    }
}

impl KeyValueStore for ScyllaDbStore {
    type Error = ScyllaDbContextError;
}

impl ScyllaDbStore {
    /// Gets the table name of the ScyllaDB store.
    #[cfg(not(feature = "metrics"))]
    pub async fn get_table_name(&self) -> String {
        self.store.store.store.get_table_name().await
    }

    /// Gets the table name of the ScyllaDB store.
    #[cfg(feature = "metrics")]
    pub async fn get_table_name(&self) -> String {
        self.store.store.store.store.store.get_table_name().await
    }

    #[cfg(not(feature = "metrics"))]
    fn get_complete_store(
        store: JournalingKeyValueStore<ScyllaDbStoreInternal>,
        cache_size: usize,
    ) -> Self {
        let store = LruCachingStore::new(store, cache_size);
        Self { store }
    }

    #[cfg(feature = "metrics")]
    fn get_complete_store(
        store: JournalingKeyValueStore<ScyllaDbStoreInternal>,
        cache_size: usize,
    ) -> Self {
        let store = MeteredStore::new(&SCYLLA_DB_METRICS, store);
        let store = LruCachingStore::new(store, cache_size);
        let store = MeteredStore::new(&LRU_CACHING_METRICS, store);
        Self { store }
    }

    /// Creates a [`ScyllaDbStore`] from the input parameters.
    #[cfg(any(test, feature = "test"))]
    pub async fn new_for_testing(
        store_config: ScyllaDbStoreConfig,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let cache_size = store_config.common_config.cache_size;
        let (simple_store, table_status) =
            ScyllaDbStoreInternal::new_for_testing(store_config).await?;
        let store = JournalingKeyValueStore::new(simple_store);
        let store = Self::get_complete_store(store, cache_size);
        Ok((store, table_status))
    }

    /// Creates a [`ScyllaDbStore`] from the input parameters.
    pub async fn initialize(
        store_config: ScyllaDbStoreConfig,
    ) -> Result<Self, ScyllaDbContextError> {
        let cache_size = store_config.common_config.cache_size;
        let simple_store = ScyllaDbStoreInternal::initialize(store_config).await?;
        let store = JournalingKeyValueStore::new(simple_store);
        let store = Self::get_complete_store(store, cache_size);
        Ok(store)
    }

    /// Delete all the tables of a database
    pub async fn test_existence(
        store_config: ScyllaDbStoreConfig,
    ) -> Result<bool, ScyllaDbContextError> {
        ScyllaDbStoreInternal::test_existence(store_config).await
    }

    /// Delete all the tables of a database
    pub async fn delete_all(store_config: ScyllaDbStoreConfig) -> Result<(), ScyllaDbContextError> {
        ScyllaDbStoreInternal::delete_all(store_config).await
    }

    /// Delete all the tables of a database
    pub async fn list_tables(
        store_config: ScyllaDbStoreConfig,
    ) -> Result<Vec<String>, ScyllaDbContextError> {
        ScyllaDbStoreInternal::list_tables(store_config).await
    }

    /// Deletes a single table from the database
    pub async fn delete_single(
        store_config: ScyllaDbStoreConfig,
    ) -> Result<(), ScyllaDbContextError> {
        ScyllaDbStoreInternal::delete_single(store_config).await
    }

    /// Creates a [`ScyllaDbStore`] from the input parameters.
    pub async fn new(
        store_config: ScyllaDbStoreConfig,
    ) -> Result<(Self, TableStatus), ScyllaDbContextError> {
        let cache_size = store_config.common_config.cache_size;
        let (simple_store, table_status) = ScyllaDbStoreInternal::new(store_config).await?;
        let store = JournalingKeyValueStore::new(simple_store);
        let store = Self::get_complete_store(store, cache_size);
        Ok((store, table_status))
    }
}

/// Creates the common initialization for RocksDB.
#[cfg(any(test, feature = "test"))]
pub fn create_scylla_db_common_config() -> CommonStoreConfig {
    CommonStoreConfig {
        max_concurrent_queries: Some(TEST_SCYLLA_DB_MAX_CONCURRENT_QUERIES),
        max_stream_queries: TEST_SCYLLA_DB_MAX_STREAM_QUERIES,
        cache_size: TEST_CACHE_SIZE,
    }
}

/// Creates a ScyllaDB test store.
#[cfg(any(test, feature = "test"))]
pub async fn create_scylla_db_test_store() -> ScyllaDbStore {
    let uri = "localhost:9042".to_string();
    let table_name = get_table_name();
    let common_config = create_scylla_db_common_config();
    let store_config = ScyllaDbStoreConfig {
        uri,
        table_name,
        common_config,
    };
    let (store, _) = ScyllaDbStore::new_for_testing(store_config)
        .await
        .expect("store");
    store
}

/// An implementation of [`crate::common::Context`] based on ScyllaDB
pub type ScyllaDbContext<E> = ContextFromStore<E, ScyllaDbStore>;

impl<E: Clone + Send + Sync> ScyllaDbContext<E> {
    /// Creates a [`ScyllaDbContext`].
    pub fn new(store: ScyllaDbStore, base_key: Vec<u8>, extra: E) -> Self {
        Self {
            store,
            base_key,
            extra,
        }
    }
}

/// Creates a [`crate::common::Context`] for testing purposes.
#[cfg(any(test, feature = "test"))]
pub async fn create_scylla_db_test_context() -> ScyllaDbContext<()> {
    let base_key = vec![];
    let db = create_scylla_db_test_store().await;
    ScyllaDbContext::new(db, base_key, ())
}
