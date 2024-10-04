// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::store::KeyValueStore`] for the ScyllaDB database.
//!
//! The current connection is done via a Session and a corresponding primary key called
//! "namespace". The maximum number of concurrent queries is controlled by
//! `max_concurrent_queries`.

/// Fundamental constant in ScyllaDB: The maximum size of a multi keys query
/// The limit is in reality 100. But we need one entry for the root key.
const MAX_MULTI_KEYS: usize = 99;

use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Deref,
    sync::Arc,
};

use async_lock::{Semaphore, SemaphoreGuard};
use async_trait::async_trait;
use futures::{future::join_all, FutureExt as _, StreamExt};
use linera_base::ensure;
use scylla::{
    frame::request::batch::BatchType,
    query::Query,
    transport::errors::{DbError, QueryError},
    IntoTypedRows, Session, SessionBuilder,
};
use thiserror::Error;

#[cfg(with_metrics)]
use crate::metering::{MeteredStore, LRU_CACHING_METRICS, SCYLLA_DB_METRICS};
#[cfg(with_testing)]
use crate::test_utils::generate_test_namespace;
use crate::{
    batch::{Batch, UnorderedBatch},
    common::get_upper_bound_option,
    journaling::{DirectWritableKeyValueStore, JournalConsistencyError, JournalingKeyValueStore},
    lru_caching::{LruCachingStore, TEST_CACHE_SIZE},
    store::{
        AdminKeyValueStore, CommonStoreConfig, KeyValueStoreError, ReadableKeyValueStore,
        WithError, WritableKeyValueStore,
    },
};

/// The client for ScyllaDb.
/// * The session allows to pass queries
/// * The namespace that is being assigned to the database
/// * The prepared queries used for implementing the features of `KeyValueStore`.
struct ScyllaDbClient {
    session: Session,
    namespace: String,
    read_value: Query,
    contains_key: Query,
    write_batch_delete_prefix_unbounded: Query,
    write_batch_delete_prefix_bounded: Query,
    write_batch_deletion: Query,
    write_batch_insertion: Query,
    find_keys_by_prefix_unbounded: Query,
    find_keys_by_prefix_bounded: Query,
    find_key_values_by_prefix_unbounded: Query,
    find_key_values_by_prefix_bounded: Query,
}

impl ScyllaDbClient {
    fn new(session: Session, namespace: &str) -> Self {
        let namespace = namespace.to_string();
        let query = format!(
            "SELECT v FROM kv.{} WHERE root_key = ? AND k = ? ALLOW FILTERING",
            namespace
        );
        let read_value = Query::new(query);
        let query = format!(
            "SELECT root_key FROM kv.{} WHERE root_key = ? AND k = ? ALLOW FILTERING",
            namespace
        );
        let contains_key = Query::new(query);

        let query = format!("DELETE FROM kv.{} WHERE root_key = ? AND k >= ?", namespace);
        let write_batch_delete_prefix_unbounded = Query::new(query);
        let query = format!(
            "DELETE FROM kv.{} WHERE root_key = ? AND k >= ? AND k < ?",
            namespace
        );
        let write_batch_delete_prefix_bounded = Query::new(query);
        let query = format!("DELETE FROM kv.{} WHERE root_key = ? AND k = ?", namespace);
        let write_batch_deletion = Query::new(query);
        let query = format!(
            "INSERT INTO kv.{} (root_key, k, v) VALUES (?, ?, ?)",
            namespace
        );
        let write_batch_insertion = Query::new(query);

        let query = format!(
            "SELECT k FROM kv.{} WHERE root_key = ? AND k >= ? ALLOW FILTERING",
            namespace
        );
        let find_keys_by_prefix_unbounded = Query::new(query);
        let query = format!(
            "SELECT k FROM kv.{} WHERE root_key = ? AND k >= ? AND k < ? ALLOW FILTERING",
            namespace
        );
        let find_keys_by_prefix_bounded = Query::new(query);

        let query = format!(
            "SELECT k,v FROM kv.{} WHERE root_key = ? AND k >= ? ALLOW FILTERING",
            namespace
        );
        let find_key_values_by_prefix_unbounded = Query::new(query);
        let query = format!(
            "SELECT k,v FROM kv.{} WHERE root_key = ? AND k >= ? AND k < ? ALLOW FILTERING",
            namespace
        );
        let find_key_values_by_prefix_bounded = Query::new(query);

        Self {
            session,
            namespace,
            read_value,
            contains_key,
            write_batch_delete_prefix_unbounded,
            write_batch_delete_prefix_bounded,
            write_batch_deletion,
            write_batch_insertion,
            find_keys_by_prefix_unbounded,
            find_keys_by_prefix_bounded,
            find_key_values_by_prefix_unbounded,
            find_key_values_by_prefix_bounded,
        }
    }

    async fn read_value_internal(
        &self,
        root_key: &[u8],
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ScyllaDbStoreError> {
        ensure!(key.len() <= MAX_KEY_SIZE, ScyllaDbStoreError::KeyTooLong);
        let session = &self.session;
        // Read the value of a key
        let values = (root_key.to_vec(), key);
        let query = &self.read_value;
        let mut rows = session.query_iter(query.clone(), &values).await?;
        Ok(match rows.next().await {
            Some(row) => Some(row?.into_typed::<(Vec<u8>,)>()?.0),
            None => None,
        })
    }

    async fn read_multi_values_internal(
        &self,
        root_key: &[u8],
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ScyllaDbStoreError> {
        let num_keys = keys.len();
        if num_keys == 0 {
            return Ok(Vec::new());
        }
        let session = &self.session;
        let mut map = HashMap::<Vec<u8>, Vec<usize>>::new();
        let mut inputs = Vec::new();
        inputs.push(root_key.to_vec());
        for (i_key, key) in keys.into_iter().enumerate() {
            ensure!(key.len() <= MAX_KEY_SIZE, ScyllaDbStoreError::KeyTooLong);
            match map.entry(key.clone()) {
                Entry::Occupied(entry) => {
                    let entry = entry.into_mut();
                    entry.push(i_key);
                }
                Entry::Vacant(entry) => {
                    entry.insert(vec![i_key]);
                    inputs.push(key);
                }
            }
        }
        let num_unique_keys = map.len();
        let mut group_query = "?".to_string();
        group_query.push_str(&",?".repeat(num_unique_keys - 1));
        let query = format!(
            "SELECT k,v FROM kv.{} WHERE root_key = ? AND k IN ({}) ALLOW FILTERING",
            self.namespace, group_query
        );
        let read_multi_value = Query::new(query);
        let mut rows = session.query_iter(read_multi_value, &inputs).await?;
        let mut values = vec![None; num_keys];
        while let Some(row) = rows.next().await {
            let (key, value) = row?.into_typed::<(Vec<u8>, Vec<u8>)>()?;
            for i_key in map.get(&key).unwrap().clone() {
                let value = Some(value.clone());
                *values.get_mut(i_key).expect("an entry in values") = value;
            }
        }
        Ok(values)
    }

    async fn contains_keys_internal(
        &self,
        root_key: &[u8],
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<bool>, ScyllaDbStoreError> {
        let num_keys = keys.len();
        if num_keys == 0 {
            return Ok(Vec::new());
        }
        let session = &self.session;
        let mut map = HashMap::<Vec<u8>, Vec<usize>>::new();
        let mut inputs = Vec::new();
        inputs.push(root_key.to_vec());
        for (i_key, key) in keys.into_iter().enumerate() {
            ensure!(key.len() <= MAX_KEY_SIZE, ScyllaDbStoreError::KeyTooLong);
            match map.entry(key.clone()) {
                Entry::Occupied(entry) => {
                    let entry = entry.into_mut();
                    entry.push(i_key);
                }
                Entry::Vacant(entry) => {
                    entry.insert(vec![i_key]);
                    inputs.push(key);
                }
            }
        }
        let num_unique_keys = map.len();
        let mut group_query = "?".to_string();
        group_query.push_str(&",?".repeat(num_unique_keys - 1));
        let query = format!(
            "SELECT k FROM kv.{} WHERE root_key = ? AND k IN ({}) ALLOW FILTERING",
            self.namespace, group_query
        );
        let contains_keys = Query::new(query);
        let mut rows = session.query_iter(contains_keys, &inputs).await?;
        let mut values = vec![false; num_keys];
        while let Some(row) = rows.next().await {
            let (key,) = row?.into_typed::<(Vec<u8>,)>()?;
            for i_key in map.get(&key).unwrap().clone() {
                *values.get_mut(i_key).expect("an entry in values") = true;
            }
        }
        Ok(values)
    }

    async fn contains_key_internal(
        &self,
        root_key: &[u8],
        key: Vec<u8>,
    ) -> Result<bool, ScyllaDbStoreError> {
        ensure!(key.len() <= MAX_KEY_SIZE, ScyllaDbStoreError::KeyTooLong);
        let session = &self.session;
        // Read the value of a key
        let values = (root_key.to_vec(), key);
        let query = &self.contains_key;
        let mut rows = session.query_iter(query.clone(), &values).await?;
        Ok(rows.next().await.is_some())
    }

    async fn write_batch_internal(
        &self,
        root_key: &[u8],
        batch: UnorderedBatch,
    ) -> Result<(), ScyllaDbStoreError> {
        let session = &self.session;
        let mut batch_query = scylla::statement::batch::Batch::new(BatchType::Logged);
        let mut batch_values = Vec::new();
        let query1 = &self.write_batch_delete_prefix_unbounded;
        let query2 = &self.write_batch_delete_prefix_bounded;
        for key_prefix in batch.key_prefix_deletions {
            ensure!(
                key_prefix.len() <= MAX_KEY_SIZE,
                ScyllaDbStoreError::KeyTooLong
            );
            match get_upper_bound_option(&key_prefix) {
                None => {
                    let values = vec![root_key.to_vec(), key_prefix];
                    batch_values.push(values);
                    batch_query.append_statement(query1.clone());
                }
                Some(upper_bound) => {
                    let values = vec![root_key.to_vec(), key_prefix, upper_bound];
                    batch_values.push(values);
                    batch_query.append_statement(query2.clone());
                }
            }
        }
        let query3 = &self.write_batch_deletion;
        for key in batch.simple_unordered_batch.deletions {
            let values = vec![root_key.to_vec(), key];
            batch_values.push(values);
            batch_query.append_statement(query3.clone());
        }
        let query4 = &self.write_batch_insertion;
        for (key, value) in batch.simple_unordered_batch.insertions {
            ensure!(key.len() <= MAX_KEY_SIZE, ScyllaDbStoreError::KeyTooLong);
            let values = vec![root_key.to_vec(), key, value];
            batch_values.push(values);
            batch_query.append_statement(query4.clone());
        }
        session.batch(&batch_query, batch_values).await?;
        Ok(())
    }

    async fn find_keys_by_prefix_internal(
        &self,
        root_key: &[u8],
        key_prefix: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, ScyllaDbStoreError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            ScyllaDbStoreError::KeyTooLong
        );
        let session = &self.session;
        // Read the value of a key
        let len = key_prefix.len();
        let query_unbounded = &self.find_keys_by_prefix_unbounded;
        let query_bounded = &self.find_keys_by_prefix_bounded;
        let mut rows = match get_upper_bound_option(&key_prefix) {
            None => {
                let values = (root_key.to_vec(), key_prefix.clone());
                session.query_iter(query_unbounded.clone(), values).await?
            }
            Some(upper_bound) => {
                let values = (root_key.to_vec(), key_prefix.clone(), upper_bound);
                session.query_iter(query_bounded.clone(), values).await?
            }
        };
        let mut keys = Vec::new();
        while let Some(row) = rows.next().await {
            let (key,) = row?.into_typed::<(Vec<u8>,)>()?;
            let short_key = key[len..].to_vec();
            keys.push(short_key);
        }
        Ok(keys)
    }

    async fn find_key_values_by_prefix_internal(
        &self,
        root_key: &[u8],
        key_prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ScyllaDbStoreError> {
        ensure!(
            key_prefix.len() <= MAX_KEY_SIZE,
            ScyllaDbStoreError::KeyTooLong
        );
        let session = &self.session;
        // Read the value of a key
        let len = key_prefix.len();
        let query_unbounded = &self.find_key_values_by_prefix_unbounded;
        let query_bounded = &self.find_key_values_by_prefix_bounded;
        let mut rows = match get_upper_bound_option(&key_prefix) {
            None => {
                let values = (root_key.to_vec(), key_prefix.clone());
                session.query_iter(query_unbounded.clone(), values).await?
            }
            Some(upper_bound) => {
                let values = (root_key.to_vec(), key_prefix.clone(), upper_bound);
                session.query_iter(query_bounded.clone(), values).await?
            }
        };
        let mut key_values = Vec::new();
        while let Some(row) = rows.next().await {
            let (key, value) = row?.into_typed::<(Vec<u8>, Vec<u8>)>()?;
            let short_key = key[len..].to_vec();
            key_values.push((short_key, value));
        }
        Ok(key_values)
    }
}

/// We limit the number of connections that can be done for tests.
const TEST_SCYLLA_DB_MAX_CONCURRENT_QUERIES: usize = 10;

/// The number of connections in the stream is limited for tests.
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
    store: Arc<ScyllaDbClient>,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
    cache_size: usize,
    root_key: Vec<u8>,
}

/// The error type for [`ScyllaDbStoreInternal`]
#[derive(Error, Debug)]
pub enum ScyllaDbStoreError {
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

    /// The journal is not coherent
    #[error(transparent)]
    JournalConsistencyError(#[from] JournalConsistencyError),
}

impl KeyValueStoreError for ScyllaDbStoreError {
    const BACKEND: &'static str = "scylla_db";
}

impl WithError for ScyllaDbStoreInternal {
    type Error = ScyllaDbStoreError;
}

impl ReadableKeyValueStore for ScyllaDbStoreInternal {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ScyllaDbStoreError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store
            .read_value_internal(&self.root_key, key.to_vec())
            .await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ScyllaDbStoreError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store
            .contains_key_internal(&self.root_key, key.to_vec())
            .await
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, ScyllaDbStoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let store = self.store.deref();
        let _guard = self.acquire().await;
        let handles = keys
            .chunks(MAX_MULTI_KEYS)
            .map(|keys| store.contains_keys_internal(&self.root_key, keys.to_vec()));
        let results: Vec<_> = join_all(handles)
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        Ok(results.into_iter().flatten().collect())
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ScyllaDbStoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let store = self.store.deref();
        let _guard = self.acquire().await;
        let handles = keys
            .chunks(MAX_MULTI_KEYS)
            .map(|keys| store.read_multi_values_internal(&self.root_key, keys.to_vec()));
        let results: Vec<_> = join_all(handles)
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        Ok(results.into_iter().flatten().collect())
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, ScyllaDbStoreError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store
            .find_keys_by_prefix_internal(&self.root_key, key_prefix.to_vec())
            .await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ScyllaDbStoreError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store
            .find_key_values_by_prefix_internal(&self.root_key, key_prefix.to_vec())
            .await
    }
}

#[async_trait]
impl DirectWritableKeyValueStore for ScyllaDbStoreInternal {
    // The constant 14000 is an empirical constant that was found to be necessary
    // to make the ScyllaDb system work. We have not been able to find this or
    // a similar constant in the source code or the documentation.
    // An experimental approach gets us that 14796 is the latest value that is
    // correct.
    const MAX_BATCH_SIZE: usize = 5000;
    /// The total size is 16M
    const MAX_BATCH_TOTAL_SIZE: usize = 16000000;
    const MAX_VALUE_SIZE: usize = MAX_VALUE_SIZE;

    // ScyllaDB cannot take a `crate::batch::Batch` directly. Indeed, if a delete is
    // followed by a write, then the delete takes priority. See the sentence "The first
    // tie-breaking rule when two cells have the same write timestamp is that dead cells
    // win over live cells" from
    // https://github.com/scylladb/scylladb/blob/master/docs/dev/timestamp-conflict-resolution.md
    type Batch = UnorderedBatch;

    async fn write_batch(&self, batch: Self::Batch) -> Result<(), ScyllaDbStoreError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store.write_batch_internal(&self.root_key, batch).await
    }
}

// ScyllaDb requires that the keys are non-empty.
fn get_big_root_key(root_key: &[u8]) -> Vec<u8> {
    let mut big_key = vec![0];
    big_key.extend(root_key);
    big_key
}

impl AdminKeyValueStore for ScyllaDbStoreInternal {
    type Config = ScyllaDbStoreConfig;

    async fn new_test_config() -> Result<ScyllaDbStoreConfig, ScyllaDbStoreError> {
        let uri = create_scylla_db_test_uri();
        let common_config = create_scylla_db_common_config();
        Ok(ScyllaDbStoreConfig { uri, common_config })
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, ScyllaDbStoreError> {
        Self::check_namespace(namespace)?;
        let session = SessionBuilder::new()
            .known_node(config.uri.as_str())
            .build()
            .boxed()
            .await?;
        let store = ScyllaDbClient::new(session, namespace);
        let store = Arc::new(store);
        let semaphore = config
            .common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = config.common_config.max_stream_queries;
        let cache_size = config.common_config.cache_size;
        let root_key = get_big_root_key(root_key);
        Ok(Self {
            store,
            semaphore,
            max_stream_queries,
            cache_size,
            root_key,
        })
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, ScyllaDbStoreError> {
        let store = self.store.clone();
        let semaphore = self.semaphore.clone();
        let max_stream_queries = self.max_stream_queries;
        let cache_size = self.cache_size;
        let root_key = get_big_root_key(root_key);
        Ok(Self {
            store,
            semaphore,
            max_stream_queries,
            cache_size,
            root_key,
        })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, ScyllaDbStoreError> {
        let session = SessionBuilder::new()
            .known_node(config.uri.as_str())
            .build()
            .boxed()
            .await?;
        let miss_msg = "'kv' not found in keyspaces";
        let mut paging_state = None;
        let mut namespaces = Vec::new();
        loop {
            let result = session
                .query_paged("DESCRIBE KEYSPACE kv", &[], paging_state)
                .await;
            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    let invalid_or_not_found = match &error {
                        QueryError::DbError(db_error, msg) => {
                            *db_error == DbError::Invalid && msg.as_str() == miss_msg
                        }
                        _ => false,
                    };
                    if invalid_or_not_found {
                        return Ok(Vec::new());
                    } else {
                        return Err(ScyllaDbStoreError::ScyllaDbQueryError(error));
                    }
                }
            };
            if let Some(rows) = result.rows {
                // The output of the description is the following:
                // * The first column is the keyspace (in that case kv)
                // * The second column is the nature of the object.
                // * The third column is the name
                // * The fourth column is the command that built it.
                for row in rows.into_typed::<(String, String, String, String)>() {
                    let (_, object_kind, name, _) = row?;
                    if object_kind == "table" {
                        namespaces.push(name);
                    }
                }
            }
            if result.paging_state.is_none() {
                return Ok(namespaces);
            }
            paging_state = result.paging_state;
        }
    }

    async fn delete_all(store_config: &Self::Config) -> Result<(), ScyllaDbStoreError> {
        let session = SessionBuilder::new()
            .known_node(store_config.uri.as_str())
            .build()
            .boxed()
            .await?;
        let query = "DROP KEYSPACE IF EXISTS kv;".to_string();
        session.query(query, &[]).await?;
        Ok(())
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, ScyllaDbStoreError> {
        Self::check_namespace(namespace)?;
        let session = SessionBuilder::new()
            .known_node(config.uri.as_str())
            .build()
            .boxed()
            .await?;
        // We check the way the test can fail. It can fail in different ways.
        let query = format!(
            "SELECT root_key FROM kv.{} LIMIT 1 ALLOW FILTERING",
            namespace
        );

        // Execute the query
        let result = session.query(query, &[]).await;

        // The missing table translates into a very specific error that we matched
        let miss_msg1 = format!("unconfigured table {}", namespace);
        let miss_msg1 = miss_msg1.as_str();
        let miss_msg2 = "Undefined name root_key in selection clause";
        let miss_msg3 = "Keyspace kv does not exist";
        let Err(error) = result else {
            // If ok, then the table exists
            return Ok(true);
        };
        let missing_table = match &error {
            QueryError::DbError(db_error, msg) => {
                if *db_error != DbError::Invalid {
                    false
                } else {
                    msg.as_str() == miss_msg1
                        || msg.as_str() == miss_msg2
                        || msg.as_str() == miss_msg3
                }
            }
            _ => false,
        };
        if missing_table {
            Ok(false)
        } else {
            Err(ScyllaDbStoreError::ScyllaDbQueryError(error))
        }
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), ScyllaDbStoreError> {
        Self::check_namespace(namespace)?;
        let session = SessionBuilder::new()
            .known_node(config.uri.as_str())
            .build()
            .boxed()
            .await?;
        // Create a keyspace if it doesn't exist
        let query = "CREATE KEYSPACE IF NOT EXISTS kv WITH REPLICATION = { \
            'class' : 'SimpleStrategy', \
            'replication_factor' : 1 \
        }";
        session.query(query, &[]).await?;

        // Create a table if it doesn't exist
        // The schema appears too complicated for non-trivial reasons.
        // See TODO(#1069).
        let query = format!(
            "CREATE TABLE kv.{} (root_key blob, k blob, v blob, primary key (root_key, k))",
            namespace
        );
        let _query = session.query(query, &[]).await?;
        Ok(())
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), ScyllaDbStoreError> {
        Self::check_namespace(namespace)?;
        let session = SessionBuilder::new()
            .known_node(config.uri.as_str())
            .build()
            .boxed()
            .await?;
        let query = format!("DROP TABLE IF EXISTS kv.{};", namespace);
        session.query(query, &[]).await?;
        Ok(())
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

    fn check_namespace(namespace: &str) -> Result<(), ScyllaDbStoreError> {
        if !namespace.is_empty()
            && namespace.len() <= 48
            && namespace
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Ok(());
        }
        Err(ScyllaDbStoreError::InvalidTableName)
    }
}

/// A shared DB store for ScyllaDB implementing LruCaching
#[derive(Clone)]
pub struct ScyllaDbStore {
    #[cfg(with_metrics)]
    store:
        MeteredStore<LruCachingStore<MeteredStore<JournalingKeyValueStore<ScyllaDbStoreInternal>>>>,
    #[cfg(not(with_metrics))]
    store: LruCachingStore<JournalingKeyValueStore<ScyllaDbStoreInternal>>,
}

/// The type for building a new ScyllaDB Key Value Store
#[derive(Debug)]
pub struct ScyllaDbStoreConfig {
    /// The url to which the requests have to be sent
    pub uri: String,
    /// The common configuration of the key value store
    pub common_config: CommonStoreConfig,
}

impl WithError for ScyllaDbStore {
    type Error = ScyllaDbStoreError;
}

impl ReadableKeyValueStore for ScyllaDbStore {
    const MAX_KEY_SIZE: usize = ScyllaDbStoreInternal::MAX_KEY_SIZE;

    type Keys = <ScyllaDbStoreInternal as ReadableKeyValueStore>::Keys;

    type KeyValues = <ScyllaDbStoreInternal as ReadableKeyValueStore>::KeyValues;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ScyllaDbStoreError> {
        self.store.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ScyllaDbStoreError> {
        self.store.contains_key(key).await
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, ScyllaDbStoreError> {
        self.store.contains_keys(keys).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ScyllaDbStoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        Box::pin(self.store.read_multi_values_bytes(keys)).await
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, ScyllaDbStoreError> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ScyllaDbStoreError> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

impl WritableKeyValueStore for ScyllaDbStore {
    const MAX_VALUE_SIZE: usize = ScyllaDbStoreInternal::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch) -> Result<(), ScyllaDbStoreError> {
        self.store.write_batch(batch).boxed().await
    }

    async fn clear_journal(&self) -> Result<(), ScyllaDbStoreError> {
        self.store.clear_journal().boxed().await
    }
}

impl AdminKeyValueStore for ScyllaDbStore {
    type Config = ScyllaDbStoreConfig;

    async fn new_test_config() -> Result<ScyllaDbStoreConfig, ScyllaDbStoreError> {
        ScyllaDbStoreInternal::new_test_config().await
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, ScyllaDbStoreError> {
        let cache_size = config.common_config.cache_size;
        let simple_store = ScyllaDbStoreInternal::connect(config, namespace, root_key).await?;
        Ok(ScyllaDbStore::from_inner(simple_store, cache_size))
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, ScyllaDbStoreError> {
        let simple_store = self.inner().clone_with_root_key(root_key)?;
        let cache_size = self.inner().cache_size;
        Ok(ScyllaDbStore::from_inner(simple_store, cache_size))
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, ScyllaDbStoreError> {
        ScyllaDbStoreInternal::list_all(config).await
    }

    async fn delete_all(config: &Self::Config) -> Result<(), ScyllaDbStoreError> {
        ScyllaDbStoreInternal::delete_all(config).await
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, ScyllaDbStoreError> {
        ScyllaDbStoreInternal::exists(config, namespace).await
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), ScyllaDbStoreError> {
        ScyllaDbStoreInternal::create(config, namespace).await
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), ScyllaDbStoreError> {
        ScyllaDbStoreInternal::delete(config, namespace).await
    }
}

impl ScyllaDbStore {
    #[cfg(with_metrics)]
    fn inner(&self) -> &ScyllaDbStoreInternal {
        &self.store.store.store.store.store
    }

    #[cfg(not(with_metrics))]
    fn inner(&self) -> &ScyllaDbStoreInternal {
        &self.store.store.store
    }

    fn from_inner(simple_store: ScyllaDbStoreInternal, cache_size: usize) -> ScyllaDbStore {
        let store = JournalingKeyValueStore::new(simple_store);
        #[cfg(with_metrics)]
        let store = MeteredStore::new(&SCYLLA_DB_METRICS, store);
        let store = LruCachingStore::new(store, cache_size);
        #[cfg(with_metrics)]
        let store = MeteredStore::new(&LRU_CACHING_METRICS, store);
        Self { store }
    }
}

/// Creates the common initialization for RocksDB.
pub fn create_scylla_db_common_config() -> CommonStoreConfig {
    CommonStoreConfig {
        max_concurrent_queries: Some(TEST_SCYLLA_DB_MAX_CONCURRENT_QUERIES),
        max_stream_queries: TEST_SCYLLA_DB_MAX_STREAM_QUERIES,
        cache_size: TEST_CACHE_SIZE,
    }
}

/// Creates the URI used for the tests.
pub fn create_scylla_db_test_uri() -> String {
    "localhost:9042".to_string()
}

/// Creates a ScyllaDB test store.
#[cfg(with_testing)]
pub async fn create_scylla_db_test_store() -> ScyllaDbStore {
    let config = ScyllaDbStore::new_test_config().await.expect("config");
    let namespace = generate_test_namespace();
    let root_key = &[];
    ScyllaDbStore::recreate_and_connect(&config, &namespace, root_key)
        .await
        .expect("store")
}
