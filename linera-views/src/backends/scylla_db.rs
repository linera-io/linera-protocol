// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::store::KeyValueStore`] for the ScyllaDB database.
//!
//! The current connection is done via a Session and a corresponding primary key called
//! "namespace". The maximum number of concurrent queries is controlled by
//! `max_concurrent_queries`.

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
    batch::BatchStatement,
    prepared_statement::PreparedStatement,
    statement::batch::BatchType,
    transport::errors::{DbError, QueryError},
    Session, SessionBuilder,
};
use thiserror::Error;

#[cfg(with_metrics)]
use crate::metering::MeteredStore;
#[cfg(with_testing)]
use crate::store::TestKeyValueStore;
use crate::{
    batch::UnorderedBatch,
    common::{get_uleb128_size, get_upper_bound_option},
    journaling::{DirectWritableKeyValueStore, JournalConsistencyError, JournalingKeyValueStore},
    lru_caching::{LruCachingConfig, LruCachingStore},
    store::{
        AdminKeyValueStore, CommonStoreInternalConfig, KeyValueStoreError, ReadableKeyValueStore,
        WithError,
    },
    value_splitting::{ValueSplittingError, ValueSplittingStore},
};

/// Fundamental constant in ScyllaDB: The maximum size of a multi keys query
/// The limit is in reality 100. But we need one entry for the root key.
const MAX_MULTI_KEYS: usize = 99;

/// The maximal size of an operation on ScyllaDB seems to be 16M
/// https://www.scylladb.com/2019/03/27/best-practices-for-scylla-applications/
/// "There is a hard limit at 16MB, and nothing bigger than that can arrive at once
///  at the database at any particular time"
/// So, we set up the maximal size of 16M - 10K for the values and 10K for the keys
/// We also arbitrarily decrease the size by 4000 bytes because an amount of size is
/// taken internally by the database.
const RAW_MAX_VALUE_SIZE: usize = 16762976;
const MAX_KEY_SIZE: usize = 10240;
const MAX_BATCH_TOTAL_SIZE: usize = RAW_MAX_VALUE_SIZE + MAX_KEY_SIZE;

/// The `RAW_MAX_VALUE_SIZE` is the maximum size on the ScyllaDB storage.
/// However, the value being written can also be the serialization of a SimpleUnorderedBatch
/// Therefore the actual `MAX_VALUE_SIZE` is lower.
/// At the maximum the key_size is 1024 bytes (see below) and we pack just one entry.
/// So if the key has 1024 bytes this gets us the inequality
/// `1 + 1 + 1 + serialized_size(MAX_KEY_SIZE)? + serialized_size(x)? <= RAW_MAX_VALUE_SIZE`.
/// and so this simplifies to `1 + 1 + 1 + (2 + 10240) + (4 + x) <= RAW_MAX_VALUE_SIZE`
/// Note on the above formula:
/// * We write 4 because `get_uleb128_size(RAW_MAX_VALUE_SIZE) = 4)`
/// * We write `1 + 1 + 1`  because the `UnorderedBatch` has three entries.
///
/// This gets us to a maximal value of 16752727.
const VISIBLE_MAX_VALUE_SIZE: usize = RAW_MAX_VALUE_SIZE
    - MAX_KEY_SIZE
    - get_uleb128_size(RAW_MAX_VALUE_SIZE)
    - get_uleb128_size(MAX_KEY_SIZE)
    - 1
    - 1
    - 1;

/// The constant 14000 is an empirical constant that was found to be necessary
/// to make the ScyllaDB system work. We have not been able to find this or
/// a similar constant in the source code or the documentation.
/// An experimental approach gets us that 14796 is the latest value that is
/// correct.
const MAX_BATCH_SIZE: usize = 5000;

/// The client for ScyllaDbB:
/// * The session allows to pass queries
/// * The namespace that is being assigned to the database
/// * The prepared queries used for implementing the features of `KeyValueStore`.
struct ScyllaDbClient {
    session: Session,
    namespace: String,
    read_value: PreparedStatement,
    contains_key: PreparedStatement,
    write_batch_delete_prefix_unbounded: BatchStatement,
    write_batch_delete_prefix_bounded: BatchStatement,
    write_batch_deletion: BatchStatement,
    write_batch_insertion: BatchStatement,
    find_keys_by_prefix_unbounded: PreparedStatement,
    find_keys_by_prefix_bounded: PreparedStatement,
    find_key_values_by_prefix_unbounded: PreparedStatement,
    find_key_values_by_prefix_bounded: PreparedStatement,
}

impl ScyllaDbClient {
    fn check_key_size(key: &[u8]) -> Result<(), ScyllaDbStoreInternalError> {
        ensure!(
            key.len() <= MAX_KEY_SIZE,
            ScyllaDbStoreInternalError::KeyTooLong
        );
        Ok(())
    }

    fn check_value_size(value: &[u8]) -> Result<(), ScyllaDbStoreInternalError> {
        ensure!(
            value.len() <= RAW_MAX_VALUE_SIZE,
            ScyllaDbStoreInternalError::ValueTooLong
        );
        Ok(())
    }

    fn check_batch_len(batch: &UnorderedBatch) -> Result<(), ScyllaDbStoreInternalError> {
        ensure!(
            batch.len() <= MAX_BATCH_SIZE,
            ScyllaDbStoreInternalError::BatchTooLong
        );
        Ok(())
    }

    async fn new(session: Session, namespace: &str) -> Result<Self, ScyllaDbStoreInternalError> {
        let namespace = namespace.to_string();
        let query = format!(
            "SELECT v FROM kv.{} WHERE root_key = ? AND k = ? ALLOW FILTERING",
            namespace
        );
        let read_value = session.prepare(query).await?;

        let query = format!(
            "SELECT root_key FROM kv.{} WHERE root_key = ? AND k = ? ALLOW FILTERING",
            namespace
        );
        let contains_key = session.prepare(query).await?;

        let query = format!("DELETE FROM kv.{} WHERE root_key = ? AND k >= ?", namespace);
        let write_batch_delete_prefix_unbounded = session.prepare(query).await?.into();
        let query = format!(
            "DELETE FROM kv.{} WHERE root_key = ? AND k >= ? AND k < ?",
            namespace
        );
        let write_batch_delete_prefix_bounded = session.prepare(query).await?.into();
        let query = format!("DELETE FROM kv.{} WHERE root_key = ? AND k = ?", namespace);
        let write_batch_deletion = session.prepare(query).await?.into();
        let query = format!(
            "INSERT INTO kv.{} (root_key, k, v) VALUES (?, ?, ?)",
            namespace
        );
        let write_batch_insertion = session.prepare(query).await?.into();

        let query = format!(
            "SELECT k FROM kv.{} WHERE root_key = ? AND k >= ? ALLOW FILTERING",
            namespace
        );
        let find_keys_by_prefix_unbounded = session.prepare(query).await?;
        let query = format!(
            "SELECT k FROM kv.{} WHERE root_key = ? AND k >= ? AND k < ? ALLOW FILTERING",
            namespace
        );
        let find_keys_by_prefix_bounded = session.prepare(query).await?;

        let query = format!(
            "SELECT k,v FROM kv.{} WHERE root_key = ? AND k >= ? ALLOW FILTERING",
            namespace
        );
        let find_key_values_by_prefix_unbounded = session.prepare(query).await?;
        let query = format!(
            "SELECT k,v FROM kv.{} WHERE root_key = ? AND k >= ? AND k < ? ALLOW FILTERING",
            namespace
        );
        let find_key_values_by_prefix_bounded = session.prepare(query).await?;

        Ok(Self {
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
        })
    }

    async fn read_value_internal(
        &self,
        root_key: &[u8],
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ScyllaDbStoreInternalError> {
        Self::check_key_size(&key)?;
        let session = &self.session;
        // Read the value of a key
        let values = (root_key.to_vec(), key);

        let results = session
            .execute_unpaged(&self.read_value, &values)
            .await?
            .into_rows_result()?;
        let mut rows = results.rows::<(Vec<u8>,)>()?;
        Ok(match rows.next() {
            Some(row) => Some(row?.0),
            None => None,
        })
    }

    async fn read_multi_values_internal(
        &self,
        root_key: &[u8],
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ScyllaDbStoreInternalError> {
        let num_keys = keys.len();
        let session = &self.session;
        let mut map = HashMap::<Vec<u8>, Vec<usize>>::new();
        let mut inputs = Vec::new();
        inputs.push(root_key.to_vec());
        for (i_key, key) in keys.into_iter().enumerate() {
            Self::check_key_size(&key)?;
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

        let mut rows = session
            .query_iter(&*query, &inputs)
            .await?
            .rows_stream::<(Vec<u8>, Vec<u8>)>()?;

        let mut values = vec![None; num_keys];
        while let Some(row) = rows.next().await {
            let (key, value) = row?;
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
    ) -> Result<Vec<bool>, ScyllaDbStoreInternalError> {
        let num_keys = keys.len();
        let session = &self.session;
        let mut map = HashMap::<Vec<u8>, Vec<usize>>::new();
        let mut inputs = Vec::new();
        inputs.push(root_key.to_vec());
        for (i_key, key) in keys.into_iter().enumerate() {
            Self::check_key_size(&key)?;
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

        let mut rows = session
            .query_iter(&*query, &inputs)
            .await?
            .rows_stream::<(Vec<u8>,)>()?;

        let mut values = vec![false; num_keys];
        while let Some(row) = rows.next().await {
            let (key,) = row?;
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
    ) -> Result<bool, ScyllaDbStoreInternalError> {
        Self::check_key_size(&key)?;
        let session = &self.session;
        // Read the value of a key
        let values = (root_key.to_vec(), key);

        let results = session
            .execute_unpaged(&self.contains_key, &values)
            .await?
            .into_rows_result()?;
        let mut rows = results.rows::<(Vec<u8>,)>()?;
        Ok(rows.next().is_some())
    }

    async fn write_batch_internal(
        &self,
        root_key: &[u8],
        batch: UnorderedBatch,
    ) -> Result<(), ScyllaDbStoreInternalError> {
        let session = &self.session;
        let mut batch_query = scylla::statement::batch::Batch::new(BatchType::Unlogged);
        let mut batch_values = Vec::new();
        let query1 = &self.write_batch_delete_prefix_unbounded;
        let query2 = &self.write_batch_delete_prefix_bounded;
        Self::check_batch_len(&batch)?;
        for key_prefix in batch.key_prefix_deletions {
            Self::check_key_size(&key_prefix)?;
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
            Self::check_key_size(&key)?;
            let values = vec![root_key.to_vec(), key];
            batch_values.push(values);
            batch_query.append_statement(query3.clone());
        }
        let query4 = &self.write_batch_insertion;
        for (key, value) in batch.simple_unordered_batch.insertions {
            Self::check_key_size(&key)?;
            Self::check_value_size(&value)?;
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
    ) -> Result<Vec<Vec<u8>>, ScyllaDbStoreInternalError> {
        Self::check_key_size(&key_prefix)?;
        let session = &self.session;
        // Read the value of a key
        let len = key_prefix.len();
        let query_unbounded = &self.find_keys_by_prefix_unbounded;
        let query_bounded = &self.find_keys_by_prefix_bounded;
        let rows = match get_upper_bound_option(&key_prefix) {
            None => {
                let values = (root_key.to_vec(), key_prefix.clone());
                session
                    .execute_iter(query_unbounded.clone(), values)
                    .await?
            }
            Some(upper_bound) => {
                let values = (root_key.to_vec(), key_prefix.clone(), upper_bound);
                session.execute_iter(query_bounded.clone(), values).await?
            }
        };
        let mut rows = rows.rows_stream::<(Vec<u8>,)>()?;
        let mut keys = Vec::new();
        while let Some(row) = rows.next().await {
            let (key,) = row?;
            let short_key = key[len..].to_vec();
            keys.push(short_key);
        }
        Ok(keys)
    }

    async fn find_key_values_by_prefix_internal(
        &self,
        root_key: &[u8],
        key_prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ScyllaDbStoreInternalError> {
        Self::check_key_size(&key_prefix)?;
        let session = &self.session;
        // Read the value of a key
        let len = key_prefix.len();
        let query_unbounded = &self.find_key_values_by_prefix_unbounded;
        let query_bounded = &self.find_key_values_by_prefix_bounded;
        let rows = match get_upper_bound_option(&key_prefix) {
            None => {
                let values = (root_key.to_vec(), key_prefix.clone());
                session
                    .execute_iter(query_unbounded.clone(), values)
                    .await?
            }
            Some(upper_bound) => {
                let values = (root_key.to_vec(), key_prefix.clone(), upper_bound);
                session.execute_iter(query_bounded.clone(), values).await?
            }
        };
        let mut rows = rows.rows_stream::<(Vec<u8>, Vec<u8>)>()?;
        let mut key_values = Vec::new();
        while let Some(row) = rows.next().await {
            let (key, value) = row?;
            let short_key = key[len..].to_vec();
            key_values.push((short_key, value));
        }
        Ok(key_values)
    }
}

/// The client itself and the keeping of the count of active connections.
#[derive(Clone)]
pub struct ScyllaDbStoreInternal {
    store: Arc<ScyllaDbClient>,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
    root_key: Vec<u8>,
}

/// The error type for [`ScyllaDbStoreInternal`]
#[derive(Error, Debug)]
pub enum ScyllaDbStoreInternalError {
    /// BCS serialization error.
    #[error(transparent)]
    BcsError(#[from] bcs::Error),

    /// The key must have at most ['MAX_KEY_SIZE'] bytes
    #[error("The key must have at most MAX_KEY_SIZE")]
    KeyTooLong,

    /// The value must have at most ['MAX_VALUE_SIZE'] bytes
    #[error("The value must have at most MAX_VALUE_SIZE")]
    ValueTooLong,

    /// A deserialization error in ScyllaDB
    #[error(transparent)]
    DeserializationError(#[from] scylla::deserialize::DeserializationError),

    /// A row error in ScyllaDB
    #[error(transparent)]
    RowsError(#[from] scylla::transport::query_result::RowsError),

    /// A type error in the accessed data in ScyllaDB
    #[error(transparent)]
    IntoRowsResultError(#[from] scylla::transport::query_result::IntoRowsResultError),

    /// A type check error in ScyllaDB
    #[error(transparent)]
    TypeCheckError(#[from] scylla::deserialize::TypeCheckError),

    /// A query error in ScyllaDB
    #[error(transparent)]
    ScyllaDbQueryError(#[from] scylla::transport::errors::QueryError),

    /// A query error in ScyllaDB
    #[error(transparent)]
    ScyllaDbNewSessionError(#[from] scylla::transport::errors::NewSessionError),

    /// Namespace contains forbidden characters
    #[error("Namespace contains forbidden characters")]
    InvalidNamespace,

    /// The journal is not coherent
    #[error(transparent)]
    JournalConsistencyError(#[from] JournalConsistencyError),

    /// The batch is too long to be written
    #[error("The batch is too long to be written")]
    BatchTooLong,
}

impl KeyValueStoreError for ScyllaDbStoreInternalError {
    const BACKEND: &'static str = "scylla_db";
}

impl WithError for ScyllaDbStoreInternal {
    type Error = ScyllaDbStoreInternalError;
}

impl ReadableKeyValueStore for ScyllaDbStoreInternal {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    async fn read_value_bytes(
        &self,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ScyllaDbStoreInternalError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store
            .read_value_internal(&self.root_key, key.to_vec())
            .await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ScyllaDbStoreInternalError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store
            .contains_key_internal(&self.root_key, key.to_vec())
            .await
    }

    async fn contains_keys(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<bool>, ScyllaDbStoreInternalError> {
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
    ) -> Result<Vec<Option<Vec<u8>>>, ScyllaDbStoreInternalError> {
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
    ) -> Result<Self::Keys, ScyllaDbStoreInternalError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store
            .find_keys_by_prefix_internal(&self.root_key, key_prefix.to_vec())
            .await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ScyllaDbStoreInternalError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store
            .find_key_values_by_prefix_internal(&self.root_key, key_prefix.to_vec())
            .await
    }
}

#[async_trait]
impl DirectWritableKeyValueStore for ScyllaDbStoreInternal {
    const MAX_BATCH_SIZE: usize = MAX_BATCH_SIZE;
    const MAX_BATCH_TOTAL_SIZE: usize = MAX_BATCH_TOTAL_SIZE;
    const MAX_VALUE_SIZE: usize = VISIBLE_MAX_VALUE_SIZE;

    // ScyllaDB cannot take a `crate::batch::Batch` directly. Indeed, if a delete is
    // followed by a write, then the delete takes priority. See the sentence "The first
    // tie-breaking rule when two cells have the same write timestamp is that dead cells
    // win over live cells" from
    // https://github.com/scylladb/scylladb/blob/master/docs/dev/timestamp-conflict-resolution.md
    type Batch = UnorderedBatch;

    async fn write_batch(&self, batch: Self::Batch) -> Result<(), ScyllaDbStoreInternalError> {
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

/// The type for building a new ScyllaDB Key Value Store
#[derive(Debug)]
pub struct ScyllaDbStoreInternalConfig {
    /// The url to which the requests have to be sent
    pub uri: String,
    /// The common configuration of the key value store
    common_config: CommonStoreInternalConfig,
}

impl AdminKeyValueStore for ScyllaDbStoreInternal {
    type Config = ScyllaDbStoreInternalConfig;

    fn get_name() -> String {
        "scylladb internal".to_string()
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, ScyllaDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let session = SessionBuilder::new()
            .known_node(config.uri.as_str())
            .build()
            .boxed()
            .await?;
        let store = ScyllaDbClient::new(session, namespace).await?;
        let store = Arc::new(store);
        let semaphore = config
            .common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = config.common_config.max_stream_queries;
        let root_key = get_big_root_key(root_key);
        Ok(Self {
            store,
            semaphore,
            max_stream_queries,
            root_key,
        })
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, ScyllaDbStoreInternalError> {
        let store = self.store.clone();
        let semaphore = self.semaphore.clone();
        let max_stream_queries = self.max_stream_queries;
        let root_key = get_big_root_key(root_key);
        Ok(Self {
            store,
            semaphore,
            max_stream_queries,
            root_key,
        })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, ScyllaDbStoreInternalError> {
        let session = SessionBuilder::new()
            .known_node(config.uri.as_str())
            .build()
            .boxed()
            .await?;
        let result = session.query_iter("DESCRIBE KEYSPACE kv", &[]).await;
        let miss_msg = "'kv' not found in keyspaces";
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
                    return Err(ScyllaDbStoreInternalError::ScyllaDbQueryError(error));
                }
            }
        };
        let mut namespaces = Vec::new();
        let mut rows_stream = result.rows_stream::<(String, String, String, String)>()?;
        while let Some(row) = rows_stream.next().await {
            let (_, object_kind, name, _) = row?;
            if object_kind == "table" {
                namespaces.push(name);
            }
        }
        Ok(namespaces)
    }

    async fn delete_all(store_config: &Self::Config) -> Result<(), ScyllaDbStoreInternalError> {
        let session = SessionBuilder::new()
            .known_node(store_config.uri.as_str())
            .build()
            .boxed()
            .await?;

        let query = "DROP KEYSPACE IF EXISTS kv;";
        let prepared = session.prepare(query).await?;

        session.execute_unpaged(&prepared, &[]).await?;
        Ok(())
    }

    async fn exists(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<bool, ScyllaDbStoreInternalError> {
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
        let result = session.prepare(&*query).await;

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
            Err(ScyllaDbStoreInternalError::ScyllaDbQueryError(error))
        }
    }

    async fn create(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<(), ScyllaDbStoreInternalError> {
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

        // Execute the query
        let prepared = session.prepare(query).await?;
        session.execute_unpaged(&prepared, &[]).await?;

        // Create a table if it doesn't exist
        // The schema appears too complicated for non-trivial reasons.
        // See TODO(#1069).
        let query = format!(
            "CREATE TABLE kv.{} (root_key blob, k blob, v blob, primary key (root_key, k))",
            namespace
        );

        // Execute the query
        let prepared = session.prepare(&*query).await?;
        session.execute_unpaged(&prepared, &[]).await?;
        Ok(())
    }

    async fn delete(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<(), ScyllaDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let session = SessionBuilder::new()
            .known_node(config.uri.as_str())
            .build()
            .boxed()
            .await?;

        let query = format!("DROP TABLE IF EXISTS kv.{};", namespace);
        let prepared = session.prepare(&*query).await?;
        let _result = session.execute_unpaged(&prepared, &[]).await?;
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

    fn check_namespace(namespace: &str) -> Result<(), ScyllaDbStoreInternalError> {
        if !namespace.is_empty()
            && namespace.len() <= 48
            && namespace
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Ok(());
        }
        Err(ScyllaDbStoreInternalError::InvalidNamespace)
    }
}

/// We limit the number of connections that can be done for tests.
#[cfg(with_testing)]
const TEST_SCYLLA_DB_MAX_CONCURRENT_QUERIES: usize = 10;

/// The number of connections in the stream is limited for tests.
#[cfg(with_testing)]
const TEST_SCYLLA_DB_MAX_STREAM_QUERIES: usize = 10;

#[cfg(with_testing)]
impl TestKeyValueStore for JournalingKeyValueStore<ScyllaDbStoreInternal> {
    async fn new_test_config() -> Result<ScyllaDbStoreInternalConfig, ScyllaDbStoreInternalError> {
        let uri = "localhost:9042".to_string();
        let common_config = CommonStoreInternalConfig {
            max_concurrent_queries: Some(TEST_SCYLLA_DB_MAX_CONCURRENT_QUERIES),
            max_stream_queries: TEST_SCYLLA_DB_MAX_STREAM_QUERIES,
        };
        Ok(ScyllaDbStoreInternalConfig { uri, common_config })
    }
}

/// The `ScyllaDbStore` composed type with metrics
#[cfg(with_metrics)]
pub type ScyllaDbStore = MeteredStore<
    LruCachingStore<
        MeteredStore<
            ValueSplittingStore<MeteredStore<JournalingKeyValueStore<ScyllaDbStoreInternal>>>,
        >,
    >,
>;

/// The `ScyllaDbStore` composed type
#[cfg(not(with_metrics))]
pub type ScyllaDbStore =
    LruCachingStore<ValueSplittingStore<JournalingKeyValueStore<ScyllaDbStoreInternal>>>;

/// The `ScyllaDbStoreConfig` input type
pub type ScyllaDbStoreConfig = LruCachingConfig<ScyllaDbStoreInternalConfig>;

impl ScyllaDbStoreConfig {
    /// Creates a `ScyllaDbStoreConfig` from the inputs.
    pub fn new(uri: String, common_config: crate::store::CommonStoreConfig) -> ScyllaDbStoreConfig {
        let inner_config = ScyllaDbStoreInternalConfig {
            uri,
            common_config: common_config.reduced(),
        };
        ScyllaDbStoreConfig {
            inner_config,
            cache_size: common_config.cache_size,
        }
    }
}

/// The combined error type for the `ScyllaDbStore`.
pub type ScyllaDbStoreError = ValueSplittingError<ScyllaDbStoreInternalError>;
