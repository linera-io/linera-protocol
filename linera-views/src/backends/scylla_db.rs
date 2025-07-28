// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implements [`crate::store::KeyValueStore`] for the ScyllaDB database.
//!
//! The current connection is done via a Session and a corresponding primary key called
//! "namespace". The maximum number of concurrent queries is controlled by
//! `max_concurrent_queries`.

use std::{
    collections::{BTreeSet, HashMap},
    ops::Deref,
    sync::Arc,
};

use async_lock::{Semaphore, SemaphoreGuard};
use dashmap::DashMap;
use futures::{future::join_all, StreamExt as _};
use linera_base::ensure;
use scylla::{
    client::{
        execution_profile::{ExecutionProfile, ExecutionProfileHandle},
        session::Session,
        session_builder::SessionBuilder,
    },
    deserialize::{DeserializationError, TypeCheckError},
    errors::{
        DbError, ExecutionError, IntoRowsResultError, NewSessionError, NextPageError, NextRowError,
        PagerExecutionError, PrepareError, RequestAttemptError, RequestError, RowsError,
    },
    policies::{
        load_balancing::{DefaultPolicy, LoadBalancingPolicy},
        retry::DefaultRetryPolicy,
    },
    response::PagingState,
    statement::{batch::BatchType, prepared::PreparedStatement, Consistency},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(with_metrics)]
use crate::metering::MeteredDatabase;
#[cfg(with_testing)]
use crate::store::TestKeyValueDatabase;
use crate::{
    batch::UnorderedBatch,
    common::{get_uleb128_size, get_upper_bound_option},
    journaling::{JournalConsistencyError, JournalingKeyValueDatabase},
    lru_caching::{LruCachingConfig, LruCachingDatabase},
    store::{
        DirectWritableKeyValueStore, KeyValueDatabase, KeyValueStoreError, ReadableKeyValueStore,
        WithError,
    },
    value_splitting::{ValueSplittingDatabase, ValueSplittingError},
    FutureSyncExt as _,
};

/// Fundamental constant in ScyllaDB: The maximum size of a multi keys query
/// The limit is in reality 100. But we need one entry for the root key.
const MAX_MULTI_KEYS: usize = 100 - 1;

/// The maximal size of an operation on ScyllaDB seems to be 16 MiB
/// https://www.scylladb.com/2019/03/27/best-practices-for-scylla-applications/
/// "There is a hard limit at 16 MiB, and nothing bigger than that can arrive at once
///  at the database at any particular time"
/// So, we set up the maximal size of 16 MiB - 10 KiB for the values and 10 KiB for the keys
/// We also arbitrarily decrease the size by 4000 bytes because an amount of size is
/// taken internally by the database.
const RAW_MAX_VALUE_SIZE: usize = 16 * 1024 * 1024 - 10 * 1024 - 4000;
const MAX_KEY_SIZE: usize = 10 * 1024;
const MAX_BATCH_TOTAL_SIZE: usize = RAW_MAX_VALUE_SIZE + MAX_KEY_SIZE;

/// The `RAW_MAX_VALUE_SIZE` is the maximum size on the ScyllaDB storage.
/// However, the value being written can also be the serialization of a `SimpleUnorderedBatch`
/// Therefore the actual `MAX_VALUE_SIZE` is lower.
/// At the maximum the key size is 1024 bytes (see below) and we pack just one entry.
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
    - 3;

/// The constant 14000 is an empirical constant that was found to be necessary
/// to make the ScyllaDB system work. We have not been able to find this or
/// a similar constant in the source code or the documentation.
/// An experimental approach gets us that 14796 is the latest value that is
/// correct.
const MAX_BATCH_SIZE: usize = 5000;

/// The keyspace to use for the ScyllaDB database.
const KEYSPACE: &str = "kv";

/// The client for ScyllaDB:
/// * The session allows to pass queries
/// * The namespace that is being assigned to the database
/// * The prepared queries used for implementing the features of `KeyValueStore`.
struct ScyllaDbClient {
    session: Session,
    namespace: String,
    read_value: PreparedStatement,
    contains_key: PreparedStatement,
    write_batch_delete_prefix_unbounded: PreparedStatement,
    write_batch_delete_prefix_bounded: PreparedStatement,
    write_batch_deletion: PreparedStatement,
    write_batch_insertion: PreparedStatement,
    find_keys_by_prefix_unbounded: PreparedStatement,
    find_keys_by_prefix_bounded: PreparedStatement,
    find_key_values_by_prefix_unbounded: PreparedStatement,
    find_key_values_by_prefix_bounded: PreparedStatement,
    multi_key_values: DashMap<usize, PreparedStatement>,
    multi_keys: DashMap<usize, PreparedStatement>,
}

impl ScyllaDbClient {
    async fn new(session: Session, namespace: &str) -> Result<Self, ScyllaDbStoreInternalError> {
        let namespace = namespace.to_string();
        let read_value = session
            .prepare(format!(
                "SELECT v FROM {}.{} WHERE root_key = ? AND k = ?",
                KEYSPACE, namespace
            ))
            .await?;

        let contains_key = session
            .prepare(format!(
                "SELECT root_key FROM {}.{} WHERE root_key = ? AND k = ?",
                KEYSPACE, namespace
            ))
            .await?;

        let write_batch_delete_prefix_unbounded = session
            .prepare(format!(
                "DELETE FROM {}.{} WHERE root_key = ? AND k >= ?",
                KEYSPACE, namespace
            ))
            .await?;

        let write_batch_delete_prefix_bounded = session
            .prepare(format!(
                "DELETE FROM {}.{} WHERE root_key = ? AND k >= ? AND k < ?",
                KEYSPACE, namespace
            ))
            .await?;

        let write_batch_deletion = session
            .prepare(format!(
                "DELETE FROM {}.{} WHERE root_key = ? AND k = ?",
                KEYSPACE, namespace
            ))
            .await?;

        let write_batch_insertion = session
            .prepare(format!(
                "INSERT INTO {}.{} (root_key, k, v) VALUES (?, ?, ?)",
                KEYSPACE, namespace
            ))
            .await?;

        let find_keys_by_prefix_unbounded = session
            .prepare(format!(
                "SELECT k FROM {}.{} WHERE root_key = ? AND k >= ?",
                KEYSPACE, namespace
            ))
            .await?;

        let find_keys_by_prefix_bounded = session
            .prepare(format!(
                "SELECT k FROM {}.{} WHERE root_key = ? AND k >= ? AND k < ?",
                KEYSPACE, namespace
            ))
            .await?;

        let find_key_values_by_prefix_unbounded = session
            .prepare(format!(
                "SELECT k,v FROM {}.{} WHERE root_key = ? AND k >= ?",
                KEYSPACE, namespace
            ))
            .await?;

        let find_key_values_by_prefix_bounded = session
            .prepare(format!(
                "SELECT k,v FROM {}.{} WHERE root_key = ? AND k >= ? AND k < ?",
                KEYSPACE, namespace
            ))
            .await?;

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
            multi_key_values: DashMap::new(),
            multi_keys: DashMap::new(),
        })
    }

    fn build_default_policy() -> Arc<dyn LoadBalancingPolicy> {
        DefaultPolicy::builder().token_aware(true).build()
    }

    fn build_default_execution_profile_handle(
        policy: Arc<dyn LoadBalancingPolicy>,
    ) -> ExecutionProfileHandle {
        let default_profile = ExecutionProfile::builder()
            .load_balancing_policy(policy)
            .retry_policy(Arc::new(DefaultRetryPolicy::new()))
            .consistency(Consistency::LocalQuorum)
            .build();
        default_profile.into_handle()
    }

    async fn build_default_session(uri: &str) -> Result<Session, ScyllaDbStoreInternalError> {
        // This explicitly sets a lot of default parameters for clarity and for making future changes
        // easier.
        SessionBuilder::new()
            .known_node(uri)
            .default_execution_profile_handle(Self::build_default_execution_profile_handle(
                Self::build_default_policy(),
            ))
            .build()
            .boxed_sync()
            .await
            .map_err(Into::into)
    }

    async fn get_multi_key_values_statement(
        &self,
        num_markers: usize,
    ) -> Result<PreparedStatement, ScyllaDbStoreInternalError> {
        if let Some(prepared_statement) = self.multi_key_values.get(&num_markers) {
            return Ok(prepared_statement.clone());
        }
        let markers = std::iter::repeat_n("?", num_markers)
            .collect::<Vec<_>>()
            .join(",");
        let prepared_statement = self
            .session
            .prepare(format!(
                "SELECT k,v FROM {}.{} WHERE root_key = ? AND k IN ({})",
                KEYSPACE, self.namespace, markers
            ))
            .await?;
        self.multi_key_values
            .insert(num_markers, prepared_statement.clone());
        Ok(prepared_statement)
    }

    async fn get_multi_keys_statement(
        &self,
        num_markers: usize,
    ) -> Result<PreparedStatement, ScyllaDbStoreInternalError> {
        if let Some(prepared_statement) = self.multi_keys.get(&num_markers) {
            return Ok(prepared_statement.clone());
        };
        let markers = std::iter::repeat_n("?", num_markers)
            .collect::<Vec<_>>()
            .join(",");
        let prepared_statement = self
            .session
            .prepare(format!(
                "SELECT k FROM {}.{} WHERE root_key = ? AND k IN ({})",
                KEYSPACE, self.namespace, markers
            ))
            .await?;
        self.multi_keys
            .insert(num_markers, prepared_statement.clone());
        Ok(prepared_statement)
    }

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

    async fn read_value_internal(
        &self,
        root_key: &[u8],
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ScyllaDbStoreInternalError> {
        Self::check_key_size(&key)?;
        let session = &self.session;
        // Read the value of a key
        let values = (root_key.to_vec(), key);

        let (result, _) = session
            .execute_single_page(&self.read_value, &values, PagingState::start())
            .await?;
        let rows = result.into_rows_result()?;
        let mut rows = rows.rows::<(Vec<u8>,)>()?;
        Ok(match rows.next() {
            Some(row) => Some(row?.0),
            None => None,
        })
    }

    fn get_occurrences_map(
        keys: Vec<Vec<u8>>,
    ) -> Result<HashMap<Vec<u8>, Vec<usize>>, ScyllaDbStoreInternalError> {
        let mut map = HashMap::<Vec<u8>, Vec<usize>>::new();
        for (i_key, key) in keys.into_iter().enumerate() {
            Self::check_key_size(&key)?;
            map.entry(key).or_default().push(i_key);
        }
        Ok(map)
    }

    async fn read_multi_values_internal(
        &self,
        root_key: &[u8],
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ScyllaDbStoreInternalError> {
        let mut values = vec![None; keys.len()];
        let map = Self::get_occurrences_map(keys)?;
        let statement = self.get_multi_key_values_statement(map.len()).await?;
        let mut inputs = vec![root_key.to_vec()];
        inputs.extend(map.keys().cloned());
        let mut rows = self
            .session
            .execute_iter(statement, &inputs)
            .await?
            .rows_stream::<(Vec<u8>, Vec<u8>)>()?;

        while let Some(row) = rows.next().await {
            let (key, value) = row?;
            for i_key in &map[&key] {
                values[*i_key] = Some(value.clone());
            }
        }
        Ok(values)
    }

    async fn contains_keys_internal(
        &self,
        root_key: &[u8],
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<bool>, ScyllaDbStoreInternalError> {
        let mut values = vec![false; keys.len()];
        let map = Self::get_occurrences_map(keys)?;
        let statement = self.get_multi_keys_statement(map.len()).await?;
        let mut inputs = vec![root_key.to_vec()];
        inputs.extend(map.keys().cloned());
        let mut rows = self
            .session
            .execute_iter(statement, &inputs)
            .await?
            .rows_stream::<(Vec<u8>,)>()?;

        while let Some(row) = rows.next().await {
            let (key,) = row?;
            for i_key in &map[&key] {
                values[*i_key] = true;
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

        let (result, _) = session
            .execute_single_page(&self.contains_key, &values, PagingState::start())
            .await?;
        let rows = result.into_rows_result()?;
        let mut rows = rows.rows::<(Vec<u8>,)>()?;
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

/// Database-level connection to ScyllaDB for managing namespaces and partitions.
#[derive(Clone)]
pub struct ScyllaDbDatabaseInternal {
    store: Arc<ScyllaDbClient>,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
}

impl WithError for ScyllaDbDatabaseInternal {
    type Error = ScyllaDbStoreInternalError;
}

/// The error type for [`ScyllaDbStoreInternal`]
#[derive(Error, Debug)]
pub enum ScyllaDbStoreInternalError {
    /// BCS serialization error.
    #[error(transparent)]
    BcsError(#[from] bcs::Error),

    /// The key must have at most `MAX_KEY_SIZE` bytes
    #[error("The key must have at most MAX_KEY_SIZE")]
    KeyTooLong,

    /// The value must have at most `RAW_MAX_VALUE_SIZE` bytes
    #[error("The value must have at most RAW_MAX_VALUE_SIZE")]
    ValueTooLong,

    /// A deserialization error in ScyllaDB
    #[error(transparent)]
    DeserializationError(#[from] DeserializationError),

    /// A row error in ScyllaDB
    #[error(transparent)]
    RowsError(#[from] RowsError),

    /// A type error in the accessed data in ScyllaDB
    #[error(transparent)]
    IntoRowsResultError(#[from] IntoRowsResultError),

    /// A type check error in ScyllaDB
    #[error(transparent)]
    TypeCheckError(#[from] TypeCheckError),

    /// A query error in ScyllaDB
    #[error(transparent)]
    PagerExecutionError(#[from] PagerExecutionError),

    /// A query error in ScyllaDB
    #[error(transparent)]
    ScyllaDbNewSessionError(#[from] NewSessionError),

    /// Namespace contains forbidden characters
    #[error("Namespace contains forbidden characters")]
    InvalidNamespace,

    /// The journal is not coherent
    #[error(transparent)]
    JournalConsistencyError(#[from] JournalConsistencyError),

    /// The batch is too long to be written
    #[error("The batch is too long to be written")]
    BatchTooLong,

    /// A prepare error in ScyllaDB
    #[error(transparent)]
    PrepareError(#[from] PrepareError),

    /// An execution error in ScyllaDB
    #[error(transparent)]
    ExecutionError(#[from] ExecutionError),

    /// A next row error in ScyllaDB
    #[error(transparent)]
    NextRowError(#[from] NextRowError),
}

impl KeyValueStoreError for ScyllaDbStoreInternalError {
    const BACKEND: &'static str = "scylla_db";
}

impl WithError for ScyllaDbStoreInternal {
    type Error = ScyllaDbStoreInternalError;
}

impl ReadableKeyValueStore for ScyllaDbStoreInternal {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;

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
    ) -> Result<Vec<Vec<u8>>, ScyllaDbStoreInternalError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store
            .find_keys_by_prefix_internal(&self.root_key, key_prefix.to_vec())
            .await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ScyllaDbStoreInternalError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store
            .find_key_values_by_prefix_internal(&self.root_key, key_prefix.to_vec())
            .await
    }
}

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

// ScyllaDB requires that the keys are non-empty.
fn get_big_root_key(root_key: &[u8]) -> Vec<u8> {
    let mut big_key = vec![0];
    big_key.extend(root_key);
    big_key
}

/// The type for building a new ScyllaDB Key Value Store
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScyllaDbStoreInternalConfig {
    /// The URL to which the requests have to be sent
    pub uri: String,
    /// Maximum number of concurrent database queries allowed for this client.
    pub max_concurrent_queries: Option<usize>,
    /// Preferred buffer size for async streams.
    pub max_stream_queries: usize,
    /// The replication factor.
    pub replication_factor: u32,
}

impl KeyValueDatabase for ScyllaDbDatabaseInternal {
    type Config = ScyllaDbStoreInternalConfig;
    type Store = ScyllaDbStoreInternal;

    fn get_name() -> String {
        "scylladb internal".to_string()
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<Self, ScyllaDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let session = ScyllaDbClient::build_default_session(&config.uri).await?;
        let store = ScyllaDbClient::new(session, namespace).await?;
        let store = Arc::new(store);
        let semaphore = config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = config.max_stream_queries;
        Ok(Self {
            store,
            semaphore,
            max_stream_queries,
        })
    }

    fn open_shared(&self, root_key: &[u8]) -> Result<Self::Store, ScyllaDbStoreInternalError> {
        let store = self.store.clone();
        let semaphore = self.semaphore.clone();
        let max_stream_queries = self.max_stream_queries;
        let root_key = get_big_root_key(root_key);
        Ok(ScyllaDbStoreInternal {
            store,
            semaphore,
            max_stream_queries,
            root_key,
        })
    }

    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self::Store, ScyllaDbStoreInternalError> {
        self.open_shared(root_key)
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, ScyllaDbStoreInternalError> {
        let session = ScyllaDbClient::build_default_session(&config.uri).await?;
        let statement = session
            .prepare(format!("DESCRIBE KEYSPACE {}", KEYSPACE))
            .await?;
        let result = session.execute_iter(statement, &[]).await;
        let miss_msg = format!("'{}' not found in keyspaces", KEYSPACE);
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                let invalid_or_keyspace_not_found = match &error {
                    PagerExecutionError::NextPageError(NextPageError::RequestFailure(
                        RequestError::LastAttemptError(RequestAttemptError::DbError(db_error, msg)),
                    )) => *db_error == DbError::Invalid && msg.as_str() == miss_msg,
                    _ => false,
                };
                if invalid_or_keyspace_not_found {
                    return Ok(Vec::new());
                } else {
                    return Err(ScyllaDbStoreInternalError::PagerExecutionError(error));
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

    async fn list_root_keys(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<Vec<Vec<u8>>, ScyllaDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let session = ScyllaDbClient::build_default_session(&config.uri).await?;
        let statement = session
            .prepare(format!(
                "SELECT root_key FROM {}.{} ALLOW FILTERING",
                KEYSPACE, namespace
            ))
            .await?;

        // Execute the query
        let rows = session.execute_iter(statement, &[]).await?;
        let mut rows = rows.rows_stream::<(Vec<u8>,)>()?;
        let mut root_keys = BTreeSet::new();
        while let Some(row) = rows.next().await {
            let (root_key,) = row?;
            let root_key = root_key[1..].to_vec();
            root_keys.insert(root_key);
        }
        Ok(root_keys.into_iter().collect::<Vec<_>>())
    }

    async fn delete_all(store_config: &Self::Config) -> Result<(), ScyllaDbStoreInternalError> {
        let session = ScyllaDbClient::build_default_session(&store_config.uri).await?;
        let statement = session
            .prepare(format!("DROP KEYSPACE IF EXISTS {}", KEYSPACE))
            .await?;

        session
            .execute_single_page(&statement, &[], PagingState::start())
            .await?;
        Ok(())
    }

    async fn exists(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<bool, ScyllaDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let session = ScyllaDbClient::build_default_session(&config.uri).await?;

        // We check the way the test can fail. It can fail in different ways.
        let result = session
            .prepare(format!(
                "SELECT root_key FROM {}.{} LIMIT 1 ALLOW FILTERING",
                KEYSPACE, namespace
            ))
            .await;

        // The missing table translates into a very specific error that we matched
        let miss_msg1 = format!("unconfigured table {}", namespace);
        let miss_msg1 = miss_msg1.as_str();
        let miss_msg2 = "Undefined name root_key in selection clause";
        let miss_msg3 = format!("Keyspace {} does not exist", KEYSPACE);
        let Err(error) = result else {
            // If OK, then the table exists
            return Ok(true);
        };
        let missing_table = match &error {
            PrepareError::AllAttemptsFailed {
                first_attempt: RequestAttemptError::DbError(db_error, msg),
            } => {
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
            Err(ScyllaDbStoreInternalError::PrepareError(error))
        }
    }

    async fn create(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<(), ScyllaDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let session = ScyllaDbClient::build_default_session(&config.uri).await?;

        // Create a keyspace if it doesn't exist
        let statement = session
            .prepare(format!(
                "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{ \
                    'class' : 'NetworkTopologyStrategy', \
                    'replication_factor' : {} \
                }}",
                KEYSPACE, config.replication_factor
            ))
            .await?;
        session
            .execute_single_page(&statement, &[], PagingState::start())
            .await?;

        // This explicitly sets a lot of default parameters for clarity and for making future
        // changes easier.
        let statement = session
            .prepare(format!(
                "CREATE TABLE {}.{} (\
                    root_key blob, \
                    k blob, \
                    v blob, \
                    PRIMARY KEY (root_key, k) \
                ) \
                WITH compaction = {{ \
                    'class'            : 'SizeTieredCompactionStrategy', \
                    'min_sstable_size' : 52428800, \
                    'bucket_low'       : 0.5, \
                    'bucket_high'      : 1.5, \
                    'min_threshold'    : 4, \
                    'max_threshold'    : 32 \
                }} \
                AND compression = {{ \
                    'sstable_compression': 'LZ4Compressor', \
                    'chunk_length_in_kb':'4' \
                }} \
                AND caching = {{ \
                    'enabled': 'true' \
                }}",
                KEYSPACE, namespace
            ))
            .await?;
        session
            .execute_single_page(&statement, &[], PagingState::start())
            .await?;
        Ok(())
    }

    async fn delete(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<(), ScyllaDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let session = ScyllaDbClient::build_default_session(&config.uri).await?;
        let statement = session
            .prepare(format!("DROP TABLE IF EXISTS {}.{};", KEYSPACE, namespace))
            .await?;
        session
            .execute_single_page(&statement, &[], PagingState::start())
            .await?;
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
}

impl ScyllaDbDatabaseInternal {
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

#[cfg(with_testing)]
impl TestKeyValueDatabase for JournalingKeyValueDatabase<ScyllaDbDatabaseInternal> {
    async fn new_test_config() -> Result<ScyllaDbStoreInternalConfig, ScyllaDbStoreInternalError> {
        // TODO(#4114): Read the port from an environment variable.
        let uri = "localhost:9042".to_string();
        Ok(ScyllaDbStoreInternalConfig {
            uri,
            max_concurrent_queries: Some(10),
            max_stream_queries: 10,
            replication_factor: 1,
        })
    }
}

/// The `ScyllaDbDatabase` composed type with metrics
#[cfg(with_metrics)]
pub type ScyllaDbDatabase = MeteredDatabase<
    LruCachingDatabase<
        MeteredDatabase<
            ValueSplittingDatabase<
                MeteredDatabase<JournalingKeyValueDatabase<ScyllaDbDatabaseInternal>>,
            >,
        >,
    >,
>;

/// The `ScyllaDbDatabase` composed type
#[cfg(not(with_metrics))]
pub type ScyllaDbDatabase = LruCachingDatabase<
    ValueSplittingDatabase<JournalingKeyValueDatabase<ScyllaDbDatabaseInternal>>,
>;

/// The `ScyllaDbStoreConfig` input type
pub type ScyllaDbStoreConfig = LruCachingConfig<ScyllaDbStoreInternalConfig>;

/// The combined error type for the `ScyllaDbDatabase`.
pub type ScyllaDbStoreError = ValueSplittingError<ScyllaDbStoreInternalError>;
