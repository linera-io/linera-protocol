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
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use async_lock::{Semaphore, SemaphoreGuard};
use futures::{future::join_all, StreamExt as _};
use linera_base::{ensure, util::future::FutureSyncExt as _};
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
    value::CqlValue,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(with_metrics)]
use crate::metering::MeteredDatabase;
#[cfg(with_testing)]
use crate::store::TestKeyValueDatabase;
use crate::{
    batch::{SimpleUnorderedBatch, UnorderedBatch},
    common::{get_uleb128_size, get_upper_bound_option},
    journaling::{JournalingError, JournalingKeyValueDatabase},
    lru_caching::{LruCachingConfig, LruCachingDatabase},
    store::{
        DirectWritableKeyValueStore, KeyValueDatabase, KeyValueStoreError, ReadableKeyValueStore,
        WithError,
    },
    value_splitting::{ValueSplittingDatabase, ValueSplittingError},
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
    read_writetime: PreparedStatement,
    contains_key: PreparedStatement,
    write_batch_delete_prefix_unbounded: PreparedStatement,
    write_batch_delete_prefix_bounded: PreparedStatement,
    write_batch_deletion: PreparedStatement,
    write_batch_insertion: PreparedStatement,
    // Variants carrying an explicit `USING TIMESTAMP ?` marker, used by the
    // single-batch exclusive-mode write path (`write_batch_exclusive`).
    write_batch_delete_prefix_unbounded_ts: PreparedStatement,
    write_batch_delete_prefix_bounded_ts: PreparedStatement,
    write_batch_deletion_ts: PreparedStatement,
    write_batch_insertion_ts: PreparedStatement,
    find_keys_by_prefix_unbounded: PreparedStatement,
    find_keys_by_prefix_bounded: PreparedStatement,
    find_key_values_by_prefix_unbounded: PreparedStatement,
    find_key_values_by_prefix_bounded: PreparedStatement,
    multi_key_values: papaya::HashMap<usize, PreparedStatement>,
    multi_keys: papaya::HashMap<usize, PreparedStatement>,
}

impl ScyllaDbClient {
    async fn new(session: Session, namespace: &str) -> Result<Self, ScyllaDbStoreInternalError> {
        let namespace = namespace.to_string();
        let read_value = session
            .prepare(format!(
                "SELECT v FROM {KEYSPACE}.\"{namespace}\" WHERE root_key = ? AND k = ?"
            ))
            .await?;

        let read_writetime = session
            .prepare(format!(
                "SELECT WRITETIME(v) FROM {KEYSPACE}.{namespace} WHERE root_key = ? AND k = ?"
            ))
            .await?;

        let contains_key = session
            .prepare(format!(
                "SELECT root_key FROM {KEYSPACE}.\"{namespace}\" WHERE root_key = ? AND k = ?"
            ))
            .await?;

        let write_batch_delete_prefix_unbounded = session
            .prepare(format!(
                "DELETE FROM {KEYSPACE}.\"{namespace}\" WHERE root_key = ? AND k >= ?"
            ))
            .await?;

        let write_batch_delete_prefix_bounded = session
            .prepare(format!(
                "DELETE FROM {KEYSPACE}.\"{namespace}\" WHERE root_key = ? AND k >= ? AND k < ?"
            ))
            .await?;

        let write_batch_deletion = session
            .prepare(format!(
                "DELETE FROM {KEYSPACE}.\"{namespace}\" WHERE root_key = ? AND k = ?"
            ))
            .await?;

        let write_batch_insertion = session
            .prepare(format!(
                "INSERT INTO {KEYSPACE}.\"{namespace}\" (root_key, k, v) VALUES (?, ?, ?)"
            ))
            .await?;

        // Timestamped variants used by the single-batch exclusive-mode path. The
        // explicit `USING TIMESTAMP ?` lets prefix-deletions (`T`) and the
        // insertions/deletions (`T + 1`) share one atomic batch without the range
        // tombstone shadowing the inserts.
        let write_batch_delete_prefix_unbounded_ts = session
            .prepare(format!(
                "DELETE FROM {KEYSPACE}.{namespace} USING TIMESTAMP ? WHERE root_key = ? AND k >= ?"
            ))
            .await?;

        let write_batch_delete_prefix_bounded_ts = session
            .prepare(format!(
                "DELETE FROM {KEYSPACE}.{namespace} USING TIMESTAMP ? \
                 WHERE root_key = ? AND k >= ? AND k < ?"
            ))
            .await?;

        let write_batch_deletion_ts = session
            .prepare(format!(
                "DELETE FROM {KEYSPACE}.{namespace} USING TIMESTAMP ? WHERE root_key = ? AND k = ?"
            ))
            .await?;

        let write_batch_insertion_ts = session
            .prepare(format!(
                "INSERT INTO {KEYSPACE}.{namespace} (root_key, k, v) VALUES (?, ?, ?) \
                 USING TIMESTAMP ?"
            ))
            .await?;

        let find_keys_by_prefix_unbounded = session
            .prepare(format!(
                "SELECT k FROM {KEYSPACE}.\"{namespace}\" WHERE root_key = ? AND k >= ?"
            ))
            .await?;

        let find_keys_by_prefix_bounded = session
            .prepare(format!(
                "SELECT k FROM {KEYSPACE}.\"{namespace}\" WHERE root_key = ? AND k >= ? AND k < ?"
            ))
            .await?;

        let find_key_values_by_prefix_unbounded = session
            .prepare(format!(
                "SELECT k,v FROM {KEYSPACE}.\"{namespace}\" WHERE root_key = ? AND k >= ?"
            ))
            .await?;

        let find_key_values_by_prefix_bounded = session
            .prepare(format!(
                "SELECT k,v FROM {KEYSPACE}.\"{namespace}\" WHERE root_key = ? AND k >= ? AND k < ?"
            ))
            .await?;

        Ok(Self {
            session,
            namespace,
            read_value,
            read_writetime,
            contains_key,
            write_batch_delete_prefix_unbounded,
            write_batch_delete_prefix_bounded,
            write_batch_deletion,
            write_batch_insertion,
            write_batch_delete_prefix_unbounded_ts,
            write_batch_delete_prefix_bounded_ts,
            write_batch_deletion_ts,
            write_batch_insertion_ts,
            find_keys_by_prefix_unbounded,
            find_keys_by_prefix_bounded,
            find_key_values_by_prefix_unbounded,
            find_key_values_by_prefix_bounded,
            multi_key_values: papaya::HashMap::new(),
            multi_keys: papaya::HashMap::new(),
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
        if let Some(prepared_statement) = self.multi_key_values.pin().get(&num_markers) {
            return Ok(prepared_statement.clone());
        }
        let markers = std::iter::repeat_n("?", num_markers)
            .collect::<Vec<_>>()
            .join(",");
        let prepared_statement = self
            .session
            .prepare(format!(
                "SELECT k,v FROM {}.\"{}\" WHERE root_key = ? AND k IN ({})",
                KEYSPACE, self.namespace, markers
            ))
            .await?;
        self.multi_key_values
            .pin()
            .insert(num_markers, prepared_statement.clone());
        Ok(prepared_statement)
    }

    async fn get_multi_keys_statement(
        &self,
        num_markers: usize,
    ) -> Result<PreparedStatement, ScyllaDbStoreInternalError> {
        if let Some(prepared_statement) = self.multi_keys.pin().get(&num_markers) {
            return Ok(prepared_statement.clone());
        };
        let markers = std::iter::repeat_n("?", num_markers)
            .collect::<Vec<_>>()
            .join(",");
        let prepared_statement = self
            .session
            .prepare(format!(
                "SELECT k FROM {}.\"{}\" WHERE root_key = ? AND k IN ({})",
                KEYSPACE, self.namespace, markers
            ))
            .await?;
        self.multi_keys
            .pin()
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

    /// Validates a key supplied by a caller's batch. Besides the size limit, the
    /// key must be non-empty: the empty (zero-length) key is `WRITETIME_SENTINEL_KEY`,
    /// reserved for the per-store timestamp sentinel that exclusive mode writes
    /// internally. Prefix scans now deliberately hide that key, so any caller
    /// content stored there would be silently invisible to reads.
    fn check_batch_key(key: &[u8]) -> Result<(), ScyllaDbStoreInternalError> {
        Self::check_key_size(key)?;
        ensure!(!key.is_empty(), ScyllaDbStoreInternalError::ZeroLengthKey);
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
            .await
            .map_err(ScyllaDbStoreInternalError::ExecutionError)?;
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
        let mut rows = Box::pin(self.session.execute_iter(statement, &inputs))
            .await?
            .rows_stream::<(Vec<u8>, Vec<u8>)>()?;

        while let Some(row) = rows.next().await {
            let (key, value) = row?;
            if let Some((&last, rest)) = map[&key].split_last() {
                for position in rest {
                    values[*position] = Some(value.clone());
                }
                values[last] = Some(value);
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
        let mut rows = Box::pin(self.session.execute_iter(statement, &inputs))
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
            .await
            .map_err(ScyllaDbStoreInternalError::ExecutionError)?;
        let rows = result.into_rows_result()?;
        let mut rows = rows.rows::<(Vec<u8>,)>()?;
        Ok(rows.next().is_some())
    }

    /// Reads the write-time of a single row in microseconds since Unix epoch,
    /// returning `None` if the row does not exist or carries no live value.
    async fn read_writetime_internal(
        &self,
        root_key: &[u8],
        key: Vec<u8>,
    ) -> Result<Option<i64>, ScyllaDbStoreInternalError> {
        Self::check_key_size(&key)?;
        let session = &self.session;
        let values = (root_key.to_vec(), key);
        let (result, _) = session
            .execute_single_page(&self.read_writetime, &values, PagingState::start())
            .await
            .map_err(ScyllaDbStoreInternalError::ExecutionError)?;
        let rows = result.into_rows_result()?;
        let mut rows = rows.rows::<(Option<i64>,)>()?;
        Ok(match rows.next() {
            Some(row) => row?.0,
            None => None,
        })
    }

    /// Issues an unlogged batch that contains only prefix-delete statements,
    /// letting the coordinator assign the write timestamp.
    async fn write_batch_prefix_deletes(
        &self,
        root_key: &[u8],
        key_prefix_deletions: Vec<Vec<u8>>,
    ) -> Result<(), ScyllaDbStoreInternalError> {
        if key_prefix_deletions.is_empty() {
            return Ok(());
        }
        let session = &self.session;
        let mut batch_query = scylla::statement::batch::Batch::new(BatchType::Unlogged);
        let mut batch_values = Vec::new();
        let q_unbounded = &self.write_batch_delete_prefix_unbounded;
        let q_bounded = &self.write_batch_delete_prefix_bounded;
        for key_prefix in key_prefix_deletions {
            Self::check_key_size(&key_prefix)?;
            match get_upper_bound_option(&key_prefix) {
                None => {
                    batch_values.push(vec![root_key.to_vec(), key_prefix]);
                    batch_query.append_statement(q_unbounded.clone());
                }
                Some(upper_bound) => {
                    batch_values.push(vec![root_key.to_vec(), key_prefix, upper_bound]);
                    batch_query.append_statement(q_bounded.clone());
                }
            }
        }
        session
            .batch(&batch_query, batch_values)
            .await
            .map_err(ScyllaDbStoreInternalError::WriteBatchExecutionError)?;
        Ok(())
    }

    /// Issues an unlogged batch containing the single-key deletions and the
    /// insertions, letting the coordinator assign the write timestamp.
    async fn write_simple_batch(
        &self,
        root_key: &[u8],
        batch: SimpleUnorderedBatch,
    ) -> Result<(), ScyllaDbStoreInternalError> {
        if batch.deletions.is_empty() && batch.insertions.is_empty() {
            return Ok(());
        }
        let session = &self.session;
        let mut batch_query = scylla::statement::batch::Batch::new(BatchType::Unlogged);
        let mut batch_values = Vec::new();
        let q_deletion = &self.write_batch_deletion;
        for key in batch.deletions {
            Self::check_batch_key(&key)?;
            batch_values.push(vec![root_key.to_vec(), key]);
            batch_query.append_statement(q_deletion.clone());
        }
        let q_insertion = &self.write_batch_insertion;
        for (key, value) in batch.insertions {
            Self::check_batch_key(&key)?;
            Self::check_value_size(&value)?;
            batch_values.push(vec![root_key.to_vec(), key, value]);
            batch_query.append_statement(q_insertion.clone());
        }
        session
            .batch(&batch_query, batch_values)
            .await
            .map_err(ScyllaDbStoreInternalError::WriteBatchExecutionError)?;
        Ok(())
    }

    /// Issues the whole write as a single atomic unlogged batch, used in
    /// exclusive mode. Every statement carries an explicit `USING TIMESTAMP`:
    /// the prefix-deletions use `t`, while the single-key deletions, the
    /// insertions, and the sentinel write use `t + 1`. The higher timestamp on
    /// the data ensures a range tombstone never shadows an insertion belonging
    /// to the same logical batch (at equal timestamps, dead cells win over live
    /// cells). Because the intended ordering is fixed by these timestamps rather
    /// than by send order, the prefix-deletions and the data can — and must —
    /// share one batch, preserving the atomicity that `write_batch` callers rely
    /// on. The sentinel write at `WRITETIME_SENTINEL_KEY` lets a future process
    /// recover this store's timestamp floor (see `ensure_ts_seeded`).
    async fn write_batch_exclusive(
        &self,
        root_key: &[u8],
        batch: UnorderedBatch,
        t: i64,
    ) -> Result<(), ScyllaDbStoreInternalError> {
        let UnorderedBatch {
            key_prefix_deletions,
            simple_unordered_batch:
                SimpleUnorderedBatch {
                    deletions,
                    insertions,
                },
        } = batch;
        let session = &self.session;
        let mut batch_query = scylla::statement::batch::Batch::new(BatchType::Unlogged);
        let mut batch_values = Vec::new();

        // Prefix-deletions at timestamp `t`.
        for key_prefix in key_prefix_deletions {
            Self::check_key_size(&key_prefix)?;
            match get_upper_bound_option(&key_prefix) {
                None => {
                    batch_values.push(vec![
                        CqlValue::BigInt(t),
                        CqlValue::Blob(root_key.to_vec()),
                        CqlValue::Blob(key_prefix),
                    ]);
                    batch_query
                        .append_statement(self.write_batch_delete_prefix_unbounded_ts.clone());
                }
                Some(upper_bound) => {
                    batch_values.push(vec![
                        CqlValue::BigInt(t),
                        CqlValue::Blob(root_key.to_vec()),
                        CqlValue::Blob(key_prefix),
                        CqlValue::Blob(upper_bound),
                    ]);
                    batch_query.append_statement(self.write_batch_delete_prefix_bounded_ts.clone());
                }
            }
        }

        // Single-key deletions, insertions, and the sentinel at timestamp `t + 1`.
        let t_data = t + 1;
        for key in deletions {
            Self::check_batch_key(&key)?;
            batch_values.push(vec![
                CqlValue::BigInt(t_data),
                CqlValue::Blob(root_key.to_vec()),
                CqlValue::Blob(key),
            ]);
            batch_query.append_statement(self.write_batch_deletion_ts.clone());
        }
        for (key, value) in insertions {
            Self::check_batch_key(&key)?;
            Self::check_value_size(&value)?;
            batch_values.push(vec![
                CqlValue::Blob(root_key.to_vec()),
                CqlValue::Blob(key),
                CqlValue::Blob(value),
                CqlValue::BigInt(t_data),
            ]);
            batch_query.append_statement(self.write_batch_insertion_ts.clone());
        }
        batch_values.push(vec![
            CqlValue::Blob(root_key.to_vec()),
            CqlValue::Blob(WRITETIME_SENTINEL_KEY.to_vec()),
            CqlValue::Blob(Vec::new()),
            CqlValue::BigInt(t_data),
        ]);
        batch_query.append_statement(self.write_batch_insertion_ts.clone());

        session
            .batch(&batch_query, batch_values)
            .await
            .map_err(ScyllaDbStoreInternalError::WriteBatchExecutionError)?;
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
                Box::pin(session.execute_iter(query_unbounded.clone(), values)).await?
            }
            Some(upper_bound) => {
                let values = (root_key.to_vec(), key_prefix.clone(), upper_bound);
                Box::pin(session.execute_iter(query_bounded.clone(), values)).await?
            }
        };
        let mut rows = rows.rows_stream::<(Vec<u8>,)>()?;
        let mut keys = Vec::new();
        while let Some(row) = rows.next().await {
            let (key,) = row?;
            // Skip the reserved timestamp sentinel (exclusive mode writes it at the
            // empty clustering key). It is an internal implementation detail and must
            // not surface to callers; it can only match an empty-prefix scan.
            if key == WRITETIME_SENTINEL_KEY {
                continue;
            }
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
                Box::pin(session.execute_iter(query_unbounded.clone(), values)).await?
            }
            Some(upper_bound) => {
                let values = (root_key.to_vec(), key_prefix.clone(), upper_bound);
                Box::pin(session.execute_iter(query_bounded.clone(), values)).await?
            }
        };
        let mut rows = rows.rows_stream::<(Vec<u8>, Vec<u8>)>()?;
        let mut key_values = Vec::new();
        while let Some(row) = rows.next().await {
            let (key, value) = row?;
            // Skip the reserved timestamp sentinel; see `find_keys_by_prefix_internal`.
            if key == WRITETIME_SENTINEL_KEY {
                continue;
            }
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
    /// Whether this store was opened with `open_exclusive`. When true, `write_batch`
    /// resolves in-batch prefix/insert collisions via per-statement `USING TIMESTAMP`;
    /// when false, it splits the batch into two sequential sub-batches with
    /// server-side timestamps to preserve ordering across writers.
    is_exclusive: bool,
    /// Per-partition timestamp floor for exclusive-mode `USING TIMESTAMP` writes.
    /// Value 0 means unseeded; populated lazily on first write by reading
    /// `WRITETIME` of a sentinel row. Each batch reserves 2 µs (T and T+1).
    ts_floor: Arc<AtomicI64>,
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

    /// A deserialization error
    #[error(transparent)]
    DeserializationError(#[from] DeserializationError),

    /// A row error
    #[error(transparent)]
    RowsError(#[from] RowsError),

    /// A conversion error in the accessed data
    #[error(transparent)]
    IntoRowsResultError(#[from] IntoRowsResultError),

    /// A type check error
    #[error(transparent)]
    TypeCheckError(#[from] TypeCheckError),

    /// A pager execution error
    #[error(transparent)]
    PagerExecutionError(#[from] PagerExecutionError),

    /// A prepare error
    #[error(transparent)]
    PrepareError(#[from] PrepareError),

    /// An execution error during a query (except write-batch).
    #[error(transparent)]
    ExecutionError(ExecutionError),

    /// An execution error during a write-batch operation.
    #[error(transparent)]
    WriteBatchExecutionError(ExecutionError),

    /// A session creation error
    #[error(transparent)]
    NewSessionError(#[from] NewSessionError),

    /// A next row error in ScyllaDB
    #[error(transparent)]
    NextRowError(#[from] NextRowError),

    /// Namespace contains forbidden characters
    #[error("Namespace contains forbidden characters")]
    InvalidNamespace,

    /// The key must have at most `MAX_KEY_SIZE` bytes
    #[error("The key must have at most MAX_KEY_SIZE")]
    KeyTooLong,

    /// The value must have at most `RAW_MAX_VALUE_SIZE` bytes
    #[error("The value must have at most RAW_MAX_VALUE_SIZE")]
    ValueTooLong,

    /// The batch is too long to be written
    #[error("The batch is too long to be written")]
    BatchTooLong,

    /// Keys have to be of nonzero length (the empty key is reserved for the
    /// timestamp sentinel).
    #[error("The key must be of nonzero length")]
    ZeroLengthKey,
}

impl KeyValueStoreError for ScyllaDbStoreInternalError {
    const BACKEND: &'static str = "scylla_db";

    fn must_reload_view(&self) -> bool {
        // Errors (notably timeouts) during a `write_batch` may leave the view in a
        // undetermined state where the batch may or may not have happened.
        matches!(self, Self::WriteBatchExecutionError(_))
    }
}

impl WithError for ScyllaDbStoreInternal {
    type Error = ScyllaDbStoreInternalError;
}

impl ReadableKeyValueStore for ScyllaDbStoreInternal {
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;

    fn max_stream_queries(&self) -> usize {
        self.max_stream_queries
    }

    fn root_key(&self) -> Result<Vec<u8>, ScyllaDbStoreInternalError> {
        Ok(self.root_key[1..].to_vec())
    }

    async fn read_value_bytes(
        &self,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ScyllaDbStoreInternalError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        Box::pin(store.read_value_internal(&self.root_key, key.to_vec())).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ScyllaDbStoreInternalError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        Box::pin(store.contains_key_internal(&self.root_key, key.to_vec())).await
    }

    async fn contains_keys(
        &self,
        keys: &[Vec<u8>],
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
        keys: &[Vec<u8>],
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
        Box::pin(store.find_keys_by_prefix_internal(&self.root_key, key_prefix.to_vec())).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ScyllaDbStoreInternalError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        Box::pin(store.find_key_values_by_prefix_internal(&self.root_key, key_prefix.to_vec()))
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
    //
    // We therefore order the prefix-deletions strictly before the insertions:
    //   * In exclusive mode we own the timestamps, so we issue a single atomic CQL
    //     batch with explicit per-statement `USING TIMESTAMP` (`T` for the
    //     prefix-deletions, `T + 1` for the data). See `write_batch_exclusive`.
    //   * In shared mode the coordinator owns the timestamps, so we split the write
    //     into two sequential CQL batches.
    type Batch = UnorderedBatch;

    async fn write_batch(&self, batch: Self::Batch) -> Result<(), ScyllaDbStoreInternalError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        ScyllaDbClient::check_batch_len(&batch)?;
        if self.is_exclusive {
            // A single atomic batch; ordering is pinned by the explicit timestamps.
            let t = self.next_batch_ts().await?;
            store.write_batch_exclusive(&self.root_key, batch, t).await
        } else {
            store
                .write_batch_prefix_deletes(&self.root_key, batch.key_prefix_deletions)
                .await?;
            store
                .write_simple_batch(&self.root_key, batch.simple_unordered_batch)
                .await?;
            Ok(())
        }
    }
}

impl ScyllaDbStoreInternal {
    /// Seeds the per-store timestamp floor on first write in exclusive mode.
    /// Reads `WRITETIME` of this chain's row in the reserved sentinel
    /// partition (written by every prior exclusive batch). Falls back to the
    /// current wall clock if the row does not yet exist. Idempotent — only
    /// the first caller wins the compare-exchange.
    async fn ensure_ts_seeded(&self) -> Result<(), ScyllaDbStoreInternalError> {
        if self.ts_floor.load(Ordering::Relaxed) > 0 {
            return Ok(());
        }
        let writetime = self
            .store
            .read_writetime_internal(&self.root_key, WRITETIME_SENTINEL_KEY.to_vec())
            .await?
            .unwrap_or(0);
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_micros()).ok())
            .unwrap_or(0);
        // `writetime` is the last batch's `T + 1`, i.e. the highest timestamp it
        // consumed; that is exactly what `ts_floor` tracks, so seed it directly.
        let seed = now_us.max(writetime);
        if self
            .ts_floor
            .compare_exchange(0, seed, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            // Another caller seeded first; their value wins.
        }
        Ok(())
    }

    /// Returns the base timestamp `T` for the next batch in exclusive mode.
    /// The batch may also use `T + 1`; the generator advances by 2 per call,
    /// preserving monotonicity across batches in this process.
    async fn next_batch_ts(&self) -> Result<i64, ScyllaDbStoreInternalError> {
        self.ensure_ts_seeded().await?;
        loop {
            let prev = self.ts_floor.load(Ordering::Relaxed);
            let now_us = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .and_then(|d| i64::try_from(d.as_micros()).ok())
                .unwrap_or(prev);
            let next = std::cmp::max(now_us, prev + 1);
            // The batch uses `next` (`T`) and `next + 1` (`T + 1`); store the latter
            // so the following batch starts strictly above both.
            if self
                .ts_floor
                .compare_exchange_weak(prev, next + 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(next);
            }
        }
    }
}

// ScyllaDB requires that the keys are non-empty.
fn get_big_root_key(root_key: &[u8]) -> Vec<u8> {
    let mut big_key = vec![0];
    big_key.extend(root_key);
    big_key
}

/// Reserved clustering key inside each chain's partition that holds the
/// timestamp sentinel used to seed the per-store client timestamp generator
/// in exclusive mode. The empty clustering key is unused by any caller:
/// views always write keys prefixed with a tag byte (>= `MIN_VIEW_TAG`),
/// and the journaling layer writes 6-byte keys starting with `[0, ...]`.
const WRITETIME_SENTINEL_KEY: &[u8] = &[];

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
            is_exclusive: false,
            ts_floor: Arc::new(AtomicI64::new(0)),
        })
    }

    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self::Store, ScyllaDbStoreInternalError> {
        let store = self.store.clone();
        let semaphore = self.semaphore.clone();
        let max_stream_queries = self.max_stream_queries;
        let root_key = get_big_root_key(root_key);
        Ok(ScyllaDbStoreInternal {
            store,
            semaphore,
            max_stream_queries,
            root_key,
            is_exclusive: true,
            ts_floor: Arc::new(AtomicI64::new(0)),
        })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, ScyllaDbStoreInternalError> {
        let session = ScyllaDbClient::build_default_session(&config.uri).await?;
        let statement = session
            .prepare(format!("DESCRIBE KEYSPACE {KEYSPACE}"))
            .await?;
        let result = Box::pin(session.execute_iter(statement, &[])).await;
        let miss_msg = format!("'{KEYSPACE}' not found in keyspaces");
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

    async fn list_root_keys(&self) -> Result<Vec<Vec<u8>>, ScyllaDbStoreInternalError> {
        let statement = self
            .store
            .session
            .prepare(format!(
                "SELECT root_key FROM {}.\"{}\" ALLOW FILTERING",
                KEYSPACE, self.store.namespace
            ))
            .await?;

        // Execute the query
        let rows = Box::pin(self.store.session.execute_iter(statement, &[])).await?;
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
            .prepare(format!("DROP KEYSPACE IF EXISTS {KEYSPACE}"))
            .await?;

        session
            .execute_single_page(&statement, &[], PagingState::start())
            .await
            .map_err(ScyllaDbStoreInternalError::ExecutionError)?;
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
                "SELECT root_key FROM {KEYSPACE}.\"{namespace}\" LIMIT 1 ALLOW FILTERING"
            ))
            .await;

        // The missing table translates into a very specific error that we matched
        let miss_msg1 = format!("unconfigured table {namespace}");
        let miss_msg1 = miss_msg1.as_str();
        let miss_msg2 = "Undefined name root_key in selection clause";
        let miss_msg3 = format!("Keyspace {KEYSPACE} does not exist");
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
            .await
            .map_err(ScyllaDbStoreInternalError::ExecutionError)?;

        // This explicitly sets a lot of default parameters for clarity and for making future
        // changes easier.
        let statement = session
            .prepare(format!(
                "CREATE TABLE {KEYSPACE}.\"{namespace}\" (\
                    root_key blob, \
                    k blob, \
                    v blob, \
                    PRIMARY KEY (root_key, k) \
                ) \
                WITH compaction = {{ \
                    'class'          : 'LeveledCompactionStrategy', \
                    'sstable_size_in_mb' : 160 \
                }} \
                AND compression = {{ \
                    'sstable_compression': 'LZ4Compressor', \
                    'chunk_length_in_kb':'4' \
                }} \
                AND caching = {{ \
                    'enabled': 'true' \
                }} \
                AND gc_grace_seconds = 0 \
                AND tombstone_gc = {{'mode': 'immediate'}}"
            ))
            .await?;
        session
            .execute_single_page(&statement, &[], PagingState::start())
            .await
            .map_err(ScyllaDbStoreInternalError::ExecutionError)?;
        Ok(())
    }

    async fn delete(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<(), ScyllaDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let session = ScyllaDbClient::build_default_session(&config.uri).await?;
        let statement = session
            .prepare(format!("DROP TABLE IF EXISTS {KEYSPACE}.\"{namespace}\";"))
            .await?;
        session
            .execute_single_page(&statement, &[], PagingState::start())
            .await
            .map_err(ScyllaDbStoreInternalError::ExecutionError)?;
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
    async fn new_test_config(
    ) -> Result<ScyllaDbStoreInternalConfig, JournalingError<ScyllaDbStoreInternalError>> {
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
pub type ScyllaDbStoreError = ValueSplittingError<JournalingError<ScyllaDbStoreInternalError>>;
