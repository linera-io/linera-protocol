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
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_lock::{Semaphore, SemaphoreGuard};
use dashmap::DashMap;
use futures::{
    future::{join_all, try_join_all},
    StreamExt as _,
};
use linera_base::ensure;
use scylla::{
    client::{
        execution_profile::{ExecutionProfile, ExecutionProfileHandle},
        pager::QueryPager,
        session::Session,
        session_builder::SessionBuilder,
    },
    cluster::{ClusterState, Node, NodeRef},
    deserialize::{DeserializationError, TypeCheckError},
    errors::{
        ClusterStateTokenError, DbError, ExecutionError, IntoRowsResultError, MetadataError,
        NewSessionError, NextPageError, NextRowError, PagerExecutionError, PrepareError,
        RequestAttemptError, RequestError, RowsError,
    },
    policies::{
        load_balancing::{DefaultPolicy, FallbackPlan, LoadBalancingPolicy, RoutingInfo},
        retry::DefaultRetryPolicy,
    },
    response::PagingState,
    routing::{Shard, Token},
    statement::{
        batch::{Batch, BatchType},
        prepared::PreparedStatement,
        Consistency,
    },
};
use serde::{Deserialize, Serialize};
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
    store::{AdminKeyValueStore, KeyValueStoreError, ReadableKeyValueStore, WithError},
    value_splitting::{ValueSplittingError, ValueSplittingStore},
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

/// The configuration of the ScyllaDB client.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScyllaDbClientConfig {
    /// This is the length of the prefix of the key that we'll be using as partition key in
    /// non-exclusive mode. In exclusive mode, the partition key is the root key.
    pub cluster_key_prefix_length_bytes: usize,
}

impl Default for ScyllaDbClientConfig {
    fn default() -> Self {
        Self {
            cluster_key_prefix_length_bytes: 4,
        }
    }
}

/// Map from partition_key to a map from keys to a list of their occurrences in the original vector.
type OccurrencesMap = HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<usize>>>;

/// The client for ScyllaDB:
/// * The session allows to pass queries
/// * The namespace that is being assigned to the database
/// * The prepared queries used for implementing the features of `KeyValueStore`.
struct ScyllaDbClient {
    session: Session,
    namespace: String,
    config: ScyllaDbClientConfig,
    read_value: PreparedStatement,
    contains_key: PreparedStatement,
    write_batch_delete_prefix_unbounded: PreparedStatement,
    write_batch_delete_prefix_bounded: PreparedStatement,
    write_batch_deletion: PreparedStatement,
    write_batch_insertion: PreparedStatement,
    find_keys_by_prefix_unbounded: PreparedStatement,
    find_keys_by_prefix_unbounded_full_scan: PreparedStatement,
    find_keys_by_prefix_bounded: PreparedStatement,
    find_keys_by_prefix_bounded_full_scan: PreparedStatement,
    find_key_values_by_prefix_unbounded: PreparedStatement,
    find_key_values_by_prefix_unbounded_full_scan: PreparedStatement,
    find_key_values_by_prefix_bounded: PreparedStatement,
    find_key_values_by_prefix_bounded_full_scan: PreparedStatement,
    multi_key_values: DashMap<usize, PreparedStatement>,
    multi_keys: DashMap<usize, PreparedStatement>,
}

impl ScyllaDbClient {
    async fn new(
        session: Session,
        namespace: &str,
        config: ScyllaDbClientConfig,
    ) -> Result<Self, ScyllaDbStoreInternalError> {
        let namespace = namespace.to_string();
        let read_value = session
            .prepare(format!(
                "SELECT value FROM {}.{} WHERE partition_key = ? AND cluster_key = ?",
                KEYSPACE, namespace
            ))
            .await?;

        let contains_key = session
            .prepare(format!(
                "SELECT partition_key FROM {}.{} WHERE partition_key = ? AND cluster_key = ?",
                KEYSPACE, namespace
            ))
            .await?;

        let write_batch_delete_prefix_unbounded = session
            .prepare(format!(
                "DELETE FROM {}.{} WHERE partition_key = ? AND cluster_key >= ?",
                KEYSPACE, namespace
            ))
            .await?;

        let write_batch_delete_prefix_bounded = session
            .prepare(format!(
                "DELETE FROM {}.{} WHERE partition_key = ? AND cluster_key >= ? AND cluster_key < ?",
                KEYSPACE, namespace
            ))
            .await?;

        let write_batch_deletion = session
            .prepare(format!(
                "DELETE FROM {}.{} WHERE partition_key = ? AND cluster_key = ?",
                KEYSPACE, namespace
            ))
            .await?;

        let write_batch_insertion = session
            .prepare(format!(
                "INSERT INTO {}.{} (partition_key, cluster_key, value) VALUES (?, ?, ?)",
                KEYSPACE, namespace
            ))
            .await?;

        let find_keys_by_prefix_unbounded = session
            .prepare(format!(
                "SELECT cluster_key FROM {}.{} WHERE partition_key = ? AND cluster_key >= ?",
                KEYSPACE, namespace
            ))
            .await?;

        let find_keys_by_prefix_unbounded_full_scan = session
            .prepare(format!(
                "SELECT cluster_key FROM {}.{} WHERE cluster_key >= ? ALLOW FILTERING",
                KEYSPACE, namespace
            ))
            .await?;

        let find_keys_by_prefix_bounded = session
            .prepare(format!(
                "SELECT cluster_key FROM {}.{} WHERE partition_key = ? AND cluster_key >= ? AND cluster_key < ?",
                KEYSPACE, namespace
            ))
            .await?;

        let find_keys_by_prefix_bounded_full_scan = session
            .prepare(format!(
                "SELECT cluster_key FROM {}.{} WHERE cluster_key >= ? AND cluster_key < ? ALLOW FILTERING",
                KEYSPACE, namespace
            ))
            .await?;

        let find_key_values_by_prefix_unbounded = session
            .prepare(format!(
                "SELECT cluster_key, value FROM {}.{} WHERE partition_key = ? AND cluster_key >= ?",
                KEYSPACE, namespace
            ))
            .await?;

        let find_key_values_by_prefix_unbounded_full_scan = session
            .prepare(format!(
                "SELECT cluster_key, value FROM {}.{} WHERE cluster_key >= ? ALLOW FILTERING",
                KEYSPACE, namespace
            ))
            .await?;

        let find_key_values_by_prefix_bounded = session
            .prepare(format!(
                "SELECT cluster_key, value FROM {}.{} WHERE partition_key = ? AND cluster_key >= ? AND cluster_key < ?",
                KEYSPACE, namespace
            ))
            .await?;

        let find_key_values_by_prefix_bounded_full_scan = session
            .prepare(format!(
                "SELECT cluster_key, value FROM {}.{} WHERE cluster_key >= ? AND cluster_key < ? ALLOW FILTERING",
                KEYSPACE, namespace
            ))
            .await?;

        Ok(Self {
            session,
            namespace,
            config,
            read_value,
            contains_key,
            write_batch_delete_prefix_unbounded,
            write_batch_delete_prefix_bounded,
            write_batch_deletion,
            write_batch_insertion,
            find_keys_by_prefix_unbounded,
            find_keys_by_prefix_unbounded_full_scan,
            find_keys_by_prefix_bounded,
            find_keys_by_prefix_bounded_full_scan,
            find_key_values_by_prefix_unbounded,
            find_key_values_by_prefix_unbounded_full_scan,
            find_key_values_by_prefix_bounded,
            find_key_values_by_prefix_bounded_full_scan,
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
        let session = SessionBuilder::new()
            .known_node(uri)
            .cluster_metadata_refresh_interval(Duration::from_secs(10))
            .default_execution_profile_handle(Self::build_default_execution_profile_handle(
                Self::build_default_policy(),
            ))
            .build()
            .boxed_sync()
            .await?;
        session.refresh_metadata().await?;
        Ok(session)
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
                "SELECT cluster_key, value FROM {}.{} WHERE partition_key = ? AND cluster_key IN ({})",
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
                "SELECT cluster_key FROM {}.{} WHERE partition_key = ? AND cluster_key IN ({})",
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

    fn check_batch_partition_key(
        &self,
        partition_key_prefix: &[u8],
        key: &[u8],
        batch_partition_key: &mut Option<Vec<u8>>,
    ) -> Result<(), ScyllaDbStoreInternalError> {
        let partition_key = self.get_partition_key(partition_key_prefix, key)?;
        if let Some(batch_partition_key) = batch_partition_key {
            ensure!(
                *batch_partition_key == partition_key,
                ScyllaDbStoreInternalError::MultiplePartitionKeysInBatch
            );
        } else {
            *batch_partition_key = Some(partition_key.to_vec());
        }
        Ok(())
    }

    fn check_batch_and_partition_keys(
        &self,
        partition_key_prefix: &[u8],
        exclusive_mode: bool,
        batch: &UnorderedBatch,
    ) -> Result<Vec<u8>, ScyllaDbStoreInternalError> {
        if !exclusive_mode {
            ensure!(
                batch.key_prefix_deletions.is_empty(),
                ScyllaDbStoreInternalError::PrefixDeletionsNotAllowedInNonExclusiveMode
            );
        }

        let mut batch_partition_key = None;
        for key_prefix in &batch.key_prefix_deletions {
            self.check_batch_partition_key(
                partition_key_prefix,
                key_prefix,
                &mut batch_partition_key,
            )?;
        }
        for key in &batch.simple_unordered_batch.deletions {
            self.check_batch_partition_key(partition_key_prefix, key, &mut batch_partition_key)?;
        }
        for (key, _) in &batch.simple_unordered_batch.insertions {
            self.check_batch_partition_key(partition_key_prefix, key, &mut batch_partition_key)?;
        }

        batch_partition_key.ok_or(ScyllaDbStoreInternalError::NoPartitionKeyInBatch)
    }

    async fn read_value_internal(
        &self,
        partition_key_prefix: &[u8],
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ScyllaDbStoreInternalError> {
        Self::check_key_size(&key)?;
        let session = &self.session;
        // Read the value of a key
        let values = (self.get_partition_key(partition_key_prefix, &key)?, key);

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

    fn get_partition_key(
        &self,
        partition_key_prefix: &[u8],
        key: &[u8],
    ) -> Result<Vec<u8>, ScyllaDbStoreInternalError> {
        match partition_key_prefix[0] {
            0 => Ok(partition_key_prefix.to_vec()),
            1 => {
                let mut partition_key = partition_key_prefix.to_vec();
                let range_end = key.len().min(self.config.cluster_key_prefix_length_bytes);
                let mut buf = vec![0; self.config.cluster_key_prefix_length_bytes];
                // Make sure we always return cluster_key_prefix_length_bytes bytes.
                buf[..range_end].copy_from_slice(&key[..range_end]);
                partition_key.extend(buf);
                Ok(partition_key)
            }
            _ => Err(ScyllaDbStoreInternalError::InvalidPartitionKeyPrefix),
        }
    }

    fn get_occurrences_map(
        &self,
        partition_key_prefix: &[u8],
        keys: Vec<Vec<u8>>,
    ) -> Result<OccurrencesMap, ScyllaDbStoreInternalError> {
        let mut occurrences_map: OccurrencesMap = HashMap::new();
        for (i_key, key) in keys.into_iter().enumerate() {
            Self::check_key_size(&key)?;
            let partition_key = self.get_partition_key(partition_key_prefix, &key)?;

            occurrences_map
                .entry(partition_key)
                .or_default()
                .entry(key)
                .or_default()
                .push(i_key);
        }
        Ok(occurrences_map)
    }

    async fn read_multi_values_internal(
        &self,
        partition_key_prefix: &[u8],
        exclusive_mode: bool,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ScyllaDbStoreInternalError> {
        let mut values = vec![None; keys.len()];
        let occurrences_map = self.get_occurrences_map(partition_key_prefix, keys)?;
        if exclusive_mode {
            ensure!(
                occurrences_map.len() == 1,
                ScyllaDbStoreInternalError::MultiplePartitionKeysInExclusiveMode
            );
        }
        let statements = occurrences_map
            .iter()
            .map(|(partition_key, keys_map)| async {
                let statement = self.get_multi_key_values_statement(keys_map.len()).await?;
                let mut inputs = vec![partition_key.clone()];
                inputs.extend(keys_map.keys().cloned());
                Ok::<_, ScyllaDbStoreInternalError>((partition_key.clone(), statement, inputs))
            })
            .collect::<Vec<_>>();
        let statements = try_join_all(statements).await?;

        let mut futures = Vec::new();
        let map_ref = &occurrences_map;
        for (partition_key, statement, inputs) in statements {
            futures.push(async move {
                let mut rows = self
                    .session
                    .execute_iter(statement, &inputs)
                    .await?
                    .rows_stream::<(Vec<u8>, Vec<u8>)>()?;
                let mut value_pairs = Vec::new();
                while let Some(row) = rows.next().await {
                    let (key, value) = row?;
                    for i_key in map_ref
                        .get(&partition_key)
                        .expect("partition_key is supposed to be in map")
                        .get(&key)
                        .expect("key is supposed to be in map")
                    {
                        value_pairs.push((*i_key, value.clone()));
                    }
                }

                Ok::<_, ScyllaDbStoreInternalError>(value_pairs)
            });
        }

        let values_pairs = try_join_all(futures).await?;
        for (i_key, value) in values_pairs.iter().flatten() {
            values[*i_key] = Some(value.clone());
        }

        Ok(values)
    }

    async fn contains_keys_internal(
        &self,
        partition_key_prefix: &[u8],
        exclusive_mode: bool,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<bool>, ScyllaDbStoreInternalError> {
        let mut values = vec![false; keys.len()];
        let occurrences_map = self.get_occurrences_map(partition_key_prefix, keys)?;
        if exclusive_mode {
            ensure!(
                occurrences_map.len() == 1,
                ScyllaDbStoreInternalError::MultiplePartitionKeysInExclusiveMode
            );
        }

        let statements = occurrences_map
            .iter()
            .map(|(partition_key, keys_map)| async {
                let statement = self.get_multi_keys_statement(keys_map.len()).await?;
                let mut inputs = vec![partition_key.clone()];
                inputs.extend(keys_map.keys().cloned());
                Ok::<_, ScyllaDbStoreInternalError>((partition_key.clone(), statement, inputs))
            })
            .collect::<Vec<_>>();
        let statements = try_join_all(statements).await?;

        let mut futures = Vec::new();
        let map_ref = &occurrences_map;
        for (partition_key, statement, inputs) in statements {
            futures.push(async move {
                let mut rows = self
                    .session
                    .execute_iter(statement, &inputs)
                    .await?
                    .rows_stream::<(Vec<u8>,)>()?;
                let mut keys = Vec::new();
                while let Some(row) = rows.next().await {
                    let (key,) = row?;
                    for i_key in map_ref
                        .get(&partition_key)
                        .expect("partition_key is supposed to be in map")
                        .get(&key)
                        .expect("key is supposed to be in map")
                    {
                        keys.push(*i_key);
                    }
                }

                Ok::<_, ScyllaDbStoreInternalError>(keys)
            });
        }
        let keys = try_join_all(futures).await?;

        for i_key in keys.iter().flatten() {
            values[*i_key] = true;
        }

        Ok(values)
    }

    async fn contains_key_internal(
        &self,
        partition_key_prefix: &[u8],
        key: Vec<u8>,
    ) -> Result<bool, ScyllaDbStoreInternalError> {
        Self::check_key_size(&key)?;
        let session = &self.session;
        // Read the value of a key
        let values = (self.get_partition_key(partition_key_prefix, &key)?, key);

        let (result, _) = session
            .execute_single_page(&self.contains_key, &values, PagingState::start())
            .await?;
        let rows = result.into_rows_result()?;
        let mut rows = rows.rows::<(Vec<u8>,)>()?;
        Ok(rows.next().is_some())
    }

    fn get_sticky_shard_policy_or_default(
        &self,
        partition_key: &[u8],
    ) -> Arc<dyn LoadBalancingPolicy> {
        StickyShardPolicy::new(
            &self.session,
            &self.namespace,
            partition_key,
            ScyllaDbClient::build_default_policy(),
        )
        .map(|policy| Arc::new(policy) as Arc<dyn LoadBalancingPolicy>)
        .unwrap_or_else(|_| ScyllaDbClient::build_default_policy())
    }

    // Returns a batch query with a sticky shard policy, that always tries to route to the same
    // ScyllaDB shard.
    // Should be used only on batches where all statements are to the same partition key.
    fn get_sticky_batch_query(
        &self,
        partition_key: &[u8],
    ) -> Result<Batch, ScyllaDbStoreInternalError> {
        // Since we assume this is all to the same partition key, we can use an unlogged batch.
        // We could use a logged batch to get atomicity across different partitions, but that
        // comes with a huge performance penalty (seems to double write latency).
        let mut batch_query = Batch::new(BatchType::Unlogged);
        let policy = self.get_sticky_shard_policy_or_default(partition_key);
        let handle = Self::build_default_execution_profile_handle(policy);
        batch_query.set_execution_profile_handle(Some(handle));

        Ok(batch_query)
    }

    // Batches should be always to the same partition key. Batches across different partitions
    // will return an error.
    async fn write_batch_internal(
        &self,
        partition_key_prefix: &[u8],
        exclusive_mode: bool,
        batch: UnorderedBatch,
    ) -> Result<(), ScyllaDbStoreInternalError> {
        if batch.is_empty() {
            return Ok(());
        }

        Self::check_batch_len(&batch)?;
        let partition_key =
            self.check_batch_and_partition_keys(partition_key_prefix, exclusive_mode, &batch)?;
        let session = &self.session;
        let mut batch_query = self.get_sticky_batch_query(&partition_key)?;
        let mut batch_values: Vec<Vec<Vec<u8>>> = Vec::new();

        for key_prefix in batch.key_prefix_deletions {
            // We'll be always on exclusive mode here, which check_batch_and_partition_keys
            // guarantees.
            Self::check_key_size(&key_prefix)?;
            match get_upper_bound_option(&key_prefix) {
                None => {
                    batch_query.append_statement(self.write_batch_delete_prefix_unbounded.clone());
                    batch_values.push(vec![partition_key.clone(), key_prefix]);
                }
                Some(upper_bound) => {
                    batch_query.append_statement(self.write_batch_delete_prefix_bounded.clone());
                    batch_values.push(vec![partition_key.clone(), key_prefix, upper_bound]);
                }
            }
        }

        for key in batch.simple_unordered_batch.deletions {
            Self::check_key_size(&key)?;
            batch_query.append_statement(self.write_batch_deletion.clone());
            batch_values.push(vec![partition_key.clone(), key]);
        }
        for (key, value) in batch.simple_unordered_batch.insertions {
            Self::check_key_size(&key)?;
            Self::check_value_size(&value)?;
            batch_query.append_statement(self.write_batch_insertion.clone());
            batch_values.push(vec![partition_key.clone(), key, value]);
        }

        session.batch(&batch_query, batch_values).await?;
        Ok(())
    }

    async fn get_one_column_result_from_query_pager(
        &self,
        query_pager: QueryPager,
        len: usize,
    ) -> Result<Vec<Vec<u8>>, ScyllaDbStoreInternalError> {
        let mut rows = query_pager.rows_stream::<(Vec<u8>,)>()?;
        let mut keys = Vec::new();
        while let Some(row) = rows.next().await {
            let (key,) = row?;
            let short_key = key[len..].to_vec();
            keys.push(short_key);
        }

        Ok(keys)
    }

    async fn get_two_columns_result_from_query_pager(
        &self,
        rows: QueryPager,
        len: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ScyllaDbStoreInternalError> {
        let mut rows = rows.rows_stream::<(Vec<u8>, Vec<u8>)>()?;
        let mut key_values = Vec::new();
        while let Some(row) = rows.next().await {
            let (key, value) = row?;
            let short_key = key[len..].to_vec();
            key_values.push((short_key, value));
        }

        Ok(key_values)
    }

    fn key_prefix_len_ge_partition_key_len(&self, key_prefix: &[u8]) -> bool {
        key_prefix.len() >= self.config.cluster_key_prefix_length_bytes
    }

    async fn find_keys_by_prefix_internal(
        &self,
        partition_key_prefix: &[u8],
        exclusive_mode: bool,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, ScyllaDbStoreInternalError> {
        Self::check_key_size(&key_prefix)?;
        let session = &self.session;
        // Read the value of a key
        let len = key_prefix.len();
        match get_upper_bound_option(&key_prefix) {
            None => {
                let query_pager =
                    if exclusive_mode || self.key_prefix_len_ge_partition_key_len(&key_prefix) {
                        session
                            .execute_iter(
                                self.find_keys_by_prefix_unbounded.clone(),
                                vec![
                                    self.get_partition_key(partition_key_prefix, &key_prefix)?,
                                    key_prefix,
                                ],
                            )
                            .await?
                    } else {
                        session
                            .execute_iter(
                                self.find_keys_by_prefix_unbounded_full_scan.clone(),
                                vec![key_prefix],
                            )
                            .await?
                    };

                self.get_one_column_result_from_query_pager(query_pager, len)
                    .await
            }
            Some(upper_bound) => {
                let query_pager =
                    if exclusive_mode || self.key_prefix_len_ge_partition_key_len(&key_prefix) {
                        session
                            .execute_iter(
                                self.find_keys_by_prefix_bounded.clone(),
                                vec![
                                    self.get_partition_key(partition_key_prefix, &key_prefix)?,
                                    key_prefix,
                                    upper_bound,
                                ],
                            )
                            .await?
                    } else {
                        session
                            .execute_iter(
                                self.find_keys_by_prefix_bounded_full_scan.clone(),
                                vec![key_prefix, upper_bound],
                            )
                            .await?
                    };

                self.get_one_column_result_from_query_pager(query_pager, len)
                    .await
            }
        }
    }

    async fn find_key_values_by_prefix_internal(
        &self,
        partition_key_prefix: &[u8],
        exclusive_mode: bool,
        key_prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ScyllaDbStoreInternalError> {
        Self::check_key_size(&key_prefix)?;
        let session = &self.session;
        let len = key_prefix.len();
        match get_upper_bound_option(&key_prefix) {
            None => {
                let partition_key = self.get_partition_key(partition_key_prefix, &key_prefix)?;
                let query_pager =
                    if exclusive_mode || self.key_prefix_len_ge_partition_key_len(&key_prefix) {
                        session
                            .execute_iter(
                                self.find_key_values_by_prefix_unbounded.clone(),
                                vec![partition_key, key_prefix],
                            )
                            .await?
                    } else {
                        session
                            .execute_iter(
                                self.find_key_values_by_prefix_unbounded_full_scan.clone(),
                                vec![key_prefix],
                            )
                            .await?
                    };
                self.get_two_columns_result_from_query_pager(query_pager, len)
                    .await
            }
            Some(upper_bound) => {
                let partition_key = self.get_partition_key(partition_key_prefix, &key_prefix)?;
                let query_pager =
                    if exclusive_mode || self.key_prefix_len_ge_partition_key_len(&key_prefix) {
                        session
                            .execute_iter(
                                self.find_key_values_by_prefix_bounded.clone(),
                                vec![partition_key, key_prefix, upper_bound],
                            )
                            .await?
                    } else {
                        session
                            .execute_iter(
                                self.find_key_values_by_prefix_bounded_full_scan.clone(),
                                vec![key_prefix, upper_bound],
                            )
                            .await?
                    };
                self.get_two_columns_result_from_query_pager(query_pager, len)
                    .await
            }
        }
    }
}

// Batch statements in ScyllaDb are currently not token aware. The batch gets sent to a random
// node: https://rust-driver.docs.scylladb.com/stable/statements/batch.html#performance
// However, for batches where all statements are to the same partition key, we can use a sticky
// shard policy to route to the same shard, and make batches be token aware.
//
// This is a policy that always tries to route to the ScyllaDB shards that contain the token, in a
// round-robin fashion.
#[derive(Debug)]
struct StickyShardPolicy {
    replicas: Vec<(Arc<Node>, Shard)>,
    current_replica_index: AtomicUsize,
    fallback: Arc<dyn LoadBalancingPolicy>,
}

impl StickyShardPolicy {
    fn new(
        session: &Session,
        namespace: &str,
        partition_key: &[u8],
        fallback: Arc<dyn LoadBalancingPolicy>,
    ) -> Result<Self, ScyllaDbStoreInternalError> {
        let cluster = session.get_cluster_state();
        let token = cluster.compute_token(KEYSPACE, namespace, &(partition_key,))?;
        let replicas = cluster.get_token_endpoints(KEYSPACE, namespace, token);
        Ok(Self {
            replicas,
            current_replica_index: AtomicUsize::new(0),
            fallback,
        })
    }
}

impl LoadBalancingPolicy for StickyShardPolicy {
    fn name(&self) -> String {
        "StickyShardPolicy".to_string()
    }

    // Always try first to route to the sticky shard.
    fn pick<'a>(
        &'a self,
        request: &'a RoutingInfo<'a>,
        cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        if self.replicas.is_empty() {
            return self.fallback.pick(request, cluster);
        }
        // fetch_add will wrap around on overflow, so we should be ok just incrementing forever here.
        let new_replica_index =
            self.current_replica_index.fetch_add(1, Ordering::Relaxed) % self.replicas.len();
        let (node, shard) = &self.replicas[new_replica_index];
        Some((node, Some(*shard)))
    }

    // Fallback to the default policy.
    fn fallback<'a>(
        &'a self,
        request: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> FallbackPlan<'a> {
        self.fallback.fallback(request, cluster)
    }
}

/// The client itself and the keeping of the count of active connections.
#[derive(Clone)]
pub struct ScyllaDbStoreInternal {
    store: Arc<ScyllaDbClient>,
    semaphore: Option<Arc<Semaphore>>,
    max_stream_queries: usize,
    partition_key_prefix: Vec<u8>,
    exclusive_mode: bool,
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

    /// A token error in ScyllaDB
    #[error(transparent)]
    ClusterStateTokenError(#[from] ClusterStateTokenError),

    /// The token endpoint information is currently missing from the driver
    #[error("The token endpoint information is currently missing from the driver")]
    MissingTokenEndpoints(Token),

    /// The mutex is poisoned
    #[error("The mutex is poisoned")]
    PoisonedMutex,

    /// A metadata error in ScyllaDB
    #[error(transparent)]
    MetadataError(#[from] MetadataError),

    /// The partition key prefix is invalid
    #[error("The partition key prefix is invalid")]
    InvalidPartitionKeyPrefix,

    /// The batch contains multiple partition keys
    #[error("Multiple partition keys in batch is not allowed")]
    MultiplePartitionKeysInBatch,

    /// The batch contains no partition key
    #[error("The batch contains no partition key. Every batch must contain a partition key")]
    NoPartitionKeyInBatch,

    /// Prefix deletions are not allowed in non-exclusive mode
    #[error("Prefix deletions are not allowed in non-exclusive mode")]
    PrefixDeletionsNotAllowedInNonExclusiveMode,

    /// Multiple partition keys in a query are not allowed in exclusive mode
    #[error("Multiple partition keys in a query are not allowed in exclusive mode")]
    MultiplePartitionKeysInExclusiveMode,
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
            .read_value_internal(&self.partition_key_prefix, key.to_vec())
            .await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ScyllaDbStoreInternalError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store
            .contains_key_internal(&self.partition_key_prefix, key.to_vec())
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
        let handles = keys.chunks(MAX_MULTI_KEYS).map(|keys| {
            store.contains_keys_internal(
                &self.partition_key_prefix,
                self.exclusive_mode,
                keys.to_vec(),
            )
        });
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
        let handles = keys.chunks(MAX_MULTI_KEYS).map(|keys| {
            store.read_multi_values_internal(
                &self.partition_key_prefix,
                self.exclusive_mode,
                keys.to_vec(),
            )
        });
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
            .find_keys_by_prefix_internal(
                &self.partition_key_prefix,
                self.exclusive_mode,
                key_prefix.to_vec(),
            )
            .await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ScyllaDbStoreInternalError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store
            .find_key_values_by_prefix_internal(
                &self.partition_key_prefix,
                self.exclusive_mode,
                key_prefix.to_vec(),
            )
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

    // Batches should be always to the same partition key. Batches across different partitions
    // will not be atomic. If the caller wants atomicity, it's the caller's responsibility to
    // make sure that the batch only has statements to the same partition key.
    async fn write_batch(&self, batch: Self::Batch) -> Result<(), ScyllaDbStoreInternalError> {
        let store = self.store.deref();
        let _guard = self.acquire().await;
        store
            .write_batch_internal(&self.partition_key_prefix, self.exclusive_mode, batch)
            .await
    }
}

fn get_exclusive_partition_key(root_key: &[u8]) -> Vec<u8> {
    // This is for views (mutable data). This is the final partition key.
    let mut partition_key = vec![0];
    partition_key.extend(root_key);
    partition_key
}

fn get_non_exclusive_partition_key_prefix() -> Vec<u8> {
    // This is for immutable data. A prefix of the key will be added to this to make the
    // partition key.
    vec![1]
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
    /// The configuration of the ScyllaDB client
    pub client_config: ScyllaDbClientConfig,
}

impl AdminKeyValueStore for ScyllaDbStoreInternal {
    type Config = ScyllaDbStoreInternalConfig;

    fn get_name() -> String {
        "scylladb internal".to_string()
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<Self, ScyllaDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let session = ScyllaDbClient::build_default_session(&config.uri).await?;
        let store = ScyllaDbClient::new(session, namespace, config.client_config.clone()).await?;
        let store = Arc::new(store);
        let semaphore = config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = config.max_stream_queries;
        let partition_key_prefix = get_non_exclusive_partition_key_prefix();
        Ok(Self {
            store,
            semaphore,
            max_stream_queries,
            partition_key_prefix,
            exclusive_mode: false,
        })
    }

    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self, ScyllaDbStoreInternalError> {
        let store = self.store.clone();
        let semaphore = self.semaphore.clone();
        let max_stream_queries = self.max_stream_queries;
        let partition_key_prefix = get_exclusive_partition_key(root_key);
        Ok(Self {
            store,
            semaphore,
            max_stream_queries,
            partition_key_prefix,
            exclusive_mode: true,
        })
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
                "SELECT partition_key FROM {}.{} ALLOW FILTERING",
                KEYSPACE, namespace
            ))
            .await?;

        // Execute the query
        let rows = session.execute_iter(statement, &[]).await?;
        let mut rows = rows.rows_stream::<(Vec<u8>,)>()?;
        let mut root_keys = BTreeSet::new();
        while let Some(row) = rows.next().await {
            let (partition_key,) = row?;
            if partition_key[0] == 0 {
                let root_key = partition_key[1..].to_vec();
                root_keys.insert(root_key);
            }
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
                "SELECT partition_key FROM {}.{} LIMIT 1 ALLOW FILTERING",
                KEYSPACE, namespace
            ))
            .await;

        // The missing table translates into a very specific error that we matched
        let miss_msg1 = format!("unconfigured table {}", namespace);
        let miss_msg1 = miss_msg1.as_str();
        let miss_msg2 = "Undefined name partition_key in selection clause";
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
                    partition_key blob, \
                    cluster_key blob, \
                    value blob, \
                    PRIMARY KEY (partition_key, cluster_key) \
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
impl TestKeyValueStore for JournalingKeyValueStore<ScyllaDbStoreInternal> {
    async fn new_test_config() -> Result<ScyllaDbStoreInternalConfig, ScyllaDbStoreInternalError> {
        // TODO(#4114): Read the port from an environment variable.
        let uri = "localhost:9042".to_string();
        Ok(ScyllaDbStoreInternalConfig {
            uri,
            max_concurrent_queries: Some(10),
            max_stream_queries: 10,
            replication_factor: 1,
            client_config: ScyllaDbClientConfig::default(),
        })
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

/// The combined error type for the `ScyllaDbStore`.
pub type ScyllaDbStoreError = ValueSplittingError<ScyllaDbStoreInternalError>;
