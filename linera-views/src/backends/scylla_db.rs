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
use futures::{
    future::{join_all, try_join_all, BoxFuture},
    FutureExt as _, StreamExt,
};
use linera_base::ensure;
use scylla::{
    client::{
        execution_profile::ExecutionProfile, pager::QueryPager, session::Session,
        session_builder::SessionBuilder,
    },
    deserialize::{DeserializationError, TypeCheckError},
    errors::{
        DbError, ExecutionError, IntoRowsResultError, NewSessionError, NextPageError, NextRowError,
        PagerExecutionError, PrepareError, RequestAttemptError, RequestError, RowsError,
    },
    frame::Compression,
    policies::{load_balancing::DefaultPolicy, retry::DefaultRetryPolicy},
    response::PagingState,
    statement::{prepared::PreparedStatement, Consistency},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64;

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

/// This is the length of the prefix of the key that we'll be hashing to determine the partition
/// bucket.
const K_PREFIX_LENGTH_BYTES: usize = 3;

/// The number of bucket partitions in the table.
const NUM_BUCKETS: u16 = 1 << 10;

/// Map from bucket_id to a map from keys to a list of their occurrences in the original vector.
type OccurrencesMap = HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<usize>>>;

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
    async fn get_multi_key_values_statement(
        &self,
        num_markers: usize,
    ) -> Result<PreparedStatement, ScyllaDbStoreInternalError> {
        if let Some(entry) = self.multi_key_values.get(&num_markers) {
            return Ok(entry.clone());
        }
        let markers = std::iter::repeat_n("?", num_markers)
            .collect::<Vec<_>>()
            .join(",");
        let query = format!(
            "SELECT k,v FROM kv.{} WHERE root_key = ? AND bucket_id = ? AND k IN ({})",
            self.namespace, markers
        );
        let prepared_statement = self.session.prepare(query).await?;
        self.multi_key_values
            .insert(num_markers, prepared_statement.clone());
        Ok(prepared_statement)
    }

    async fn get_multi_keys_statement(
        &self,
        num_markers: usize,
    ) -> Result<PreparedStatement, ScyllaDbStoreInternalError> {
        if let Some(entry) = self.multi_keys.get(&num_markers) {
            return Ok(entry.clone());
        }
        let markers = std::iter::repeat_n("?", num_markers)
            .collect::<Vec<_>>()
            .join(",");
        let query = format!(
            "SELECT k FROM kv.{} WHERE root_key = ? AND bucket_id = ? AND k IN ({})",
            self.namespace, markers
        );
        let prepared_statement = self.session.prepare(query).await?;
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

    async fn new(session: Session, namespace: &str) -> Result<Self, ScyllaDbStoreInternalError> {
        let namespace = namespace.to_string();
        let read_value = session
            .prepare(format!(
                "SELECT v FROM kv.{} WHERE root_key = ? AND bucket_id = ? AND k = ?",
                namespace
            ))
            .await?;

        let contains_key = session
            .prepare(format!(
                "SELECT root_key FROM kv.{} WHERE root_key = ? AND bucket_id = ? AND k = ?",
                namespace
            ))
            .await?;

        let write_batch_delete_prefix_unbounded = session
            .prepare(format!(
                "DELETE FROM kv.{} WHERE root_key = ? AND bucket_id = ? AND k >= ?",
                namespace
            ))
            .await?;

        let write_batch_delete_prefix_bounded = session
            .prepare(format!(
                "DELETE FROM kv.{} WHERE root_key = ? AND bucket_id = ? AND k >= ? AND k < ?",
                namespace
            ))
            .await?;

        let write_batch_deletion = session
            .prepare(format!(
                "DELETE FROM kv.{} WHERE root_key = ? AND bucket_id = ? AND k = ?",
                namespace
            ))
            .await?;

        let write_batch_insertion = session
            .prepare(format!(
                "INSERT INTO kv.{} (root_key, bucket_id, k, v) VALUES (?, ?, ?, ?)",
                namespace
            ))
            .await?;

        let find_keys_by_prefix_unbounded = session
            .prepare(format!(
                "SELECT k FROM kv.{} WHERE root_key = ? AND bucket_id = ? AND k >= ?",
                namespace
            ))
            .await?;

        let find_keys_by_prefix_bounded = session
            .prepare(format!(
                "SELECT k FROM kv.{} WHERE root_key = ? AND bucket_id = ? AND k >= ? AND k < ?",
                namespace
            ))
            .await?;

        let find_key_values_by_prefix_unbounded = session
            .prepare(format!(
                "SELECT k,v FROM kv.{} WHERE root_key = ? AND bucket_id = ? AND k >= ?",
                namespace
            ))
            .await?;

        let find_key_values_by_prefix_bounded = session
            .prepare(format!(
                "SELECT k,v FROM kv.{} WHERE root_key = ? AND bucket_id = ? AND k >= ? AND k < ?",
                namespace
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

    async fn read_value_internal(
        &self,
        root_key: &[u8],
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ScyllaDbStoreInternalError> {
        Self::check_key_size(&key)?;
        let session = &self.session;
        // Read the value of a key
        let values = (root_key.to_vec(), Self::get_bucket_id(root_key, &key), key);

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

    fn get_bucket_id(root_key: &[u8], key: &[u8]) -> Vec<u8> {
        if root_key.len() > 1 {
            // A root key longer than 1 means that we used `clone_with_root_key` and we don't need
            // to add extra partitioning. So return the same bucket id always.
            vec![0, 0]
        } else {
            let range_end = key.len().min(K_PREFIX_LENGTH_BYTES);
            let bucket_id = (xxh3_64(&key[..range_end]) as u16) & (NUM_BUCKETS - 1);
            bucket_id.to_be_bytes().to_vec()
        }
    }

    fn get_occurences_map(
        root_key: &[u8],
        keys: Vec<Vec<u8>>,
    ) -> Result<OccurrencesMap, ScyllaDbStoreInternalError> {
        let mut map: OccurrencesMap = HashMap::new();
        for (i_key, key) in keys.into_iter().enumerate() {
            Self::check_key_size(&key)?;
            let bucket_id = Self::get_bucket_id(root_key, &key);

            map.entry(bucket_id)
                .or_default()
                .entry(key)
                .or_default()
                .push(i_key);
        }
        Ok(map)
    }

    async fn read_multi_values_internal(
        &self,
        root_key: &[u8],
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ScyllaDbStoreInternalError> {
        let mut values = vec![None; keys.len()];
        let map = Self::get_occurences_map(root_key, keys)?;
        let statements = map
            .iter()
            .map(|(bucket_id, keys_map)| async {
                let statement = self.get_multi_key_values_statement(keys_map.len()).await?;
                let mut inputs = vec![root_key.to_vec(), bucket_id.clone()];
                inputs.extend(keys_map.keys().cloned());
                Ok::<_, ScyllaDbStoreInternalError>((bucket_id.clone(), statement, inputs))
            })
            .collect::<Vec<_>>();
        let statements = try_join_all(statements).await?;

        let mut futures = Vec::new();
        let map_ref = &map;
        for (bucket_id, statement, inputs) in statements {
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
                        .get(&bucket_id)
                        .expect("bucket_id is supposed to be in map")
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
        root_key: &[u8],
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<bool>, ScyllaDbStoreInternalError> {
        let mut values = vec![false; keys.len()];
        let map = Self::get_occurences_map(root_key, keys)?;
        let statements = map
            .iter()
            .map(|(bucket_id, keys_map)| async {
                let statement = self.get_multi_keys_statement(keys_map.len()).await?;
                let mut inputs = vec![root_key.to_vec(), bucket_id.clone()];
                inputs.extend(keys_map.keys().cloned());
                Ok::<_, ScyllaDbStoreInternalError>((bucket_id.clone(), statement, inputs))
            })
            .collect::<Vec<_>>();
        let statements = try_join_all(statements).await?;

        let mut futures = Vec::new();
        let map_ref = &map;
        for (bucket_id, statement, inputs) in statements {
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
                        .get(&bucket_id)
                        .expect("bucket_id is supposed to be in map")
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
        root_key: &[u8],
        key: Vec<u8>,
    ) -> Result<bool, ScyllaDbStoreInternalError> {
        Self::check_key_size(&key)?;
        let session = &self.session;
        // Read the value of a key
        let values = (root_key.to_vec(), Self::get_bucket_id(root_key, &key), key);

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
        Self::check_batch_len(&batch)?;
        let session = &self.session;
        let root_key = root_key.to_vec();

        let mut futures: Vec<BoxFuture<'_, Result<(), ScyllaDbStoreInternalError>>> = Vec::new();
        for key_prefix in batch.key_prefix_deletions {
            Self::check_key_size(&key_prefix)?;
            match get_upper_bound_option(&key_prefix) {
                None => {
                    let values = if key_prefix.len() > K_PREFIX_LENGTH_BYTES {
                        vec![(
                            root_key.clone(),
                            Self::get_bucket_id(&root_key, &key_prefix),
                            key_prefix,
                        )]
                    } else {
                        // If the key prefix is shorter than K_PREFIX_LENGTH_BYTES, we need to
                        // go through all buckets.
                        let mut inner_values = Vec::new();
                        for bucket_id in 0..NUM_BUCKETS {
                            inner_values.push((
                                root_key.clone(),
                                bucket_id.to_be_bytes().to_vec(),
                                key_prefix.clone(),
                            ));
                        }
                        inner_values
                    };

                    futures.extend(values.into_iter().map(|values| {
                        async move {
                            session
                                .execute_single_page(
                                    &self.write_batch_delete_prefix_unbounded,
                                    values,
                                    PagingState::start(),
                                )
                                .await
                                .map(|_| ())
                                .map_err(Into::into)
                        }
                        .boxed()
                    }));
                }
                Some(upper) => {
                    let values = if key_prefix.len() > K_PREFIX_LENGTH_BYTES {
                        vec![(
                            root_key.clone(),
                            Self::get_bucket_id(&root_key, &key_prefix),
                            key_prefix,
                            upper,
                        )]
                    } else {
                        // If the key prefix is shorter than K_PREFIX_LENGTH_BYTES, we need to
                        // go through all buckets.
                        let mut inner_values = Vec::new();
                        for bucket_id in 0..NUM_BUCKETS {
                            inner_values.push((
                                root_key.clone(),
                                bucket_id.to_be_bytes().to_vec(),
                                key_prefix.clone(),
                                upper.clone(),
                            ));
                        }
                        inner_values
                    };

                    futures.extend(values.into_iter().map(|values| {
                        async move {
                            session
                                .execute_single_page(
                                    &self.write_batch_delete_prefix_bounded,
                                    values,
                                    PagingState::start(),
                                )
                                .await
                                .map(|_| ())
                                .map_err(Into::into)
                        }
                        .boxed()
                    }));
                }
            }
        }
        try_join_all(futures).await?;

        let mut futures: Vec<BoxFuture<'_, Result<(), ScyllaDbStoreInternalError>>> = Vec::new();
        for key in batch.simple_unordered_batch.deletions {
            Self::check_key_size(&key)?;
            let prepared_statement = &self.write_batch_deletion;
            let values = (root_key.clone(), Self::get_bucket_id(&root_key, &key), key);
            futures.push(Box::pin(async move {
                session
                    .execute_single_page(prepared_statement, values, PagingState::start())
                    .await
                    .map(|_| ())
                    .map_err(Into::into)
            }));
        }

        for (key, value) in batch.simple_unordered_batch.insertions {
            Self::check_key_size(&key)?;
            Self::check_value_size(&value)?;
            let prepared_statement = &self.write_batch_insertion;
            let values = (
                root_key.clone(),
                Self::get_bucket_id(&root_key, &key),
                key,
                value,
            );
            futures.push(Box::pin(async move {
                session
                    .execute_single_page(prepared_statement, values, PagingState::start())
                    .await
                    .map(|_| ())
                    .map_err(Into::into)
            }));
        }

        try_join_all(futures).await?;
        Ok(())
    }

    async fn get_keys_from_query_pager(
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

    async fn get_key_values_from_query_pager(
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

    async fn get_flattened_ordered_keys<T: Ord + Clone>(
        keys: Vec<Vec<T>>,
        returned_keys_len: usize,
    ) -> Result<Vec<T>, ScyllaDbStoreInternalError> {
        let mut flattened_keys = Vec::with_capacity(returned_keys_len);
        let mut idxs = vec![0; keys.len()];
        for _ in 0..returned_keys_len {
            let mut smallest_idx = 0;
            let mut smallest_key: Option<T> = None;
            for (j, inner_keys) in keys.iter().enumerate() {
                if idxs[j] >= inner_keys.len() {
                    continue;
                }

                if smallest_key.is_none()
                    || inner_keys[idxs[j]]
                        < smallest_key.clone().expect("Should have a smallest key")
                {
                    smallest_idx = j;
                    smallest_key = Some(inner_keys[idxs[j]].clone());
                }
            }

            flattened_keys.push(smallest_key.expect("Should have a smallest key"));
            idxs[smallest_idx] += 1;
        }
        Ok(flattened_keys)
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
        let returned_keys_len;
        let keys = match get_upper_bound_option(&key_prefix) {
            None => {
                if key_prefix.len() > K_PREFIX_LENGTH_BYTES {
                    let values = (
                        root_key.to_vec(),
                        Self::get_bucket_id(root_key, &key_prefix),
                        key_prefix.clone(),
                    );
                    let query_pager = session
                        .execute_iter(self.find_keys_by_prefix_unbounded.clone(), values)
                        .await?;
                    let inner_keys = self.get_keys_from_query_pager(query_pager, len).await?;
                    returned_keys_len = inner_keys.len();
                    vec![inner_keys]
                } else {
                    let mut futures = Vec::new();
                    for bucket_id in 0..NUM_BUCKETS {
                        let values = (
                            root_key.to_vec(),
                            bucket_id.to_be_bytes().to_vec(),
                            key_prefix.clone(),
                        );
                        futures.push(Box::pin(async move {
                            let query_pager = session
                                .execute_iter(self.find_keys_by_prefix_unbounded.clone(), values)
                                .await?;
                            let inner_keys =
                                self.get_keys_from_query_pager(query_pager, len).await?;
                            let inner_keys_len = inner_keys.len();
                            Ok::<(Vec<Vec<u8>>, usize), ScyllaDbStoreInternalError>((
                                inner_keys,
                                inner_keys_len,
                            ))
                        }));
                    }
                    let results = try_join_all(futures).await?;
                    returned_keys_len = results.iter().map(|(_, len)| len).sum();
                    let keys: Vec<Vec<Vec<u8>>> =
                        results.into_iter().map(|(keys, _)| keys).collect();
                    keys
                }
            }
            Some(upper_bound) => {
                if key_prefix.len() > K_PREFIX_LENGTH_BYTES {
                    let values = (
                        root_key.to_vec(),
                        Self::get_bucket_id(root_key, &key_prefix),
                        key_prefix.clone(),
                        upper_bound,
                    );
                    let query_pager = session
                        .execute_iter(self.find_keys_by_prefix_bounded.clone(), values)
                        .await?;
                    let inner_keys = self.get_keys_from_query_pager(query_pager, len).await?;
                    returned_keys_len = inner_keys.len();
                    vec![inner_keys]
                } else {
                    let mut futures = Vec::new();
                    for bucket_id in 0..NUM_BUCKETS {
                        let values = (
                            root_key.to_vec(),
                            bucket_id.to_be_bytes().to_vec(),
                            key_prefix.clone(),
                            upper_bound.clone(),
                        );
                        futures.push(Box::pin(async move {
                            let query_pager = session
                                .execute_iter(self.find_keys_by_prefix_bounded.clone(), values)
                                .await?;
                            let inner_keys =
                                self.get_keys_from_query_pager(query_pager, len).await?;
                            let inner_keys_len = inner_keys.len();
                            Ok::<(Vec<Vec<u8>>, usize), ScyllaDbStoreInternalError>((
                                inner_keys,
                                inner_keys_len,
                            ))
                        }));
                    }
                    let results = try_join_all(futures).await?;
                    returned_keys_len = results.iter().map(|(_, len)| len).sum();
                    let keys: Vec<Vec<Vec<u8>>> =
                        results.into_iter().map(|(keys, _)| keys).collect();
                    keys
                }
            }
        };
        Self::get_flattened_ordered_keys(keys, returned_keys_len).await
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
        let returned_keys_len;
        let key_values =
            match get_upper_bound_option(&key_prefix) {
                None => {
                    if key_prefix.len() > K_PREFIX_LENGTH_BYTES {
                        let values = (
                            root_key.to_vec(),
                            Self::get_bucket_id(root_key, &key_prefix),
                            key_prefix.clone(),
                        );
                        let query_pager = session
                            .execute_iter(self.find_key_values_by_prefix_unbounded.clone(), values)
                            .await?;
                        let inner_keys = self
                            .get_key_values_from_query_pager(query_pager, len)
                            .await?;
                        returned_keys_len = inner_keys.len();
                        vec![inner_keys]
                    } else {
                        let mut futures = Vec::new();
                        for bucket_id in 0..NUM_BUCKETS {
                            let values = (
                                root_key.to_vec(),
                                bucket_id.to_be_bytes().to_vec(),
                                key_prefix.clone(),
                            );
                            futures.push(Box::pin(async move {
                                let query_pager = session
                                    .execute_iter(
                                        self.find_key_values_by_prefix_unbounded.clone(),
                                        values,
                                    )
                                    .await?;
                                let inner_keys = self
                                    .get_key_values_from_query_pager(query_pager, len)
                                    .await?;
                                let inner_keys_len = inner_keys.len();
                                Ok::<(Vec<(Vec<u8>, Vec<u8>)>, usize), ScyllaDbStoreInternalError>(
                                    (inner_keys, inner_keys_len),
                                )
                            }));
                        }
                        let results = try_join_all(futures).await?;
                        returned_keys_len = results.iter().map(|(_, len)| len).sum();
                        let keys: Vec<Vec<(Vec<u8>, Vec<u8>)>> =
                            results.into_iter().map(|(keys, _)| keys).collect();
                        keys
                    }
                }
                Some(upper_bound) => {
                    if key_prefix.len() > K_PREFIX_LENGTH_BYTES {
                        let values = (
                            root_key.to_vec(),
                            Self::get_bucket_id(root_key, &key_prefix),
                            key_prefix.clone(),
                            upper_bound,
                        );
                        let query_pager = session
                            .execute_iter(self.find_key_values_by_prefix_bounded.clone(), values)
                            .await?;
                        let inner_keys = self
                            .get_key_values_from_query_pager(query_pager, len)
                            .await?;
                        returned_keys_len = inner_keys.len();
                        vec![inner_keys]
                    } else {
                        let mut futures = Vec::new();
                        for bucket_id in 0..NUM_BUCKETS {
                            let values = (
                                root_key.to_vec(),
                                bucket_id.to_be_bytes().to_vec(),
                                key_prefix.clone(),
                                upper_bound.clone(),
                            );
                            futures.push(Box::pin(async move {
                                let query_pager = session
                                    .execute_iter(
                                        self.find_key_values_by_prefix_bounded.clone(),
                                        values,
                                    )
                                    .await?;
                                let inner_keys = self
                                    .get_key_values_from_query_pager(query_pager, len)
                                    .await?;
                                let inner_keys_len = inner_keys.len();
                                Ok::<(Vec<(Vec<u8>, Vec<u8>)>, usize), ScyllaDbStoreInternalError>(
                                    (inner_keys, inner_keys_len),
                                )
                            }));
                        }
                        let results = try_join_all(futures).await?;
                        returned_keys_len = results.iter().map(|(_, len)| len).sum();
                        let keys: Vec<Vec<(Vec<u8>, Vec<u8>)>> =
                            results.into_iter().map(|(keys, _)| keys).collect();
                        keys
                    }
                }
            };
        Self::get_flattened_ordered_keys(key_values, returned_keys_len).await
    }
}

async fn build_session(uri: &str) -> Result<Session, ScyllaDbStoreInternalError> {
    let policy = DefaultPolicy::builder().token_aware(true).build();
    let default_profile = ExecutionProfile::builder()
        .load_balancing_policy(policy)
        .retry_policy(Arc::new(DefaultRetryPolicy::new()))
        .consistency(Consistency::LocalQuorum)
        .build();
    let handle = default_profile.into_handle();
    SessionBuilder::new()
        .known_node(uri)
        .default_execution_profile_handle(handle)
        .compression(Some(Compression::Lz4))
        .build()
        .boxed()
        .await
        .map_err(Into::into)
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
    ) -> Result<Self, ScyllaDbStoreInternalError> {
        Self::check_namespace(namespace)?;
        let session = build_session(&config.uri).await?;
        let store = ScyllaDbClient::new(session, namespace).await?;
        let store = Arc::new(store);
        let semaphore = config
            .common_config
            .max_concurrent_queries
            .map(|n| Arc::new(Semaphore::new(n)));
        let max_stream_queries = config.common_config.max_stream_queries;
        let root_key = get_big_root_key(&[]);
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
        let session = build_session(&config.uri).await?;
        let statement = session.prepare("DESCRIBE KEYSPACE kv").await?;
        let result = session.execute_iter(statement, &[]).await;
        let miss_msg = "'kv' not found in keyspaces";
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
        let session = build_session(&config.uri).await?;
        let statement = session
            .prepare(format!(
                "SELECT root_key FROM kv.{} ALLOW FILTERING",
                namespace
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
        let session = build_session(&store_config.uri).await?;
        let statement = session.prepare("DROP KEYSPACE IF EXISTS kv;").await?;

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
        let session = build_session(&config.uri).await?;

        // We check the way the test can fail. It can fail in different ways.
        let result = session
            .prepare(format!(
                "SELECT root_key FROM kv.{} LIMIT 1 ALLOW FILTERING",
                namespace
            ))
            .await;

        // The missing table translates into a very specific error that we matched
        let miss_msg1 = format!("unconfigured table {}", namespace);
        let miss_msg1 = miss_msg1.as_str();
        let miss_msg2 = "Undefined name root_key in selection clause";
        let miss_msg3 = "Keyspace kv does not exist";
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
        let session = build_session(&config.uri).await?;

        // Create a keyspace if it doesn't exist
        let statement = session
            .prepare(format!(
                "CREATE KEYSPACE IF NOT EXISTS kv WITH REPLICATION = {{ \
                'class' : 'NetworkTopologyStrategy', \
                'replication_factor' : {} \
            }}",
                config.common_config.replication_factor
            ))
            .await?;
        session
            .execute_single_page(&statement, &[], PagingState::start())
            .await?;

        let statement = session
            .prepare(format!(
                "CREATE TABLE kv.{} (\
                root_key blob, \
                bucket_id blob, \
                k blob, \
                v blob, \
                PRIMARY KEY ((root_key, bucket_id), k) \
            ) \
            WITH compaction = {{ \
                'class'            : 'SizeTieredCompactionStrategy', \
                'min_sstable_size' : 268435456, \
                'bucket_low'       : 0.5, \
                'bucket_high'      : 1.5, \
                'min_threshold'    : 4, \
                'max_threshold'    : 32 \
            }} \
            AND compression = {{ 'sstable_compression': 'LZ4Compressor', 'chunk_length_kb':'8' }} \
            AND caching = {{ 'enabled': 'true' }}",
                namespace
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
        let session = build_session(&config.uri).await?;
        let statement = session
            .prepare(format!("DROP TABLE IF EXISTS kv.{};", namespace))
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
            replication_factor: 1,
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
            storage_cache_config: common_config.storage_cache_config,
        }
    }
}

/// The combined error type for the `ScyllaDbStore`.
pub type ScyllaDbStoreError = ValueSplittingError<ScyllaDbStoreInternalError>;
