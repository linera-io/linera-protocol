// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::{KeyValueStore, ReadableKeyValueStore, WritableKeyValueStore},
};
use async_trait::async_trait;
use convert_case::{Case, Casing};
use linera_base::sync::Lazy;
use prometheus::{register_histogram_vec, HistogramVec};
use std::{future::Future, time::Instant};

#[derive(Clone)]
/// The implementation of the `KeyValueStoreMetrics` for the `KeyValueStore`.
pub struct KeyValueStoreMetrics {
    read_value_bytes: HistogramVec,
    contains_key: HistogramVec,
    read_multi_values_bytes: HistogramVec,
    find_keys_by_prefix: HistogramVec,
    find_key_values_by_prefix: HistogramVec,
    write_batch: HistogramVec,
    clear_journal: HistogramVec,
}

/// The metrics for the "rocks db"
#[cfg(feature = "rocksdb")]
pub static ROCKS_DB_METRICS: Lazy<KeyValueStoreMetrics> =
    Lazy::new(|| KeyValueStoreMetrics::new("rocks db internal".to_string()));

/// The metrics for the "dynamo db"
#[cfg(feature = "aws")]
pub static DYNAMO_DB_METRICS: Lazy<KeyValueStoreMetrics> =
    Lazy::new(|| KeyValueStoreMetrics::new("dynamo db internal".to_string()));

/// The metrics for the "scylla db"
#[cfg(feature = "scylladb")]
pub static SCYLLA_DB_METRICS: Lazy<KeyValueStoreMetrics> =
    Lazy::new(|| KeyValueStoreMetrics::new("scylla db internal".to_string()));

/// The metrics for the "scylla db"
#[cfg(any(feature = "rocksdb", feature = "aws"))]
pub static VALUE_SPLITTING_METRICS: Lazy<KeyValueStoreMetrics> =
    Lazy::new(|| KeyValueStoreMetrics::new("value splitting".to_string()));

/// The metrics for the "lru caching"
#[cfg(any(feature = "rocksdb", feature = "aws", feature = "scylladb"))]
pub static LRU_CACHING_METRICS: Lazy<KeyValueStoreMetrics> =
    Lazy::new(|| KeyValueStoreMetrics::new("lru caching".to_string()));

impl KeyValueStoreMetrics {
    /// Creation of a named Metered counter.
    pub fn new(name: String) -> Self {
        // name can be "rocks db". Then var_name = "rocks_db" and title_name = "RocksDb"
        let var_name = name.replace(' ', "_");
        let title_name = name.to_case(Case::Snake);

        let read_value1 = format!("{}_read_value_bytes", var_name);
        let read_value2 = format!("{} read value bytes", title_name);
        let read_value_bytes = register_histogram_vec!(read_value1, read_value2, &[])
            .expect("Counter creation should not fail");

        let contains_key1 = format!("{}_contains_key", var_name);
        let contains_key2 = format!("{} contains key", title_name);
        let contains_key = register_histogram_vec!(contains_key1, contains_key2, &[])
            .expect("Counter creation should not fail");

        let read_multi_values1 = format!("{}_read_multi_value_bytes", var_name);
        let read_multi_values2 = format!("{} read multi value bytes", title_name);
        let read_multi_values_bytes =
            register_histogram_vec!(read_multi_values1, read_multi_values2, &[])
                .expect("Counter creation should not fail");

        let find_keys1 = format!("{}_find_keys_by_prefix", var_name);
        let find_keys2 = format!("{} find keys by prefix", title_name);
        let find_keys_by_prefix = register_histogram_vec!(find_keys1, find_keys2, &[])
            .expect("Counter creation should not fail");

        let find_key_values1 = format!("{}_find_key_values_by_prefix", var_name);
        let find_key_values2 = format!("{} find key values by prefix", title_name);
        let find_key_values_by_prefix =
            register_histogram_vec!(find_key_values1, find_key_values2, &[])
                .expect("Counter creation should not fail");

        let write_batch1 = format!("{}_write_batch", var_name);
        let write_batch2 = format!("{} write batch", title_name);
        let write_batch = register_histogram_vec!(write_batch1, write_batch2, &[])
            .expect("Counter creation should not fail");

        let clear_journal1 = format!("{}_clear_journal", var_name);
        let clear_journal2 = format!("{} clear journal", title_name);
        let clear_journal = register_histogram_vec!(clear_journal1, clear_journal2, &[])
            .expect("Counter creation should not fail");

        KeyValueStoreMetrics {
            read_value_bytes,
            contains_key,
            read_multi_values_bytes,
            find_keys_by_prefix,
            find_key_values_by_prefix,
            write_batch,
            clear_journal,
        }
    }
}

/// A metered wrapper that keeps track of every operation
#[derive(Clone)]
pub struct MeteredStore<K> {
    /// the metrics being stored
    counter: &'static Lazy<KeyValueStoreMetrics>,
    /// The underlying store of the metered store
    pub store: K,
}

async fn run_with_execution_time_metric<F, O>(f: F, hist: &HistogramVec) -> O
where
    F: Future<Output = O>,
{
    let start = Instant::now();
    let out = f.await;
    let duration = start.elapsed();
    hist.with_label_values(&[])
        .observe(duration.as_micros() as f64);
    out
}

#[async_trait]
impl<K, E> ReadableKeyValueStore<E> for MeteredStore<K>
where
    K: ReadableKeyValueStore<E> + Send + Sync,
{
    const MAX_KEY_SIZE: usize = K::MAX_KEY_SIZE;
    type Keys = K::Keys;
    type KeyValues = K::KeyValues;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, E> {
        run_with_execution_time_metric(
            self.store.read_value_bytes(key),
            &self.counter.read_value_bytes,
        )
        .await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, E> {
        run_with_execution_time_metric(self.store.contains_key(key), &self.counter.contains_key)
            .await
    }

    async fn read_multi_values_bytes(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>, E> {
        run_with_execution_time_metric(
            self.store.read_multi_values_bytes(keys),
            &self.counter.read_multi_values_bytes,
        )
        .await
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, E> {
        run_with_execution_time_metric(
            self.store.find_keys_by_prefix(key_prefix),
            &self.counter.find_keys_by_prefix,
        )
        .await
    }

    async fn find_key_values_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::KeyValues, E> {
        run_with_execution_time_metric(
            self.store.find_key_values_by_prefix(key_prefix),
            &self.counter.find_key_values_by_prefix,
        )
        .await
    }
}

#[async_trait]
impl<K, E> WritableKeyValueStore<E> for MeteredStore<K>
where
    K: WritableKeyValueStore<E> + Send + Sync,
{
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), E> {
        run_with_execution_time_metric(
            self.store.write_batch(batch, base_key),
            &self.counter.write_batch,
        )
        .await
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), E> {
        run_with_execution_time_metric(
            self.store.clear_journal(base_key),
            &self.counter.clear_journal,
        )
        .await
    }
}

impl<K> KeyValueStore for MeteredStore<K>
where
    K: KeyValueStore + Send + Sync,
{
    type Error = K::Error;
}

impl<K> MeteredStore<K> {
    /// Creates a new Metered store
    pub fn new(counter: &'static Lazy<KeyValueStoreMetrics>, store: K) -> Self {
        Self { counter, store }
    }
}
