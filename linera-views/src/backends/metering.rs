// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Adds metrics to a key-value store.

use std::sync::LazyLock;

use convert_case::{Case, Casing};
use linera_base::prometheus_util::{register_histogram_vec, MeasureLatency};
use prometheus::HistogramVec;

use crate::{
    batch::Batch,
    common::{ReadableKeyValueStore, WithError, WritableKeyValueStore},
};

#[derive(Clone)]
/// The implementation of the `KeyValueStoreMetrics` for the `KeyValueStore`.
pub struct KeyValueStoreMetrics {
    read_value_bytes: HistogramVec,
    contains_key: HistogramVec,
    contains_keys: HistogramVec,
    read_multi_values_bytes: HistogramVec,
    find_keys_by_prefix: HistogramVec,
    find_key_values_by_prefix: HistogramVec,
    write_batch: HistogramVec,
    clear_journal: HistogramVec,
}

/// The metrics for the "rocks db"
#[cfg(with_rocksdb)]
pub(crate) static ROCKS_DB_METRICS: LazyLock<KeyValueStoreMetrics> =
    LazyLock::new(|| KeyValueStoreMetrics::new("rocks db internal".to_string()));

/// The metrics for the "dynamo db"
#[cfg(with_dynamodb)]
pub(crate) static DYNAMO_DB_METRICS: LazyLock<KeyValueStoreMetrics> =
    LazyLock::new(|| KeyValueStoreMetrics::new("dynamo db internal".to_string()));

/// The metrics for the "scylla db"
#[cfg(with_scylladb)]
pub(crate) static SCYLLA_DB_METRICS: LazyLock<KeyValueStoreMetrics> =
    LazyLock::new(|| KeyValueStoreMetrics::new("scylla db internal".to_string()));

/// The metrics for the "scylla db"
#[cfg(any(with_rocksdb, with_dynamodb))]
pub(crate) static VALUE_SPLITTING_METRICS: LazyLock<KeyValueStoreMetrics> =
    LazyLock::new(|| KeyValueStoreMetrics::new("value splitting".to_string()));

/// The metrics for the "lru caching"
#[cfg(any(with_rocksdb, with_dynamodb, with_scylladb))]
pub(crate) static LRU_CACHING_METRICS: LazyLock<KeyValueStoreMetrics> =
    LazyLock::new(|| KeyValueStoreMetrics::new("lru caching".to_string()));

impl KeyValueStoreMetrics {
    /// Creation of a named Metered counter.
    pub fn new(name: String) -> Self {
        // name can be "rocks db". Then var_name = "rocks_db" and title_name = "RocksDb"
        let var_name = name.replace(' ', "_");
        let title_name = name.to_case(Case::Snake);

        let read_value1 = format!("{}_read_value_bytes", var_name);
        let read_value2 = format!("{} read value bytes", title_name);
        let read_value_bytes = register_histogram_vec(&read_value1, &read_value2, &[], None)
            .expect("Counter creation should not fail");

        let contains_key1 = format!("{}_contains_key", var_name);
        let contains_key2 = format!("{} contains key", title_name);
        let contains_key = register_histogram_vec(&contains_key1, &contains_key2, &[], None)
            .expect("Counter creation should not fail");

        let contains_keys1 = format!("{}_contains_keys", var_name);
        let contains_keys2 = format!("{} contains keys", title_name);
        let contains_keys = register_histogram_vec(&contains_keys1, &contains_keys2, &[], None)
            .expect("Counter creation should not fail");

        let read_multi_values1 = format!("{}_read_multi_value_bytes", var_name);
        let read_multi_values2 = format!("{} read multi value bytes", title_name);
        let read_multi_values_bytes =
            register_histogram_vec(&read_multi_values1, &read_multi_values2, &[], None)
                .expect("Counter creation should not fail");

        let find_keys1 = format!("{}_find_keys_by_prefix", var_name);
        let find_keys2 = format!("{} find keys by prefix", title_name);
        let find_keys_by_prefix = register_histogram_vec(&find_keys1, &find_keys2, &[], None)
            .expect("Counter creation should not fail");

        let find_key_values1 = format!("{}_find_key_values_by_prefix", var_name);
        let find_key_values2 = format!("{} find key values by prefix", title_name);
        let find_key_values_by_prefix =
            register_histogram_vec(&find_key_values1, &find_key_values2, &[], None)
                .expect("Counter creation should not fail");

        let write_batch1 = format!("{}_write_batch", var_name);
        let write_batch2 = format!("{} write batch", title_name);
        let write_batch = register_histogram_vec(&write_batch1, &write_batch2, &[], None)
            .expect("Counter creation should not fail");

        let clear_journal1 = format!("{}_clear_journal", var_name);
        let clear_journal2 = format!("{} clear journal", title_name);
        let clear_journal = register_histogram_vec(&clear_journal1, &clear_journal2, &[], None)
            .expect("Counter creation should not fail");

        KeyValueStoreMetrics {
            read_value_bytes,
            contains_key,
            contains_keys,
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
    counter: &'static LazyLock<KeyValueStoreMetrics>,
    /// The underlying store of the metered store
    pub store: K,
}

impl<K> WithError for MeteredStore<K>
where
    K: WithError,
{
    type Error = K::Error;
}

impl<K> ReadableKeyValueStore for MeteredStore<K>
where
    K: ReadableKeyValueStore + Send + Sync,
{
    const MAX_KEY_SIZE: usize = K::MAX_KEY_SIZE;
    type Keys = K::Keys;
    type KeyValues = K::KeyValues;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let _metric = self.counter.read_value_bytes.measure_latency();
        self.store.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error> {
        let _metric = self.counter.contains_key.measure_latency();
        self.store.contains_key(key).await
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Self::Error> {
        let _metric = self.counter.contains_keys.measure_latency();
        self.store.contains_keys(keys).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let _metric = self.counter.read_multi_values_bytes.measure_latency();
        self.store.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        let _metric = self.counter.find_keys_by_prefix.measure_latency();
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        let _metric = self.counter.find_key_values_by_prefix.measure_latency();
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

impl<K> WritableKeyValueStore for MeteredStore<K>
where
    K: WritableKeyValueStore + Send + Sync,
{
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        let _metric = self.counter.write_batch.measure_latency();
        self.store.write_batch(batch).await
    }

    async fn clear_journal(&self) -> Result<(), Self::Error> {
        let _metric = self.counter.clear_journal.measure_latency();
        self.store.clear_journal().await
    }
}

impl<K> MeteredStore<K> {
    /// Creates a new Metered store
    pub fn new(counter: &'static LazyLock<KeyValueStoreMetrics>, store: K) -> Self {
        Self { counter, store }
    }
}
