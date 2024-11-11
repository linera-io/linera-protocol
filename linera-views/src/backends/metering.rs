// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Adds metrics to a key-value store.

use std::sync::LazyLock;

use convert_case::{Case, Casing};
use linera_base::prometheus_util::{
    register_histogram_vec, register_int_counter_vec, MeasureLatency,
};
use prometheus::{HistogramVec, IntCounterVec};

use crate::{
    batch::Batch,
    store::{
        KeyIterable as _, KeyValueIterable as _, ReadableKeyValueStore, WithError,
        WritableKeyValueStore,
    },
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
    read_value_none_cases: IntCounterVec,
    read_value_key_size: HistogramVec,
    read_value_value_size: HistogramVec,
    read_multi_values_num_entries: HistogramVec,
    read_multi_values_key_sizes: HistogramVec,
    contains_keys_num_entries: HistogramVec,
    contains_keys_key_sizes: HistogramVec,
    contains_key_key_size: HistogramVec,
    find_keys_by_prefix_prefix_size: HistogramVec,
    find_keys_by_prefix_num_keys: HistogramVec,
    find_keys_by_prefix_keys_size: HistogramVec,
    find_key_values_by_prefix_prefix_size: HistogramVec,
    find_key_values_by_prefix_num_keys: HistogramVec,
    find_key_values_by_prefix_key_values_size: HistogramVec,
    write_batch_size: HistogramVec,
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

        let read_value_bytes1 = format!("{}_read_value_bytes_latency", var_name);
        let read_value_bytes2 = format!("{} read value bytes latency", title_name);
        let read_value_bytes =
            register_histogram_vec(&read_value_bytes1, &read_value_bytes2, &[], None);

        let contains_key1 = format!("{}_contains_key_latency", var_name);
        let contains_key2 = format!("{} contains key latency", title_name);
        let contains_key = register_histogram_vec(&contains_key1, &contains_key2, &[], None);

        let contains_keys1 = format!("{}_contains_keys_latency", var_name);
        let contains_keys2 = format!("{} contains keys latency", title_name);
        let contains_keys = register_histogram_vec(&contains_keys1, &contains_keys2, &[], None);

        let read_multi_values1 = format!("{}_read_multi_value_bytes_latency", var_name);
        let read_multi_values2 = format!("{} read multi value bytes latency", title_name);
        let read_multi_values_bytes =
            register_histogram_vec(&read_multi_values1, &read_multi_values2, &[], None);

        let find_keys1 = format!("{}_find_keys_by_prefix_latency", var_name);
        let find_keys2 = format!("{} find keys by prefix latency", title_name);
        let find_keys_by_prefix = register_histogram_vec(&find_keys1, &find_keys2, &[], None);

        let find_key_values1 = format!("{}_find_key_values_by_prefix_latency", var_name);
        let find_key_values2 = format!("{} find key values by prefix latency", title_name);
        let find_key_values_by_prefix =
            register_histogram_vec(&find_key_values1, &find_key_values2, &[], None);

        let write_batch1 = format!("{}_write_batch_latency", var_name);
        let write_batch2 = format!("{} write batch latency", title_name);
        let write_batch = register_histogram_vec(&write_batch1, &write_batch2, &[], None);

        let clear_journal1 = format!("{}_clear_journal_latency", var_name);
        let clear_journal2 = format!("{} clear journal latency", title_name);
        let clear_journal = register_histogram_vec(&clear_journal1, &clear_journal2, &[], None);

        let read_value_none_cases1 = format!("{}_read_value_number_none_cases", var_name);
        let read_value_none_cases2 = format!("{} read value number none cases", title_name);
        let read_value_none_cases =
            register_int_counter_vec(&read_value_none_cases1, &read_value_none_cases2, &[]);

        let read_value_key_size1 = format!("{}_read_value_key_size", var_name);
        let read_value_key_size2 = format!("{} read value key size", title_name);
        let read_value_key_size =
            register_histogram_vec(&read_value_key_size1, &read_value_key_size2, &[], None);

        let read_value_value_size1 = format!("{}_read_value_value_size", var_name);
        let read_value_value_size2 = format!("{} read value value size", title_name);
        let read_value_value_size =
            register_histogram_vec(&read_value_value_size1, &read_value_value_size2, &[], None);

        let read_multi_values_num_entries1 = format!("{}_read_multi_values_num_entries", var_name);
        let read_multi_values_num_entries2 =
            format!("{} read multi values num entries", title_name);
        let read_multi_values_num_entries = register_histogram_vec(
            &read_multi_values_num_entries1,
            &read_multi_values_num_entries2,
            &[],
            None,
        );

        let read_multi_values_key_sizes1 = format!("{}_read_multi_values_key_sizes", var_name);
        let read_multi_values_key_sizes2 = format!("{} read multi values key sizes", title_name);
        let read_multi_values_key_sizes = register_histogram_vec(
            &read_multi_values_key_sizes1,
            &read_multi_values_key_sizes2,
            &[],
            None,
        );

        let contains_keys_num_entries1 = format!("{}_contains_keys_num_entries", var_name);
        let contains_keys_num_entries2 = format!("{} contains keys num entries", title_name);
        let contains_keys_num_entries = register_histogram_vec(
            &contains_keys_num_entries1,
            &contains_keys_num_entries2,
            &[],
            None,
        );

        let contains_keys_key_sizes1 = format!("{}_contains_keys_key_sizes", var_name);
        let contains_keys_key_sizes2 = format!("{} contains keys key sizes", title_name);
        let contains_keys_key_sizes = register_histogram_vec(
            &contains_keys_key_sizes1,
            &contains_keys_key_sizes2,
            &[],
            None,
        );

        let contains_key_key_size1 = format!("{}_contains_key_key_size", var_name);
        let contains_key_key_size2 = format!("{} contains key key size", title_name);
        let contains_key_key_size =
            register_histogram_vec(&contains_key_key_size1, &contains_key_key_size2, &[], None);

        let find_keys_by_prefix_prefix_size1 =
            format!("{}_find_keys_by_prefix_prefix_size", var_name);
        let find_keys_by_prefix_prefix_size2 =
            format!("{} find keys by prefix prefix size", title_name);
        let find_keys_by_prefix_prefix_size = register_histogram_vec(
            &find_keys_by_prefix_prefix_size1,
            &find_keys_by_prefix_prefix_size2,
            &[],
            None,
        );

        let find_keys_by_prefix_num_keys1 = format!("{}_find_keys_by_prefix_num_keys", var_name);
        let find_keys_by_prefix_num_keys2 = format!("{} find keys by prefix num keys", title_name);
        let find_keys_by_prefix_num_keys = register_histogram_vec(
            &find_keys_by_prefix_num_keys1,
            &find_keys_by_prefix_num_keys2,
            &[],
            None,
        );

        let find_keys_by_prefix_keys_size1 = format!("{}_find_keys_by_prefix_keys_size", var_name);
        let find_keys_by_prefix_keys_size2 =
            format!("{} find keys by prefix keys size", title_name);
        let find_keys_by_prefix_keys_size = register_histogram_vec(
            &find_keys_by_prefix_keys_size1,
            &find_keys_by_prefix_keys_size2,
            &[],
            None,
        );

        let find_key_values_by_prefix_prefix_size1 =
            format!("{}_find_key_values_by_prefix_prefix_size", var_name);
        let find_key_values_by_prefix_prefix_size2 =
            format!("{} find key values by prefix prefix size", title_name);
        let find_key_values_by_prefix_prefix_size = register_histogram_vec(
            &find_key_values_by_prefix_prefix_size1,
            &find_key_values_by_prefix_prefix_size2,
            &[],
            None,
        );

        let find_key_values_by_prefix_num_keys1 =
            format!("{}_find_key_values_by_prefix_num_keys", var_name);
        let find_key_values_by_prefix_num_keys2 =
            format!("{} find key values by prefix num keys", title_name);
        let find_key_values_by_prefix_num_keys = register_histogram_vec(
            &find_key_values_by_prefix_num_keys1,
            &find_key_values_by_prefix_num_keys2,
            &[],
            None,
        );

        let find_key_values_by_prefix_key_values_size1 =
            format!("{}_find_key_values_by_prefix_key_values_size", var_name);
        let find_key_values_by_prefix_key_values_size2 =
            format!("{} find key values by prefix key values size", title_name);
        let find_key_values_by_prefix_key_values_size = register_histogram_vec(
            &find_key_values_by_prefix_key_values_size1,
            &find_key_values_by_prefix_key_values_size2,
            &[],
            None,
        );

        let write_batch_size1 = format!("{}_write_batch_size", var_name);
        let write_batch_size2 = format!("{} write batch size", title_name);
        let write_batch_size =
            register_histogram_vec(&write_batch_size1, &write_batch_size2, &[], None);

        KeyValueStoreMetrics {
            read_value_bytes,
            contains_key,
            contains_keys,
            read_multi_values_bytes,
            find_keys_by_prefix,
            find_key_values_by_prefix,
            write_batch,
            clear_journal,
            read_value_none_cases,
            read_value_key_size,
            read_value_value_size,
            read_multi_values_num_entries,
            read_multi_values_key_sizes,
            contains_keys_num_entries,
            contains_keys_key_sizes,
            contains_key_key_size,
            find_keys_by_prefix_prefix_size,
            find_keys_by_prefix_num_keys,
            find_keys_by_prefix_keys_size,
            find_key_values_by_prefix_prefix_size,
            find_key_values_by_prefix_num_keys,
            find_key_values_by_prefix_key_values_size,
            write_batch_size,
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
        let _latency = self.counter.read_value_bytes.measure_latency();
        self.counter
            .read_value_key_size
            .with_label_values(&[])
            .observe(key.len() as f64);
        let result = self.store.read_value_bytes(key).await?;
        match &result {
            None => self
                .counter
                .read_value_none_cases
                .with_label_values(&[])
                .inc(),
            Some(value) => self
                .counter
                .read_value_value_size
                .with_label_values(&[])
                .observe(value.len() as f64),
        }
        Ok(result)
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error> {
        let _latency = self.counter.contains_key.measure_latency();
        self.counter
            .contains_key_key_size
            .with_label_values(&[])
            .observe(key.len() as f64);
        self.store.contains_key(key).await
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Self::Error> {
        let _latency = self.counter.contains_keys.measure_latency();
        self.counter
            .contains_keys_num_entries
            .with_label_values(&[])
            .observe(keys.len() as f64);
        let key_sizes = keys.iter().map(|k| k.len()).sum::<usize>();
        self.counter
            .contains_keys_key_sizes
            .with_label_values(&[])
            .observe(key_sizes as f64);
        self.store.contains_keys(keys).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let _latency = self.counter.read_multi_values_bytes.measure_latency();
        self.counter
            .read_multi_values_num_entries
            .with_label_values(&[])
            .observe(keys.len() as f64);
        let key_sizes = keys.iter().map(|k| k.len()).sum::<usize>();
        self.counter
            .read_multi_values_key_sizes
            .with_label_values(&[])
            .observe(key_sizes as f64);
        self.store.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        let _latency = self.counter.find_keys_by_prefix.measure_latency();
        self.counter
            .find_keys_by_prefix_prefix_size
            .with_label_values(&[])
            .observe(key_prefix.len() as f64);
        let result = self.store.find_keys_by_prefix(key_prefix).await?;
        let (num_keys, keys_size) = result
            .iterator()
            .map(|key| key.map(|k| k.len()))
            .collect::<Result<Vec<usize>, _>>()?
            .into_iter()
            .fold((0, 0), |(count, size), len| (count + 1, size + len));
        self.counter
            .find_keys_by_prefix_num_keys
            .with_label_values(&[])
            .observe(num_keys as f64);
        self.counter
            .find_keys_by_prefix_keys_size
            .with_label_values(&[])
            .observe(keys_size as f64);
        Ok(result)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        let _latency = self.counter.find_key_values_by_prefix.measure_latency();
        self.counter
            .find_key_values_by_prefix_prefix_size
            .with_label_values(&[])
            .observe(key_prefix.len() as f64);
        let result = self.store.find_key_values_by_prefix(key_prefix).await?;
        let (num_keys, key_values_size) = result
            .iterator()
            .map(|key_value| key_value.map(|(key, value)| key.len() + value.len()))
            .collect::<Result<Vec<usize>, _>>()?
            .into_iter()
            .fold((0, 0), |(count, size), len| (count + 1, size + len));
        self.counter
            .find_key_values_by_prefix_num_keys
            .with_label_values(&[])
            .observe(num_keys as f64);
        self.counter
            .find_key_values_by_prefix_key_values_size
            .with_label_values(&[])
            .observe(key_values_size as f64);
        Ok(result)
    }
}

impl<K> WritableKeyValueStore for MeteredStore<K>
where
    K: WritableKeyValueStore + Send + Sync,
{
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        let _latency = self.counter.write_batch.measure_latency();
        self.counter
            .write_batch_size
            .with_label_values(&[])
            .observe(batch.size() as f64);
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
