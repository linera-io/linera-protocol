// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Adds metrics to a key-value store.

use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{Arc, LazyLock, Mutex},
};

use convert_case::{Case, Casing};
use linera_base::prometheus_util::{
    register_histogram_vec, register_int_counter_vec, MeasureLatency,
};
use prometheus::{HistogramVec, IntCounterVec};

#[cfg(with_testing)]
use crate::store::TestKeyValueStore;
use crate::{
    batch::Batch,
    store::{
        AdminKeyValueStore, KeyIterable as _, KeyValueIterable as _, ReadableKeyValueStore,
        WithError, WritableKeyValueStore,
    },
};

#[derive(Clone)]
/// The implementation of the `KeyValueStoreMetrics` for the `KeyValueStore`.
pub struct KeyValueStoreMetrics {
    read_value_bytes_latency: HistogramVec,
    contains_key_latency: HistogramVec,
    contains_keys_latency: HistogramVec,
    read_multi_values_bytes_latency: HistogramVec,
    find_keys_by_prefix_latency: HistogramVec,
    find_key_values_by_prefix_latency: HistogramVec,
    write_batch_latency: HistogramVec,
    clear_journal_latency: HistogramVec,
    connect_latency: HistogramVec,
    clone_with_root_key_latency: HistogramVec,
    list_all_latency: HistogramVec,
    delete_all_latency: HistogramVec,
    exists_latency: HistogramVec,
    create_latency: HistogramVec,
    delete_latency: HistogramVec,
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
    list_all_sizes: HistogramVec,
    exists_true_cases: IntCounterVec,
}

#[derive(Default)]
struct StoreMetrics {
    stores: BTreeMap<String, Arc<KeyValueStoreMetrics>>,
}

/// The global variables of the RocksDB stores
static STORE_COUNTERS: LazyLock<Mutex<StoreMetrics>> =
    LazyLock::new(|| Mutex::new(StoreMetrics::default()));

fn get_counter(name: &str) -> Arc<KeyValueStoreMetrics> {
    let mut store_metrics = STORE_COUNTERS.lock().unwrap();
    let key = name.to_string();
    match store_metrics.stores.entry(key) {
        Entry::Occupied(entry) => {
            let entry = entry.into_mut();
            entry.clone()
        }
        Entry::Vacant(entry) => {
            let store_metric = Arc::new(KeyValueStoreMetrics::new(name.to_string()));
            entry.insert(store_metric.clone());
            store_metric
        }
    }
}

impl KeyValueStoreMetrics {
    /// Creation of a named Metered counter.
    pub fn new(name: String) -> Self {
        // name can be "rocks db". Then var_name = "rocks_db" and title_name = "RocksDb"
        let var_name = name.replace(' ', "_");
        let title_name = name.to_case(Case::Snake);

        let entry1 = format!("{}_read_value_bytes_latency", var_name);
        let entry2 = format!("{} read value bytes latency", title_name);
        let read_value_bytes_latency = register_histogram_vec(&entry1, &entry2, &[], None):

        let entry1 = format!("{}_contains_key_latency", var_name);
        let entry2 = format!("{} contains key latency", title_name);
        let contains_key_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_contains_keys_latency", var_name);
        let entry2 = format!("{} contains keys latency", title_name);
        let contains_keys_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_read_multi_value_bytes_latency", var_name);
        let entry2 = format!("{} read multi value bytes latency", title_name);
        let read_multi_values_bytes_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_find_keys_by_prefix_latency", var_name);
        let entry2 = format!("{} find keys by prefix latency", title_name);
        let find_keys_by_prefix_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_find_key_values_by_prefix_latency", var_name);
        let entry2 = format!("{} find key values by prefix latency", title_name);
        let find_key_values_by_prefix_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_write_batch_latency", var_name);
        let entry2 = format!("{} write batch latency", title_name);
        let write_batch_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_clear_journal_latency", var_name);
        let entry2 = format!("{} clear journal latency", title_name);
        let clear_journal_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_connect_latency", var_name);
        let entry2 = format!("{} connect latency", title_name);
        let connect_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_clone_with_root_key_latency", var_name);
        let entry2 = format!("{} clone with root key latency", title_name);
        let clone_with_root_key_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_list_all_latency", var_name);
        let entry2 = format!("{} list all latency", title_name);
        let list_all_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_delete_all_latency", var_name);
        let entry2 = format!("{} delete all latency", title_name);
        let delete_all_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_exists_latency", var_name);
        let entry2 = format!("{} exists latency", title_name);
        let exists_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_create_latency", var_name);
        let entry2 = format!("{} create latency", title_name);
        let create_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_delete_latency", var_name);
        let entry2 = format!("{} delete latency", title_name);
        let delete_latency = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_read_value_number_none_cases", var_name);
        let entry2 = format!("{} read value number none cases", title_name);
        let read_value_none_cases = register_int_counter_vec(&entry1, &entry2, &[]);

        let entry1 = format!("{}_read_value_key_size", var_name);
        let entry2 = format!("{} read value key size", title_name);
        let read_value_key_size = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_read_value_value_size", var_name);
        let entry2 = format!("{} read value value size", title_name);
        let read_value_value_size = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_read_multi_values_num_entries", var_name);
        let entry2 = format!("{} read multi values num entries", title_name);
        let read_multi_values_num_entries = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_read_multi_values_key_sizes", var_name);
        let entry2 = format!("{} read multi values key sizes", title_name);
        let read_multi_values_key_sizes = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_contains_keys_num_entries", var_name);
        let entry2 = format!("{} contains keys num entries", title_name);
        let contains_keys_num_entries = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_contains_keys_key_sizes", var_name);
        let entry2 = format!("{} contains keys key sizes", title_name);
        let contains_keys_key_sizes = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_contains_key_key_size", var_name);
        let entry2 = format!("{} contains key key size", title_name);
        let contains_key_key_size = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_find_keys_by_prefix_prefix_size", var_name);
        let entry2 = format!("{} find keys by prefix prefix size", title_name);
        let find_keys_by_prefix_prefix_size = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_find_keys_by_prefix_num_keys", var_name);
        let entry2 = format!("{} find keys by prefix num keys", title_name);
        let find_keys_by_prefix_num_keys = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_find_keys_by_prefix_keys_size", var_name);
        let entry2 = format!("{} find keys by prefix keys size", title_name);
        let find_keys_by_prefix_keys_size = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_find_key_values_by_prefix_prefix_size", var_name);
        let entry2 = format!("{} find key values by prefix prefix size", title_name);
        let find_key_values_by_prefix_prefix_size =
            register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_find_key_values_by_prefix_num_keys", var_name);
        let entry2 = format!("{} find key values by prefix num keys", title_name);
        let find_key_values_by_prefix_num_keys =
            register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_find_key_values_by_prefix_key_values_size", var_name);
        let entry2 = format!("{} find key values by prefix key values size", title_name);
        let find_key_values_by_prefix_key_values_size =
            register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_write_batch_size", var_name);
        let entry2 = format!("{} write batch size", title_name);
        let write_batch_size = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_list_all_sizes", var_name);
        let entry2 = format!("{} list all sizes", title_name);
        let list_all_sizes = register_histogram_vec(&entry1, &entry2, &[], None);

        let entry1 = format!("{}_exists_true_cases", var_name);
        let entry2 = format!("{} exists true cases", title_name);
        let exists_true_cases = register_int_counter_vec(&entry1, &entry2, &[]);

        KeyValueStoreMetrics {
            read_value_bytes_latency,
            contains_key_latency,
            contains_keys_latency,
            read_multi_values_bytes_latency,
            find_keys_by_prefix_latency,
            find_key_values_by_prefix_latency,
            write_batch_latency,
            clear_journal_latency,
            connect_latency,
            clone_with_root_key_latency,
            list_all_latency,
            delete_all_latency,
            exists_latency,
            create_latency,
            delete_latency,
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
            list_all_sizes,
            exists_true_cases,
        }
    }
}

/// A metered wrapper that keeps track of every operation
#[derive(Clone)]
pub struct MeteredStore<K> {
    /// the metrics being stored
    counter: Arc<KeyValueStoreMetrics>,
    /// The underlying store of the metered store
    store: K,
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
        let _latency = self.counter.read_value_bytes_latency.measure_latency();
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
        let _latency = self.counter.contains_key_latency.measure_latency();
        self.counter
            .contains_key_key_size
            .with_label_values(&[])
            .observe(key.len() as f64);
        self.store.contains_key(key).await
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Self::Error> {
        let _latency = self.counter.contains_keys_latency.measure_latency();
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
        let _latency = self
            .counter
            .read_multi_values_bytes_latency
            .measure_latency();
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
        let _latency = self.counter.find_keys_by_prefix_latency.measure_latency();
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
        let _latency = self
            .counter
            .find_key_values_by_prefix_latency
            .measure_latency();
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
        let _latency = self.counter.write_batch_latency.measure_latency();
        self.counter
            .write_batch_size
            .with_label_values(&[])
            .observe(batch.size() as f64);
        self.store.write_batch(batch).await
    }

    async fn clear_journal(&self) -> Result<(), Self::Error> {
        let _metric = self.counter.clear_journal_latency.measure_latency();
        self.store.clear_journal().await
    }
}

impl<K> AdminKeyValueStore for MeteredStore<K>
where
    K: AdminKeyValueStore + Send + Sync,
{
    type Config = K::Config;

    fn get_name() -> String {
        K::get_name()
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, Self::Error> {
        let name = K::get_name();
        let counter = get_counter(&name);
        let _latency = counter.connect_latency.measure_latency();
        let store = K::connect(config, namespace, root_key).await?;
        let counter = get_counter(&name);
        Ok(Self { counter, store })
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, Self::Error> {
        let _latency = self.counter.clone_with_root_key_latency.measure_latency();
        let store = self.store.clone_with_root_key(root_key)?;
        let counter = self.counter.clone();
        Ok(Self { counter, store })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, Self::Error> {
        let name = K::get_name();
        let counter = get_counter(&name);
        let _latency = counter.list_all_latency.measure_latency();
        let namespaces = K::list_all(config).await?;
        let counter = get_counter(&name);
        counter
            .list_all_sizes
            .with_label_values(&[])
            .observe(namespaces.len() as f64);
        Ok(namespaces)
    }

    async fn delete_all(config: &Self::Config) -> Result<(), Self::Error> {
        let name = K::get_name();
        let counter = get_counter(&name);
        let _latency = counter.delete_all_latency.measure_latency();
        K::delete_all(config).await
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, Self::Error> {
        let name = K::get_name();
        let counter = get_counter(&name);
        let _latency = counter.exists_latency.measure_latency();
        let result = K::exists(config, namespace).await?;
        if result {
            let counter = get_counter(&name);
            counter.exists_true_cases.with_label_values(&[]).inc();
        }
        Ok(result)
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        let name = K::get_name();
        let counter = get_counter(&name);
        let _latency = counter.create_latency.measure_latency();
        K::create(config, namespace).await
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        let name = K::get_name();
        let counter = get_counter(&name);
        let _latency = counter.delete_latency.measure_latency();
        K::delete(config, namespace).await
    }
}

#[cfg(with_testing)]
impl<K> TestKeyValueStore for MeteredStore<K>
where
    K: TestKeyValueStore + Send + Sync,
{
    async fn new_test_config() -> Result<K::Config, Self::Error> {
        K::new_test_config().await
    }
}
