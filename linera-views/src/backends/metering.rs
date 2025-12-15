// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Adds metrics to a key-value store.

use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{Arc, LazyLock, Mutex},
};

use convert_case::{Case, Casing};
use linera_base::prometheus_util::{
    exponential_bucket_latencies, register_histogram_vec, register_int_counter_vec,
    MeasureLatency as _,
};
use prometheus::{exponential_buckets, HistogramVec, IntCounterVec};

#[cfg(with_testing)]
use crate::store::TestKeyValueDatabase;
use crate::{
    batch::Batch,
    store::{KeyValueDatabase, ReadableKeyValueStore, WithError, WritableKeyValueStore},
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
    open_shared_latency: HistogramVec,
    open_exclusive_latency: HistogramVec,
    list_all_latency: HistogramVec,
    list_root_keys_latency: HistogramVec,
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

        // Latency buckets in milliseconds: up to 10 seconds
        let latency_buckets = exponential_bucket_latencies(10000.0);
        // Size buckets in bytes: 1B to 10MB
        let size_buckets =
            Some(exponential_buckets(1.0, 4.0, 12).expect("Size buckets creation should not fail"));
        // Count buckets: 1 to 100,000
        let count_buckets = Some(
            exponential_buckets(1.0, 3.0, 11).expect("Count buckets creation should not fail"),
        );

        let entry1 = format!("{}_read_value_bytes_latency", var_name);
        let entry2 = format!("{} read value bytes latency", title_name);
        let read_value_bytes_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_contains_key_latency", var_name);
        let entry2 = format!("{} contains key latency", title_name);
        let contains_key_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_contains_keys_latency", var_name);
        let entry2 = format!("{} contains keys latency", title_name);
        let contains_keys_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_read_multi_value_bytes_latency", var_name);
        let entry2 = format!("{} read multi value bytes latency", title_name);
        let read_multi_values_bytes_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_find_keys_by_prefix_latency", var_name);
        let entry2 = format!("{} find keys by prefix latency", title_name);
        let find_keys_by_prefix_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_find_key_values_by_prefix_latency", var_name);
        let entry2 = format!("{} find key values by prefix latency", title_name);
        let find_key_values_by_prefix_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_write_batch_latency", var_name);
        let entry2 = format!("{} write batch latency", title_name);
        let write_batch_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_clear_journal_latency", var_name);
        let entry2 = format!("{} clear journal latency", title_name);
        let clear_journal_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_connect_latency", var_name);
        let entry2 = format!("{} connect latency", title_name);
        let connect_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_open_shared_latency", var_name);
        let entry2 = format!("{} open shared partition", title_name);
        let open_shared_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_open_exclusive_latency", var_name);
        let entry2 = format!("{} open exclusive partition", title_name);
        let open_exclusive_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_list_all_latency", var_name);
        let entry2 = format!("{} list all latency", title_name);
        let list_all_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_list_root_keys_latency", var_name);
        let entry2 = format!("{} list root keys latency", title_name);
        let list_root_keys_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_delete_all_latency", var_name);
        let entry2 = format!("{} delete all latency", title_name);
        let delete_all_latency =
            register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_exists_latency", var_name);
        let entry2 = format!("{} exists latency", title_name);
        let exists_latency = register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_create_latency", var_name);
        let entry2 = format!("{} create latency", title_name);
        let create_latency = register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_delete_latency", var_name);
        let entry2 = format!("{} delete latency", title_name);
        let delete_latency = register_histogram_vec(&entry1, &entry2, &[], latency_buckets.clone());

        let entry1 = format!("{}_read_value_none_cases", var_name);
        let entry2 = format!("{} read value none cases", title_name);
        let read_value_none_cases = register_int_counter_vec(&entry1, &entry2, &[]);

        let entry1 = format!("{}_read_value_key_size", var_name);
        let entry2 = format!("{} read value key size", title_name);
        let read_value_key_size =
            register_histogram_vec(&entry1, &entry2, &[], size_buckets.clone());

        let entry1 = format!("{}_read_value_value_size", var_name);
        let entry2 = format!("{} read value value size", title_name);
        let read_value_value_size =
            register_histogram_vec(&entry1, &entry2, &[], size_buckets.clone());

        let entry1 = format!("{}_read_multi_values_num_entries", var_name);
        let entry2 = format!("{} read multi values num entries", title_name);
        let read_multi_values_num_entries =
            register_histogram_vec(&entry1, &entry2, &[], count_buckets.clone());

        let entry1 = format!("{}_read_multi_values_key_sizes", var_name);
        let entry2 = format!("{} read multi values key sizes", title_name);
        let read_multi_values_key_sizes =
            register_histogram_vec(&entry1, &entry2, &[], size_buckets.clone());

        let entry1 = format!("{}_contains_keys_num_entries", var_name);
        let entry2 = format!("{} contains keys num entries", title_name);
        let contains_keys_num_entries =
            register_histogram_vec(&entry1, &entry2, &[], count_buckets.clone());

        let entry1 = format!("{}_contains_keys_key_sizes", var_name);
        let entry2 = format!("{} contains keys key sizes", title_name);
        let contains_keys_key_sizes =
            register_histogram_vec(&entry1, &entry2, &[], size_buckets.clone());

        let entry1 = format!("{}_contains_key_key_size", var_name);
        let entry2 = format!("{} contains key key size", title_name);
        let contains_key_key_size =
            register_histogram_vec(&entry1, &entry2, &[], size_buckets.clone());

        let entry1 = format!("{}_find_keys_by_prefix_prefix_size", var_name);
        let entry2 = format!("{} find keys by prefix prefix size", title_name);
        let find_keys_by_prefix_prefix_size =
            register_histogram_vec(&entry1, &entry2, &[], size_buckets.clone());

        let entry1 = format!("{}_find_keys_by_prefix_num_keys", var_name);
        let entry2 = format!("{} find keys by prefix num keys", title_name);
        let find_keys_by_prefix_num_keys =
            register_histogram_vec(&entry1, &entry2, &[], count_buckets.clone());

        let entry1 = format!("{}_find_keys_by_prefix_keys_size", var_name);
        let entry2 = format!("{} find keys by prefix keys size", title_name);
        let find_keys_by_prefix_keys_size =
            register_histogram_vec(&entry1, &entry2, &[], size_buckets.clone());

        let entry1 = format!("{}_find_key_values_by_prefix_prefix_size", var_name);
        let entry2 = format!("{} find key values by prefix prefix size", title_name);
        let find_key_values_by_prefix_prefix_size =
            register_histogram_vec(&entry1, &entry2, &[], size_buckets.clone());

        let entry1 = format!("{}_find_key_values_by_prefix_num_keys", var_name);
        let entry2 = format!("{} find key values by prefix num keys", title_name);
        let find_key_values_by_prefix_num_keys =
            register_histogram_vec(&entry1, &entry2, &[], count_buckets.clone());

        let entry1 = format!("{}_find_key_values_by_prefix_key_values_size", var_name);
        let entry2 = format!("{} find key values by prefix key values size", title_name);
        let find_key_values_by_prefix_key_values_size =
            register_histogram_vec(&entry1, &entry2, &[], size_buckets.clone());

        let entry1 = format!("{}_write_batch_size", var_name);
        let entry2 = format!("{} write batch size", title_name);
        let write_batch_size = register_histogram_vec(&entry1, &entry2, &[], size_buckets);

        let entry1 = format!("{}_list_all_sizes", var_name);
        let entry2 = format!("{} list all sizes", title_name);
        let list_all_sizes = register_histogram_vec(&entry1, &entry2, &[], count_buckets);

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
            open_shared_latency,
            open_exclusive_latency,
            list_all_latency,
            list_root_keys_latency,
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

/// A metered database that keeps track of every operation.
#[derive(Clone)]
pub struct MeteredDatabase<D> {
    /// The metrics being computed.
    counter: Arc<KeyValueStoreMetrics>,
    /// The underlying database.
    database: D,
}

/// A metered store that keeps track of every operation.
#[derive(Clone)]
pub struct MeteredStore<S> {
    /// The metrics being computed.
    counter: Arc<KeyValueStoreMetrics>,
    /// The underlying store.
    store: S,
}

impl<D> WithError for MeteredDatabase<D>
where
    D: WithError,
{
    type Error = D::Error;
}

impl<S> WithError for MeteredStore<S>
where
    S: WithError,
{
    type Error = S::Error;
}

impl<S> ReadableKeyValueStore for MeteredStore<S>
where
    S: ReadableKeyValueStore,
{
    const MAX_KEY_SIZE: usize = S::MAX_KEY_SIZE;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    fn root_key(&self) -> Result<Vec<u8>, Self::Error> {
        self.store.root_key()
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

    async fn contains_keys(&self, keys: &[Vec<u8>]) -> Result<Vec<bool>, Self::Error> {
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
        keys: &[Vec<u8>],
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

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let _latency = self.counter.find_keys_by_prefix_latency.measure_latency();
        self.counter
            .find_keys_by_prefix_prefix_size
            .with_label_values(&[])
            .observe(key_prefix.len() as f64);
        let result = self.store.find_keys_by_prefix(key_prefix).await?;
        let (num_keys, keys_size) = result
            .iter()
            .map(|key| key.len())
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
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
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
            .iter()
            .map(|(key, value)| key.len() + value.len())
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

impl<S> WritableKeyValueStore for MeteredStore<S>
where
    S: WritableKeyValueStore,
{
    const MAX_VALUE_SIZE: usize = S::MAX_VALUE_SIZE;

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

impl<D> KeyValueDatabase for MeteredDatabase<D>
where
    D: KeyValueDatabase,
{
    type Config = D::Config;
    type Store = MeteredStore<D::Store>;

    fn get_name() -> String {
        D::get_name()
    }

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, Self::Error> {
        let name = D::get_name();
        let counter = get_counter(&name);
        let _latency = counter.connect_latency.measure_latency();
        let database = D::connect(config, namespace).await?;
        let counter = get_counter(&name);
        Ok(Self { counter, database })
    }

    fn open_shared(&self, root_key: &[u8]) -> Result<Self::Store, Self::Error> {
        let _latency = self.counter.open_shared_latency.measure_latency();
        let store = self.database.open_shared(root_key)?;
        let counter = self.counter.clone();
        Ok(MeteredStore { counter, store })
    }

    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self::Store, Self::Error> {
        let _latency = self.counter.open_exclusive_latency.measure_latency();
        let store = self.database.open_exclusive(root_key)?;
        let counter = self.counter.clone();
        Ok(MeteredStore { counter, store })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, Self::Error> {
        let name = D::get_name();
        let counter = get_counter(&name);
        let _latency = counter.list_all_latency.measure_latency();
        let namespaces = D::list_all(config).await?;
        let counter = get_counter(&name);
        counter
            .list_all_sizes
            .with_label_values(&[])
            .observe(namespaces.len() as f64);
        Ok(namespaces)
    }

    async fn list_root_keys(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        let _latency = self.counter.list_root_keys_latency.measure_latency();
        self.database.list_root_keys().await
    }

    async fn delete_all(config: &Self::Config) -> Result<(), Self::Error> {
        let name = D::get_name();
        let counter = get_counter(&name);
        let _latency = counter.delete_all_latency.measure_latency();
        D::delete_all(config).await
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, Self::Error> {
        let name = D::get_name();
        let counter = get_counter(&name);
        let _latency = counter.exists_latency.measure_latency();
        let result = D::exists(config, namespace).await?;
        if result {
            let counter = get_counter(&name);
            counter.exists_true_cases.with_label_values(&[]).inc();
        }
        Ok(result)
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        let name = D::get_name();
        let counter = get_counter(&name);
        let _latency = counter.create_latency.measure_latency();
        D::create(config, namespace).await
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        let name = D::get_name();
        let counter = get_counter(&name);
        let _latency = counter.delete_latency.measure_latency();
        D::delete(config, namespace).await
    }
}

#[cfg(with_testing)]
impl<D> TestKeyValueDatabase for MeteredDatabase<D>
where
    D: TestKeyValueDatabase,
{
    async fn new_test_config() -> Result<D::Config, Self::Error> {
        D::new_test_config().await
    }
}
