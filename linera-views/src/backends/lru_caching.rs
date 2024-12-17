// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Add LRU (least recently used) caching to a given store.

/// The standard cache size used for tests.
pub const TEST_CACHE_SIZE: usize = 1000;

#[cfg(with_metrics)]
use std::sync::LazyLock;
use std::{
    collections::{btree_map, hash_map::RandomState, BTreeMap},
    sync::{Arc, Mutex},
};

use linked_hash_map::LinkedHashMap;
#[cfg(with_metrics)]
use {linera_base::prometheus_util::register_int_counter_vec, prometheus::IntCounterVec};

use crate::{
    batch::{Batch, WriteOperation},
    common::get_interval,
    store::{AdminKeyValueStore, ReadableKeyValueStore, WithError, WritableKeyValueStore},
};
#[cfg(with_testing)]
use crate::{memory::MemoryStore, store::TestKeyValueStore};

#[cfg(with_metrics)]
/// The total number of cache faults
static NUM_CACHE_FAULT: LazyLock<IntCounterVec> =
    LazyLock::new(|| register_int_counter_vec("num_cache_fault", "Number of cache faults", &[]));

#[cfg(with_metrics)]
/// The total number of cache successes
static NUM_CACHE_SUCCESS: LazyLock<IntCounterVec> =
    LazyLock::new(|| register_int_counter_vec("num_cache_success", "Number of cache success", &[]));

/// The `LruPrefixCache` stores the data for simple `read_values` queries
/// It is inspired by the crate `lru-cache`.
///
/// We cannot apply this crate directly because the batch operation
/// need to update the cache. In the case of `DeletePrefix` we have to
/// handle the keys by prefixes. And so we need to have a BTreeMap to
/// keep track of this.

/// The data structures
struct LruPrefixCache {
    map: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    queue: LinkedHashMap<Vec<u8>, (), RandomState>,
    max_cache_size: usize,
}

impl<'a> LruPrefixCache {
    /// Creates a LruPrefixCache.
    pub fn new(max_cache_size: usize) -> Self {
        Self {
            map: BTreeMap::new(),
            queue: LinkedHashMap::new(),
            max_cache_size,
        }
    }

    /// Inserts an entry into the cache.
    pub fn insert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        match self.map.entry(key.clone()) {
            btree_map::Entry::Occupied(mut entry) => {
                entry.insert(value);
                // Put it on first position for LRU
                self.queue.remove(&key);
                self.queue.insert(key, ());
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
                self.queue.insert(key, ());
                if self.queue.len() > self.max_cache_size {
                    let Some(value) = self.queue.pop_front() else {
                        unreachable!()
                    };
                    self.map.remove(&value.0);
                }
            }
        }
    }

    /// Marks cached keys that match the prefix as deleted. Importantly, this does not create new entries in the cache.
    pub fn delete_prefix(&mut self, key_prefix: &[u8]) {
        for (_, value) in self.map.range_mut(get_interval(key_prefix.to_vec())) {
            *value = None;
        }
    }

    /// Gets the entry from the key.
    pub fn query(&'a self, key: &'a [u8]) -> Option<&'a Option<Vec<u8>>> {
        self.map.get(key)
    }
}

/// We take a store, a maximum size and build a LRU-based system.
#[derive(Clone)]
pub struct LruCachingStore<K> {
    /// The inner store that is called by the LRU cache one
    store: K,
    lru_read_values: Option<Arc<Mutex<LruPrefixCache>>>,
}

impl<K> WithError for LruCachingStore<K>
where
    K: WithError,
{
    type Error = K::Error;
}

impl<K> ReadableKeyValueStore for LruCachingStore<K>
where
    K: ReadableKeyValueStore + Send + Sync,
{
    // The LRU cache does not change the underlying store's size limits.
    const MAX_KEY_SIZE: usize = K::MAX_KEY_SIZE;
    type Keys = K::Keys;
    type KeyValues = K::KeyValues;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let Some(lru_read_values) = &self.lru_read_values else {
            return self.store.read_value_bytes(key).await;
        };
        // First inquiring in the read_value_bytes LRU
        {
            let lru_read_values_container = lru_read_values.lock().unwrap();
            if let Some(value) = lru_read_values_container.query(key) {
                #[cfg(with_metrics)]
                NUM_CACHE_SUCCESS.with_label_values(&[]).inc();
                return Ok(value.clone());
            }
        }
        #[cfg(with_metrics)]
        NUM_CACHE_FAULT.with_label_values(&[]).inc();
        let value = self.store.read_value_bytes(key).await?;
        let mut lru_read_values = lru_read_values.lock().unwrap();
        lru_read_values.insert(key.to_vec(), value.clone());
        Ok(value)
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error> {
        if let Some(values) = &self.lru_read_values {
            let values = values.lock().unwrap();
            if let Some(value) = values.query(key) {
                return Ok(value.is_some());
            }
        }
        self.store.contains_key(key).await
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Self::Error> {
        let Some(values) = &self.lru_read_values else {
            return self.store.contains_keys(keys).await;
        };
        let size = keys.len();
        let mut results = vec![false; size];
        let mut indices = Vec::new();
        let mut key_requests = Vec::new();
        {
            let values = values.lock().unwrap();
            for i in 0..size {
                if let Some(value) = values.query(&keys[i]) {
                    results[i] = value.is_some();
                } else {
                    indices.push(i);
                    key_requests.push(keys[i].clone());
                }
            }
        }
        if !key_requests.is_empty() {
            let key_results = self.store.contains_keys(key_requests).await?;
            for (index, result) in indices.into_iter().zip(key_results) {
                results[index] = result;
            }
        }
        Ok(results)
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let Some(lru_read_values) = &self.lru_read_values else {
            return self.store.read_multi_values_bytes(keys).await;
        };

        let mut result = Vec::with_capacity(keys.len());
        let mut cache_miss_indices = Vec::new();
        let mut miss_keys = Vec::new();
        {
            let lru_read_values_container = lru_read_values.lock().unwrap();
            for (i, key) in keys.into_iter().enumerate() {
                if let Some(value) = lru_read_values_container.query(&key) {
                    #[cfg(with_metrics)]
                    NUM_CACHE_SUCCESS.with_label_values(&[]).inc();
                    result.push(value.clone());
                } else {
                    #[cfg(with_metrics)]
                    NUM_CACHE_FAULT.with_label_values(&[]).inc();
                    result.push(None);
                    cache_miss_indices.push(i);
                    miss_keys.push(key);
                }
            }
        }
        if !miss_keys.is_empty() {
            let values = self
                .store
                .read_multi_values_bytes(miss_keys.clone())
                .await?;
            let mut lru_read_values = lru_read_values.lock().unwrap();
            for (i, (key, value)) in cache_miss_indices
                .into_iter()
                .zip(miss_keys.into_iter().zip(values))
            {
                lru_read_values.insert(key, value.clone());
                result[i] = value;
            }
        }
        Ok(result)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

impl<K> WritableKeyValueStore for LruCachingStore<K>
where
    K: WritableKeyValueStore + Send + Sync,
{
    // The LRU cache does not change the underlying store's size limits.
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        let Some(lru_read_values) = &self.lru_read_values else {
            return self.store.write_batch(batch).await;
        };

        {
            let mut lru_read_values = lru_read_values.lock().unwrap();
            for operation in &batch.operations {
                match operation {
                    WriteOperation::Put { key, value } => {
                        lru_read_values.insert(key.to_vec(), Some(value.to_vec()));
                    }
                    WriteOperation::Delete { key } => {
                        lru_read_values.insert(key.to_vec(), None);
                    }
                    WriteOperation::DeletePrefix { key_prefix } => {
                        lru_read_values.delete_prefix(key_prefix);
                    }
                }
            }
        }
        self.store.write_batch(batch).await
    }

    async fn clear_journal(&self) -> Result<(), Self::Error> {
        self.store.clear_journal().await
    }
}

/// The configuration type for the `LruCachingStore`.
pub struct LruCachingConfig<C> {
    /// The inner configuration of the `LruCachingStore`.
    pub inner_config: C,
    /// The cache size being used
    pub cache_size: usize,
}

impl<K> AdminKeyValueStore for LruCachingStore<K>
where
    K: AdminKeyValueStore + Send + Sync,
{
    type Config = LruCachingConfig<K::Config>;

    fn get_name() -> String {
        format!("lru caching {}", K::get_name())
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, Self::Error> {
        let store = K::connect(&config.inner_config, namespace, root_key).await?;
        let cache_size = config.cache_size;
        Ok(LruCachingStore::new(store, cache_size))
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, Self::Error> {
        let store = self.store.clone_with_root_key(root_key)?;
        let cache_size = self.cache_size();
        Ok(LruCachingStore::new(store, cache_size))
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, Self::Error> {
        K::list_all(&config.inner_config).await
    }

    async fn delete_all(config: &Self::Config) -> Result<(), Self::Error> {
        K::delete_all(&config.inner_config).await
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, Self::Error> {
        K::exists(&config.inner_config, namespace).await
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        K::create(&config.inner_config, namespace).await
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        K::delete(&config.inner_config, namespace).await
    }
}

#[cfg(with_testing)]
impl<K> TestKeyValueStore for LruCachingStore<K>
where
    K: TestKeyValueStore + Send + Sync,
{
    async fn new_test_config() -> Result<LruCachingConfig<K::Config>, K::Error> {
        let inner_config = K::new_test_config().await?;
        let cache_size = TEST_CACHE_SIZE;
        Ok(LruCachingConfig {
            inner_config,
            cache_size,
        })
    }
}

fn new_lru_prefix_cache(cache_size: usize) -> Option<Arc<Mutex<LruPrefixCache>>> {
    if cache_size == 0 {
        None
    } else {
        Some(Arc::new(Mutex::new(LruPrefixCache::new(cache_size))))
    }
}

impl<K> LruCachingStore<K> {
    /// Creates a new key-value store that provides LRU caching at top of the given store.
    pub fn new(store: K, cache_size: usize) -> Self {
        let lru_read_values = new_lru_prefix_cache(cache_size);
        Self {
            store,
            lru_read_values,
        }
    }

    /// Gets the `cache_size`
    pub fn cache_size(&self) -> usize {
        match &self.lru_read_values {
            None => 0,
            Some(lru_read_values) => {
                let lru_read_values = lru_read_values.lock().unwrap();
                lru_read_values.max_cache_size
            }
        }
    }
}

/// A memory store with caching.
#[cfg(with_testing)]
pub type LruCachingMemoryStore = LruCachingStore<MemoryStore>;
