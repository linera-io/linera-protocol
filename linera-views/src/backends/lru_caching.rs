// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Add caching to a given store.
//! The current policy is based on the LRU (Least Recently Used).

use std::{
    collections::{btree_map, hash_map::RandomState, BTreeMap, BTreeSet},
    fs::File,
    io::BufReader,
    sync::{Arc, Mutex},
};

use linked_hash_map::LinkedHashMap;
#[cfg(with_metrics)]
use {linera_base::prometheus_util::register_int_counter_vec, prometheus::IntCounterVec};

use crate::{
    batch::{Batch, WriteOperation},
    common::get_interval,
    store::{
        AdminKeyValueStore, KeyIterable, KeyValueIterable, ReadableKeyValueStore, WithError,
        WritableKeyValueStore,
    },
};
#[cfg(with_testing)]
use crate::{memory::MemoryStore, store::TestKeyValueStore};

mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util;
    use prometheus::{HistogramVec, IntCounterVec};

    /// The total number of value cache faults
    pub static NUM_CACHE_VALUE_FAULT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        prometheus_util::register_int_counter_vec(
            "num_cache_value_fault",
            "Number of value cache faults",
            &[],
        )
    });

    /// The total number of cache successes
    pub static NUM_CACHE_VALUE_SUCCESS: LazyLock<IntCounterVec> = LazyLock::new(|| {
        prometheus_util::register_int_counter_vec(
            "num_cache_value_success",
            "Number of value cache success",
            &[],
        )
    });

    /// The total number of find cache faults
    pub static NUM_CACHE_FIND_FAULT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        prometheus_util::register_int_counter_vec(
            "num_cache_find_fault",
            "Number of find cache faults",
            &[],
        )
    });

    /// The total number of find cache successes
    pub static NUM_CACHE_FIND_SUCCESS: LazyLock<IntCounterVec> = LazyLock::new(|| {
        prometheus_util::register_int_counter_vec(
            "num_cache_find_success",
            "Number of find cache success",
            &[],
        )
    });

    /// Size of the inserted value entry
    pub static VALUE_CACHE_ENTRY_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
        prometheus_util::register_histogram_vec(
            "value_cache_entry_size",
            "Value cache entry size",
            &[],
            Some(vec![
                10.0, 30.0, 100.0, 300.0, 1000.0, 3000.0, 10000.0, 30000.0, 100000.0, 300000.0,
                1000000.0,
            ]),
        )
    });

    /// Size of the inserted find entry
    pub static FIND_CACHE_ENTRY_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
        prometheus_util::register_histogram_vec(
            "find_cache_entry_size",
            "Find cache entry size",
            &[],
            Some(vec![
                10.0, 30.0, 100.0, 300.0, 1000.0, 3000.0, 10000.0, 30000.0, 100000.0, 300000.0,
                1000000.0,
            ]),
        )
    });
}

/// The parametrization of the cache
#[derive(Clone, Debug, serde::Deserialize)]
pub struct StorageCachePolicy {
    /// The maximum size of the cache, in bytes (keys size + value sizes)
    pub max_cache_size: usize,
    /// The maximum size of an entry size, in bytes
    pub max_entry_size: usize,
    /// The maximum number of entries in the cache.
    pub max_cache_entries: usize,
}

/// The maximum number of entries in the cache.
/// If the number of entries in the cache is too large then the underlying maps
/// become the limiting factor
pub const DEFAULT_STORAGE_CACHE_POLICY: StorageCachePolicy = StorageCachePolicy {
    max_cache_size: 10000000,
    max_entry_size: 1000000,
    max_cache_entries: 1000,
};

/// Read the `StorageCachePolicy` from the existing file and if None, then the default
/// storage policy is returned
pub fn read_storage_cache_policy(storage_cache_policy: Option<String>) -> StorageCachePolicy {
    match storage_cache_policy {
        None => DEFAULT_STORAGE_CACHE_POLICY,
        Some(storage_cache_policy) => {
            let file = File::open(storage_cache_policy)
                .expect("File {storage_cache_policy} does not exist");
            let reader = BufReader::new(file);
            serde_json::from_reader(reader).expect("The parsing of the StorageCachePolicy failed")
        }
    }
}

#[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
enum CacheEntry {
    Find,
    Value,
}

enum ValueCacheEntry {
    DoesNotExist,
    Exists,
    Value(Vec<u8>),
}

impl ValueCacheEntry {
    fn from_contains_key(test: bool) -> ValueCacheEntry {
        match test {
            false => ValueCacheEntry::DoesNotExist,
            true => ValueCacheEntry::Exists,
        }
    }

    fn from_read_value(result: &Option<Vec<u8>>) -> ValueCacheEntry {
        match result {
            None => ValueCacheEntry::DoesNotExist,
            Some(vec) => ValueCacheEntry::Value(vec.to_vec()),
        }
    }

    fn get_contains_key(&self) -> bool {
        match self {
            ValueCacheEntry::DoesNotExist => false,
            ValueCacheEntry::Exists => true,
            ValueCacheEntry::Value(_) => true,
        }
    }

    fn get_read_value(&self) -> Option<Option<Vec<u8>>> {
        match self {
            ValueCacheEntry::DoesNotExist => Some(None),
            ValueCacheEntry::Exists => None,
            ValueCacheEntry::Value(vec) => Some(Some(vec.clone())),
        }
    }

    fn size(&self) -> usize {
        match self {
            ValueCacheEntry::DoesNotExist => 0,
            ValueCacheEntry::Exists => 0,
            ValueCacheEntry::Value(vec) => vec.len(),
        }
    }
}

enum FindCacheEntry {
    Keys(BTreeSet<Vec<u8>>),
    KeyValues(BTreeMap<Vec<u8>, Vec<u8>>),
}

impl FindCacheEntry {
    fn get_find_keys(&self, key_prefix: &[u8]) -> Vec<Vec<u8>> {
        let key_prefix = key_prefix.to_vec();
        let delta = key_prefix.len();
        match self {
            FindCacheEntry::Keys(map) => map
                .range(get_interval(key_prefix))
                .map(|key| key[delta..].to_vec())
                .collect::<Vec<_>>(),
            FindCacheEntry::KeyValues(map) => map
                .range(get_interval(key_prefix))
                .map(|(key, _)| key[delta..].to_vec())
                .collect::<Vec<_>>(),
        }
    }

    fn get_find_key_values(&self, key_prefix: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            FindCacheEntry::Keys(_) => None,
            FindCacheEntry::KeyValues(map) => {
                let key_prefix = key_prefix.to_vec();
                let delta = key_prefix.len();
                Some(
                    map.range(get_interval(key_prefix))
                        .map(|(key, value)| (key[delta..].to_vec(), value.to_vec()))
                        .collect::<Vec<_>>(),
                )
            }
        }
    }

    fn get_contains_key(&self, key: &[u8]) -> bool {
        match self {
            FindCacheEntry::Keys(map) => map.contains(key),
            FindCacheEntry::KeyValues(map) => map.contains_key(key),
        }
    }

    fn get_read_value(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        match self {
            FindCacheEntry::Keys(_map) => None,
            FindCacheEntry::KeyValues(map) => Some(map.get(key).cloned()),
        }
    }

    fn update_cache_entry(&mut self, key: &[u8], new_value: Option<Vec<u8>>) {
        match self {
            FindCacheEntry::Keys(map) => {
                match new_value {
                    None => map.remove(key),
                    Some(_) => map.insert(key.to_vec()),
                };
            }
            FindCacheEntry::KeyValues(map) => {
                match new_value {
                    None => map.remove(key),
                    Some(new_value) => map.insert(key.to_vec(), new_value),
                };
            }
        }
    }

    fn delete_prefix(&mut self, key_prefix: &[u8]) {
        match self {
            FindCacheEntry::Keys(map) => {
                let keys = map
                    .range(get_interval(key_prefix.to_vec()))
                    .cloned()
                    .collect::<Vec<_>>();
                for key in keys {
                    map.remove(&key);
                }
            }
            FindCacheEntry::KeyValues(map) => {
                let keys = map
                    .range(get_interval(key_prefix.to_vec()))
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                for key in keys {
                    map.remove(&key);
                }
            }
        }
    }

    fn size(&self) -> usize {
        let mut total_size = 0;
        match self {
            FindCacheEntry::Keys(map) => {
                for key in map {
                    total_size += key.len();
                }
            }
            FindCacheEntry::KeyValues(map) => {
                for (key, value) in map {
                    total_size += key.len() + value.len();
                }
            }
        }
        total_size
    }
}

/// The `StoragePrefixCache` stores the data for simple `read_values` queries
/// It is inspired by the crate `lru-cache`.
///
/// We cannot apply this crate directly because the batch operation
/// need to update the cache. In the case of `DeletePrefix` we have to
/// handle the keys by prefixes. And so we need to have a BTreeMap to
/// keep track of this.

/// The data structures
#[allow(clippy::type_complexity)]
struct StoragePrefixCache {
    map_find: BTreeMap<Vec<u8>, FindCacheEntry>,
    map_value: BTreeMap<Vec<u8>, ValueCacheEntry>,
    queue: LinkedHashMap<(Vec<u8>, CacheEntry), usize, RandomState>,
    storage_cache_policy: StorageCachePolicy,
    total_size: usize,
}

impl StoragePrefixCache {
    /// Creates a LruPrefixCache.
    pub fn new(storage_cache_policy: StorageCachePolicy) -> Self {
        Self {
            map_find: BTreeMap::new(),
            map_value: BTreeMap::new(),
            queue: LinkedHashMap::new(),
            storage_cache_policy,
            total_size: 0,
        }
    }

    fn get_lower_bound(&self, key: &[u8]) -> Option<(&Vec<u8>, &FindCacheEntry)> {
        match self.map_find.range(..=key.to_vec()).next_back() {
            None => None,
            Some((key_store, value)) => {
                if key.starts_with(key_store) {
                    Some((key_store, value))
                } else {
                    None
                }
            }
        }
    }

    fn get_lower_bound_update(&mut self, key: &[u8]) -> Option<(&Vec<u8>, &mut FindCacheEntry)> {
        match self.map_find.range_mut(..=key.to_vec()).next_back() {
            None => None,
            Some((key_store, value)) => {
                if key.starts_with(key_store) {
                    Some((key_store, value))
                } else {
                    None
                }
            }
        }
    }

    /// Inserts a new entry, clearing entries of the queue if needed
    fn insert_queue(&mut self, full_key: (Vec<u8>, CacheEntry), cache_size: usize) {
        self.queue.insert(full_key, cache_size);
        self.total_size += cache_size;
        while self.total_size > self.storage_cache_policy.max_cache_size
            || self.queue.len() > self.storage_cache_policy.max_cache_entries
        {
            let Some(value) = self.queue.pop_front() else {
                break;
            };
            match value.0 {
                (v, CacheEntry::Find) => {
                    self.map_find.remove(&v);
                }
                (v, CacheEntry::Value) => {
                    self.map_value.remove(&v);
                }
            }
            self.total_size -= value.1;
        }
    }

    /// Removes an entry from the queue
    fn remove_from_queue(&mut self, full_key: &(Vec<u8>, CacheEntry)) {
        let existing_cache_size = self.queue.remove(full_key).unwrap();
        self.total_size -= existing_cache_size;
    }

    /// Sets a size to zero in the queue
    fn zero_set_entry_in_queue(&mut self, full_key: &(Vec<u8>, CacheEntry)) {
        let len = full_key.0.len();
        let cache_size = self.queue.get_mut(full_key).unwrap();
        self.total_size -= *cache_size;
        *cache_size = len;
        self.total_size += *cache_size;
    }

    /// Inserts a value entry into the cache.
    pub fn insert_value(&mut self, key: Vec<u8>, cache_entry: ValueCacheEntry) {
        let cache_size = cache_entry.size() + key.len();
        #[cfg(with_metrics)]
        metrics::VALUE_CACHE_ENTRY_SIZE
            .with_label_values(&[])
            .observe(cache_size as f64);
        if cache_size > self.storage_cache_policy.max_entry_size {
            return;
        }
        let full_key = (key.clone(), CacheEntry::Value);
        match self.map_value.entry(key) {
            btree_map::Entry::Occupied(mut entry) => {
                entry.insert(cache_entry);
                self.remove_from_queue(&full_key);
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(cache_entry);
            }
        }
        self.insert_queue(full_key, cache_size);
    }

    /// Invalidates corresponding find entries
    pub fn correct_find_entry(&mut self, key: &[u8], new_value: Option<Vec<u8>>) {
        let lower_bound = self.get_lower_bound_update(key);
        if let Some((lower_bound, cache_entry)) = lower_bound {
            let key_red = &key[lower_bound.len()..];
            cache_entry.update_cache_entry(key_red, new_value);
        }
    }

    /// Inserts a read_value entry into the cache.
    pub fn insert_read_value(&mut self, key: Vec<u8>, value: &Option<Vec<u8>>) {
        let cache_entry = ValueCacheEntry::from_read_value(value);
        self.insert_value(key, cache_entry)
    }

    /// Inserts a read_value entry into the cache.
    pub fn insert_contains_key(&mut self, key: Vec<u8>, result: bool) {
        let cache_entry = ValueCacheEntry::from_contains_key(result);
        self.insert_value(key, cache_entry)
    }

    /// Inserts a find entry into the cache.
    pub fn insert_find(&mut self, key_prefix: Vec<u8>, cache_entry: FindCacheEntry) {
        let cache_size = cache_entry.size() + key_prefix.len();
        #[cfg(with_metrics)]
        metrics::FIND_CACHE_ENTRY_SIZE
            .with_label_values(&[])
            .observe(cache_size as f64);
        if cache_size > self.storage_cache_policy.max_cache_size {
            // Inserting that entry would lead to complete clearing of the cache
            // which is counter productive
            return;
        }
        let keys = self
            .map_find
            .range(get_interval(key_prefix.clone()))
            .map(|(x, _)| x.clone())
            .collect::<Vec<_>>();
        for key in keys {
            self.map_find.remove(&key);
            let full_key = (key, CacheEntry::Find);
            self.remove_from_queue(&full_key);
        }
        if let FindCacheEntry::KeyValues(_) = cache_entry {
            let keys = self
                .map_value
                .range(get_interval(key_prefix.clone()))
                .map(|(x, _)| x.clone())
                .collect::<Vec<_>>();
            for key in keys {
                self.map_value.remove(&key);
                let full_key = (key, CacheEntry::Value);
                self.remove_from_queue(&full_key);
            }
        }
        let full_prefix = (key_prefix.clone(), CacheEntry::Find);
        match self.map_find.entry(key_prefix.clone()) {
            btree_map::Entry::Occupied(mut entry) => {
                entry.insert(cache_entry);
                self.remove_from_queue(&full_prefix);
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(cache_entry);
            }
        }
        self.insert_queue(full_prefix, cache_size);
    }

    /// Inserts a find entry into the cache.
    pub fn insert_find_keys(&mut self, key_prefix: Vec<u8>, value: &[Vec<u8>]) {
        let map = value.iter().cloned().collect::<BTreeSet<_>>();
        let cache_entry = FindCacheEntry::Keys(map);
        self.insert_find(key_prefix, cache_entry);
    }

    /// Inserts a find_key_values entry into the cache.
    pub fn insert_find_key_values(&mut self, key_prefix: Vec<u8>, value: &[(Vec<u8>, Vec<u8>)]) {
        let map = value.iter().cloned().collect::<BTreeMap<_, _>>();
        let cache_entry = FindCacheEntry::KeyValues(map);
        self.insert_find(key_prefix, cache_entry);
    }

    /// Marks cached keys that match the prefix as deleted. Importantly, this does not create new entries in the cache.
    pub fn delete_prefix(&mut self, key_prefix: &[u8]) {
        let mut cache_entries = Vec::new();
        for (key, value) in self.map_value.range_mut(get_interval(key_prefix.to_vec())) {
            *value = ValueCacheEntry::DoesNotExist;
            cache_entries.push((key.clone(), CacheEntry::Value));
        }
        for (key, value) in self.map_find.range_mut(get_interval(key_prefix.to_vec())) {
            *value = FindCacheEntry::KeyValues(BTreeMap::new());
            cache_entries.push((key.clone(), CacheEntry::Find));
        }
        for cache_entry in cache_entries {
            self.zero_set_entry_in_queue(&cache_entry);
        }
        let lower_bound = self.get_lower_bound_update(key_prefix);
        let result = if let Some((lower_bound, cache_entry)) = lower_bound {
            let key_prefix_red = &key_prefix[lower_bound.len()..];
            cache_entry.delete_prefix(key_prefix_red);
            let new_cache_size = cache_entry.size() + key_prefix.len();
            Some((new_cache_size, lower_bound.clone()))
        } else {
            None
        };
        if let Some((new_cache_size, lower_bound)) = result {
            let full_prefix = (lower_bound.clone(), CacheEntry::Find);
            let existing_cache_size = self.queue.get_mut(&full_prefix).unwrap();
            self.total_size -= *existing_cache_size;
            *existing_cache_size = new_cache_size;
            self.total_size += new_cache_size;
        }
    }

    /// Gets the read_value entry from the key.
    pub fn query_read_value(&mut self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let result = match self.map_value.get(key) {
            None => None,
            Some(entry) => entry.get_read_value(),
        };
        if let Some(result) = result {
            let full_key = (key.to_vec(), CacheEntry::Value);
            let cache_size = self.queue.remove(&full_key).unwrap();
            self.queue.insert(full_key, cache_size);
            return Some(result);
        }
        let lower_bound = self.get_lower_bound(key);
        let (lower_bound, result) = if let Some((lower_bound, cache_entry)) = lower_bound {
            let key_red = &key[lower_bound.len()..];
            (Some(lower_bound), cache_entry.get_read_value(key_red))
        } else {
            (None, None)
        };
        if result.is_some() {
            if let Some(lower_bound) = lower_bound {
                let full_key = (lower_bound.clone(), CacheEntry::Find);
                let cache_size = self.queue.remove(&full_key).unwrap();
                self.queue.insert(full_key, cache_size);
            }
        }
        result
    }

    /// Gets the contains_key entry from the key.
    pub fn query_contains_key(&mut self, key: &[u8]) -> Option<bool> {
        let result = self
            .map_value
            .get(key)
            .map(|entry| entry.get_contains_key());
        if let Some(result) = result {
            let full_key = (key.to_vec(), CacheEntry::Value);
            let cache_size = self.queue.remove(&full_key).unwrap();
            self.queue.insert(full_key, cache_size);
            return Some(result);
        }
        let lower_bound = self.get_lower_bound(key);
        let (lower_bound, result) = if let Some((lower_bound, cache_entry)) = lower_bound {
            let key_red = &key[lower_bound.len()..];
            (
                Some(lower_bound),
                Some(cache_entry.get_contains_key(key_red)),
            )
        } else {
            (None, None)
        };
        if result.is_some() {
            if let Some(lower_bound) = lower_bound {
                let full_key = (lower_bound.clone(), CacheEntry::Find);
                let cache_size = self.queue.remove(&full_key).unwrap();
                self.queue.insert(full_key, cache_size);
            }
        }
        result
    }

    /// Gets the find_keys entry from the key prefix
    pub fn query_find_keys(&mut self, key_prefix: &[u8]) -> Option<Vec<Vec<u8>>> {
        let (lower_bound, result) = match self.get_lower_bound(key_prefix) {
            None => (None, None),
            Some((lower_bound, cache_entry)) => {
                let key_prefix_red = &key_prefix[lower_bound.len()..];
                (
                    Some(lower_bound),
                    Some(cache_entry.get_find_keys(key_prefix_red)),
                )
            }
        };
        if let Some(lower_bound) = lower_bound {
            let full_key = (lower_bound.clone(), CacheEntry::Find);
            let cache_size = self.queue.remove(&full_key).unwrap();
            self.queue.insert(full_key, cache_size);
        }
        result
    }

    /// Gets the find key values entry from the key prefix
    pub fn query_find_key_values(&mut self, key_prefix: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let (lower_bound, result) = match self.get_lower_bound(key_prefix) {
            None => (None, None),
            Some((lower_bound, cache_entry)) => {
                let key_prefix_red = &key_prefix[lower_bound.len()..];
                (
                    Some(lower_bound),
                    cache_entry.get_find_key_values(key_prefix_red),
                )
            }
        };
        if result.is_some() {
            if let Some(lower_bound) = lower_bound {
                let full_key = (lower_bound.clone(), CacheEntry::Find);
                let cache_size = self.queue.remove(&full_key).unwrap();
                self.queue.insert(full_key, cache_size);
            }
        }
        result
    }
}

/// We take a store, a maximum size and build a cache based system.
#[derive(Clone)]
pub struct CachingStore<K> {
    /// The inner store that is called by the LRU cache one
    store: K,
    cache: Option<Arc<Mutex<StoragePrefixCache>>>,
}

impl<K> WithError for CachingStore<K>
where
    K: WithError,
{
    type Error = K::Error;
}

impl<K> ReadableKeyValueStore for CachingStore<K>
where
    K: ReadableKeyValueStore + Send + Sync,
{
    // The cache does not change the underlying store's size limits.
    const MAX_KEY_SIZE: usize = K::MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.read_value_bytes(key).await;
        };
        // First inquiring in the read_value_bytes cache
        {
            let mut cache = cache.lock().unwrap();
            if let Some(value) = cache.query_read_value(key) {
                #[cfg(with_metrics)]
                metrics::NUM_CACHE_VALUE_SUCCESS
                    .with_label_values(&[])
                    .inc();
                return Ok(value);
            }
        }
        #[cfg(with_metrics)]
        metrics::NUM_CACHE_VALUE_FAULT.with_label_values(&[]).inc();
        let value = self.store.read_value_bytes(key).await?;
        let mut cache = cache.lock().unwrap();
        cache.insert_read_value(key.to_vec(), &value);
        Ok(value)
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.contains_key(key).await;
        };
        {
            let mut cache = cache.lock().unwrap();
            if let Some(result) = cache.query_contains_key(key) {
                #[cfg(with_metrics)]
                metrics::NUM_CACHE_VALUE_SUCCESS
                    .with_label_values(&[])
                    .inc();
                return Ok(result);
            }
        }
        #[cfg(with_metrics)]
        metrics::NUM_CACHE_VALUE_FAULT.with_label_values(&[]).inc();
        let result = self.store.contains_key(key).await?;
        let mut cache = cache.lock().unwrap();
        cache.insert_contains_key(key.to_vec(), result);
        Ok(result)
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.contains_keys(keys).await;
        };
        let size = keys.len();
        let mut results = vec![false; size];
        let mut indices = Vec::new();
        let mut key_requests = Vec::new();
        {
            let mut cache = cache.lock().unwrap();
            for i in 0..size {
                if let Some(result) = cache.query_contains_key(&keys[i]) {
                    #[cfg(with_metrics)]
                    metrics::NUM_CACHE_VALUE_SUCCESS
                        .with_label_values(&[])
                        .inc();
                    results[i] = result;
                } else {
                    #[cfg(with_metrics)]
                    metrics::NUM_CACHE_VALUE_FAULT.with_label_values(&[]).inc();
                    indices.push(i);
                    key_requests.push(keys[i].clone());
                }
            }
        }
        if !key_requests.is_empty() {
            let key_results = self.store.contains_keys(key_requests.clone()).await?;
            let mut cache = cache.lock().unwrap();
            for ((index, result), key) in indices.into_iter().zip(key_results).zip(key_requests) {
                results[index] = result;
                cache.insert_contains_key(key, result);
            }
        }
        Ok(results)
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.read_multi_values_bytes(keys).await;
        };

        let mut result = Vec::with_capacity(keys.len());
        let mut cache_miss_indices = Vec::new();
        let mut miss_keys = Vec::new();
        {
            let mut cache = cache.lock().unwrap();
            for (i, key) in keys.into_iter().enumerate() {
                if let Some(value) = cache.query_read_value(&key) {
                    #[cfg(with_metrics)]
                    metrics::NUM_CACHE_VALUE_SUCCESS
                        .with_label_values(&[])
                        .inc();
                    result.push(value);
                } else {
                    #[cfg(with_metrics)]
                    metrics::NUM_CACHE_VALUE_FAULT.with_label_values(&[]).inc();
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
            let mut cache = cache.lock().unwrap();
            for (i, (key, value)) in cache_miss_indices
                .into_iter()
                .zip(miss_keys.into_iter().zip(values))
            {
                cache.insert_read_value(key, &value);
                result[i] = value;
            }
        }
        Ok(result)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.uncached_find_keys_by_prefix(key_prefix).await;
        };
        {
            let mut cache = cache.lock().unwrap();
            if let Some(value) = cache.query_find_keys(key_prefix) {
                #[cfg(with_metrics)]
                metrics::NUM_CACHE_FIND_SUCCESS.with_label_values(&[]).inc();
                return Ok(value);
            }
        }
        #[cfg(with_metrics)]
        metrics::NUM_CACHE_FIND_FAULT.with_label_values(&[]).inc();
        let keys = self.uncached_find_keys_by_prefix(key_prefix).await?;
        let mut cache = cache.lock().unwrap();
        cache.insert_find_keys(key_prefix.to_vec(), &keys);
        Ok(keys)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.uncached_find_key_values_by_prefix(key_prefix).await;
        };
        {
            let mut cache = cache.lock().unwrap();
            if let Some(value) = cache.query_find_key_values(key_prefix) {
                #[cfg(with_metrics)]
                metrics::NUM_CACHE_FIND_SUCCESS.with_label_values(&[]).inc();
                return Ok(value);
            }
        }
        #[cfg(with_metrics)]
        metrics::NUM_CACHE_FIND_FAULT.with_label_values(&[]).inc();
        let key_values = self.uncached_find_key_values_by_prefix(key_prefix).await?;
        let mut cache = cache.lock().unwrap();
        cache.insert_find_key_values(key_prefix.to_vec(), &key_values);
        Ok(key_values)
    }
}

impl<K> WritableKeyValueStore for CachingStore<K>
where
    K: WritableKeyValueStore + Send + Sync,
{
    // The cache does not change the underlying store's size limits.
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.write_batch(batch).await;
        };
        {
            let mut cache = cache.lock().unwrap();
            for operation in &batch.operations {
                match operation {
                    WriteOperation::Put { key, value } => {
                        cache.correct_find_entry(key, Some(value.to_vec()));
                        let cache_entry = ValueCacheEntry::Value(value.to_vec());
                        cache.insert_value(key.to_vec(), cache_entry);
                    }
                    WriteOperation::Delete { key } => {
                        cache.correct_find_entry(key, None);
                        let cache_entry = ValueCacheEntry::DoesNotExist;
                        cache.insert_value(key.to_vec(), cache_entry);
                    }
                    WriteOperation::DeletePrefix { key_prefix } => {
                        cache.delete_prefix(key_prefix);
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

/// The configuration type for the `CachingStore`.
pub struct CachingConfig<C> {
    /// The inner configuration of the `CachingStore`.
    pub inner_config: C,
    /// The cache size being used
    pub storage_cache_policy: StorageCachePolicy,
}

impl<K> AdminKeyValueStore for CachingStore<K>
where
    K: AdminKeyValueStore + Send + Sync,
{
    type Config = CachingConfig<K::Config>;

    fn get_name() -> String {
        format!("caching {}", K::get_name())
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, Self::Error> {
        let store = K::connect(&config.inner_config, namespace, root_key).await?;
        let storage_cache_policy = config.storage_cache_policy.clone();
        Ok(CachingStore::new(store, storage_cache_policy))
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, Self::Error> {
        let store = self.store.clone_with_root_key(root_key)?;
        let storage_cache_policy = self.storage_cache_policy();
        Ok(CachingStore::new(store, storage_cache_policy))
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
impl<K> TestKeyValueStore for CachingStore<K>
where
    K: TestKeyValueStore + Send + Sync,
{
    async fn new_test_config() -> Result<CachingConfig<K::Config>, K::Error> {
        let inner_config = K::new_test_config().await?;
        let storage_cache_policy = DEFAULT_STORAGE_CACHE_POLICY;
        Ok(CachingConfig {
            inner_config,
            storage_cache_policy,
        })
    }
}

fn new_storage_prefix_cache(
    storage_cache_policy: StorageCachePolicy,
) -> Option<Arc<Mutex<StoragePrefixCache>>> {
    if storage_cache_policy.max_cache_size == 0 {
        None
    } else {
        let cache = StoragePrefixCache::new(storage_cache_policy);
        Some(Arc::new(Mutex::new(cache)))
    }
}

impl<K> CachingStore<K> {
    /// Creates a new key-value store that provides caching at top of the given store.
    pub fn new(store: K, storage_cache_policy: StorageCachePolicy) -> Self {
        let cache = new_storage_prefix_cache(storage_cache_policy);
        Self { store, cache }
    }

    /// Gets the `storage_cache_policy` or if absent one that matches to an empty cache.
    pub fn storage_cache_policy(&self) -> StorageCachePolicy {
        match &self.cache {
            None => StorageCachePolicy {
                max_cache_size: 0,
                max_entry_size: 0,
                max_cache_entries: 0,
            },
            Some(cache) => {
                let cache = cache.lock().unwrap();
                cache.storage_cache_policy.clone()
            }
        }
    }
}

impl<K> CachingStore<K>
where
    K: ReadableKeyValueStore + WithError,
{
    async fn uncached_find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, K::Error> {
        let keys = self.store.find_keys_by_prefix(key_prefix).await?;
        let mut keys_vec = Vec::new();
        for key in keys.iterator() {
            keys_vec.push(key?.to_vec());
        }
        Ok(keys_vec)
    }

    async fn uncached_find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, K::Error> {
        let key_values = self.store.find_key_values_by_prefix(key_prefix).await?;
        let mut key_values_vec = Vec::new();
        for key_value in key_values.iterator() {
            let key_value = key_value?;
            let key_value = (key_value.0.to_vec(), key_value.1.to_vec());
            key_values_vec.push(key_value);
        }
        Ok(key_values_vec)
    }
}

/// A memory store with caching.
#[cfg(with_testing)]
pub type CachingMemoryStore = CachingStore<MemoryStore>;
