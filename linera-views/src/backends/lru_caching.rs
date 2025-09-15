// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Add LRU (least recently used) caching to a given store.

use std::{
    collections::{btree_map, hash_map::RandomState, BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};

#[cfg(with_testing)]
use crate::memory::MemoryDatabase;
#[cfg(with_testing)]
use crate::store::TestKeyValueDatabase;
use crate::{
    batch::{Batch, WriteOperation},
    common::get_interval,
    store::{KeyValueDatabase, ReadableKeyValueStore, WithError, WritableKeyValueStore},
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::register_int_counter_vec;
    use prometheus::IntCounterVec;

    /// The total number of cache read value misses
    pub static READ_VALUE_CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_read_value_cache_miss",
            "Number of read value cache misses",
            &[],
        )
    });

    /// The total number of read value cache hits
    pub static READ_VALUE_CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_read_value_cache_hits",
            "Number of read value cache hits",
            &[],
        )
    });

    /// The total number of contains key cache misses
    pub static CONTAINS_KEY_CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_contains_key_cache_miss",
            "Number of contains key cache misses",
            &[],
        )
    });

    /// The total number of contains key cache hits
    pub static CONTAINS_KEY_CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_contains_key_cache_hit",
            "Number of contains key cache hits",
            &[],
        )
    });

    /// The total number of find_keys_by_prefix cache misses
    pub static FIND_KEYS_BY_PREFIX_CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_find_keys_by_prefix_cache_miss",
            "Number of find keys by prefix cache misses",
            &[],
        )
    });

    /// The total number of find_keys_by_prefix cache hits
    pub static FIND_KEYS_BY_PREFIX_CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_find_keys_by_prefix_cache_hit",
            "Number of find keys by prefix cache hits",
            &[],
        )
    });

    /// The total number of find_key_values_by_prefix cache misses
    pub static FIND_KEY_VALUES_BY_PREFIX_CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_find_key_values_by_prefix_cache_miss",
            "Number of find key values by prefix cache misses",
            &[],
        )
    });

    /// The total number of find_key_values_by_prefix cache hits
    pub static FIND_KEY_VALUES_BY_PREFIX_CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_find_key_values_by_prefix_cache_hit",
            "Number of find key values by prefix cache hits",
            &[],
        )
    });
}

/// The parametrization of the cache.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageCacheConfig {
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
pub const DEFAULT_STORAGE_CACHE_CONFIG: StorageCacheConfig = StorageCacheConfig {
    max_cache_size: 10000000,
    max_entry_size: 1000000,
    max_cache_entries: 1000,
};

#[derive(Eq, Hash, PartialEq)]
enum CacheKey {
    Value(Vec<u8>),
    Find(Vec<u8>),
}

enum ValueCacheEntry {
    DoesNotExist,
    Exists,
    Value(Vec<u8>),
}

impl ValueCacheEntry {
    fn size(&self) -> usize {
        match self {
            ValueCacheEntry::Value(vec) => vec.len(),
            _ => 0,
        }
    }
}

enum FindCacheEntry {
    Keys(BTreeSet<Vec<u8>>),
    KeyValues(BTreeMap<Vec<u8>, Vec<u8>>),
}

impl FindCacheEntry {
    fn size(&self) -> usize {
        match self {
            FindCacheEntry::Keys(set) => set.iter().map(|key| key.len()).sum(),
            FindCacheEntry::KeyValues(map) => map
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum(),
        }
    }

    fn get_find_keys(&self, key_prefix: &[u8]) -> Vec<Vec<u8>> {
        let key_prefix = key_prefix.to_vec();
        let delta = key_prefix.len();
        match self {
            FindCacheEntry::Keys(set) => set
                .range(get_interval(key_prefix))
                .map(|key| key[delta..].to_vec())
                .collect(),
            FindCacheEntry::KeyValues(map) => map
                .range(get_interval(key_prefix))
                .map(|(key, _)| key[delta..].to_vec())
                .collect(),
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
                        .collect(),
                )
            }
        }
    }
}

/// Stores the data for simple `read_values` queries.
///
/// This data structure is inspired by the crate `lru-cache` but was modified to support
/// range deletions.
struct LruPrefixCache {
    value_map: BTreeMap<Vec<u8>, ValueCacheEntry>,
    find_map: BTreeMap<Vec<u8>, FindCacheEntry>,
    queue: LinkedHashMap<CacheKey, usize, RandomState>,
    config: StorageCacheConfig,
    total_size: usize,
    total_value_size: usize,
    total_find_size: usize,
    /// Whether we have exclusive R/W access to the keys under the root key of the store.
    has_exclusive_access: bool,
}

impl LruPrefixCache {
    /// Creates an `LruPrefixCache`.
    fn new(config: StorageCacheConfig, has_exclusive_access: bool) -> Self {
        Self {
            value_map: BTreeMap::new(),
            find_map: BTreeMap::new(),
            queue: LinkedHashMap::new(),
            config,
            total_size: 0,
            total_value_size: 0,
            total_find_size: 0,
            has_exclusive_access,
        }
    }

    /// Trim the cache so that it fits within the constraints.
    fn trim_cache(&mut self) {
        // NOTE: The removal of entries might be too aggressive for the
        // total_size / total_value_size / total_find_size
        while self.total_size > self.config.max_cache_size
            || self.queue.len() > self.config.max_cache_entries
        {
            let Some((cache_key, size)) = self.queue.pop_front() else {
                break;
            };
            match cache_key {
                CacheKey::Value(key) => {
                    self.value_map.remove(&key);
                    self.total_size -= size;
                    self.total_value_size -= size;
                },
                CacheKey::Find(key) => {
                    self.find_map.remove(&key);
                    self.total_size -= size;
                    self.total_find_size -= size;
                },
            }
        }
    }

    /// Inserts an entry into the cache.
    fn insert_value(&mut self, key: Vec<u8>, cache_entry: ValueCacheEntry) {
        let size = key.len() + cache_entry.size();
        if (matches!(cache_entry, ValueCacheEntry::DoesNotExist) && !self.has_exclusive_access)
            || size > self.config.max_entry_size
        {
            let cache_key = CacheKey::Value(key);
            // Just forget about the entry.
            if let Some(old_size) = self.queue.remove(&cache_key) {
                self.total_size -= old_size;
                self.total_value_size -= old_size;
                let CacheKey::Value(key) = cache_key else {
                    unreachable!();
                };
                self.value_map.remove(&key);
            };
            return;
        }
        match self.value_map.entry(key.clone()) {
            btree_map::Entry::Occupied(mut entry) => {
                entry.insert(cache_entry);
                // Put it on first position for LRU
                let cache_key = CacheKey::Value(key);
                let old_size = self.queue.remove(&cache_key).expect("old_size");
                self.total_size -= old_size;
                self.total_value_size -= old_size;
                self.queue.insert(cache_key, size);
                self.total_size += size;
                self.total_value_size += size;
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(cache_entry);
                let cache_key = CacheKey::Value(key);
                self.queue.insert(cache_key, size);
                self.total_size += size;
                self.total_value_size += size;
            }
        }
        self.trim_cache();
    }

    /// Inserts a read_value entry into the cache.
    fn insert_read_value(&mut self, key: Vec<u8>, value: &Option<Vec<u8>>) {
        let cache_entry = match value {
            None => ValueCacheEntry::DoesNotExist,
            Some(vec) => ValueCacheEntry::Value(vec.to_vec()),
        };
        self.insert_value(key, cache_entry)
    }

    /// Inserts a read_value entry into the cache.
    fn insert_contains_key(&mut self, key: Vec<u8>, result: bool) {
        let cache_entry = match result {
            false => ValueCacheEntry::DoesNotExist,
            true => ValueCacheEntry::Exists,
        };
        self.insert_value(key, cache_entry)
    }

    /// Marks cached keys that match the prefix as deleted. Importantly, this does not
    /// create new entries in the cache.
    fn delete_prefix(&mut self, key_prefix: &[u8]) {
        if self.has_exclusive_access {
            for (key, value) in self.value_map.range_mut(get_interval(key_prefix.to_vec())) {
                let cache_key = CacheKey::Value(key.to_vec());
                *self.queue.get_mut(&cache_key).unwrap() = key.len();
                self.total_size -= value.size();
                self.total_value_size -= value.size();
                *value = ValueCacheEntry::DoesNotExist;
            }
            let mut prefixes = Vec::new();
            for (prefix, _) in self.find_map.range(get_interval(key_prefix.to_vec())) {
                prefixes.push(prefix.to_vec());
            }
            for prefix in prefixes {
                self.value_map.remove(&prefix);
                let cache_key = CacheKey::Find(prefix);
                let Some(size) = self.queue.remove(&cache_key) else {
                    unreachable!("The key should be in the queue");
                };
                self.total_size -= size;
                self.total_find_size -= size;
            }
        } else {
            // Just forget about the entries.
            let mut keys = Vec::new();
            for (key, _) in self.value_map.range(get_interval(key_prefix.to_vec())) {
                keys.push(key.to_vec());
            }
            for key in keys {
                self.value_map.remove(&key);
                let cache_key = CacheKey::Value(key);
                let Some(size) = self.queue.remove(&cache_key) else {
                    unreachable!("The key should be in the queue");
                };
                self.total_size -= size;
                self.total_value_size -= size;
            }
            // No find keys for shared access
        }
    }

    /// Returns the cached value, or `Some(None)` if the entry does not exist in the
    /// database. If `None` is returned, the entry might exist in the database but is
    /// not in the cache.
    fn query_read_value(&mut self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let result = match self.value_map.get(key) {
            None => None,
            Some(entry) => match entry {
                ValueCacheEntry::DoesNotExist => Some(None),
                ValueCacheEntry::Exists => None,
                ValueCacheEntry::Value(vec) => Some(Some(vec.clone())),
            },
        };
        if result.is_some() {
            let cache_key = CacheKey::Value(key.to_vec());
            // Put back the key on top
            let size = self.queue.remove(&cache_key).expect("size");
            self.queue.insert(cache_key, size);
        }
        result
    }

    /// Returns `Some(true)` or `Some(false)` if we know that the entry does or does not
    /// exist in the database. Returns `None` if that information is not in the cache.
    fn query_contains_key(&mut self, key: &[u8]) -> Option<bool> {
        let result = self
            .value_map
            .get(key)
            .map(|entry| !matches!(entry, ValueCacheEntry::DoesNotExist));
        if result.is_some() {
            let cache_key = CacheKey::Value(key.to_vec());
            // Put back the key on top
            let size = self.queue.remove(&cache_key).expect("size");
            self.queue.insert(cache_key, size);
        }
        result
    }
}

/// A key-value database with added LRU caching.
#[derive(Clone)]
pub struct LruCachingDatabase<D> {
    /// The inner store that is called by the LRU cache one
    database: D,
    /// The configuration.
    config: StorageCacheConfig,
}

/// A key-value store with added LRU caching.
#[derive(Clone)]
pub struct LruCachingStore<S> {
    /// The inner store that is called by the LRU cache one
    store: S,
    /// The LRU cache of values.
    cache: Option<Arc<Mutex<LruPrefixCache>>>,
}

impl<D> WithError for LruCachingDatabase<D>
where
    D: WithError,
{
    type Error = D::Error;
}

impl<S> WithError for LruCachingStore<S>
where
    S: WithError,
{
    type Error = S::Error;
}

impl<K> ReadableKeyValueStore for LruCachingStore<K>
where
    K: ReadableKeyValueStore,
{
    // The LRU cache does not change the underlying store's size limits.
    const MAX_KEY_SIZE: usize = K::MAX_KEY_SIZE;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.read_value_bytes(key).await;
        };
        // First inquiring in the read_value_bytes LRU
        {
            let mut cache = cache.lock().unwrap();
            if let Some(value) = cache.query_read_value(key) {
                #[cfg(with_metrics)]
                metrics::READ_VALUE_CACHE_HIT_COUNT
                    .with_label_values(&[])
                    .inc();
                return Ok(value);
            }
        }
        #[cfg(with_metrics)]
        metrics::READ_VALUE_CACHE_MISS_COUNT
            .with_label_values(&[])
            .inc();
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
            if let Some(value) = cache.query_contains_key(key) {
                #[cfg(with_metrics)]
                metrics::CONTAINS_KEY_CACHE_HIT_COUNT
                    .with_label_values(&[])
                    .inc();
                return Ok(value);
            }
        }
        #[cfg(with_metrics)]
        metrics::CONTAINS_KEY_CACHE_MISS_COUNT
            .with_label_values(&[])
            .inc();
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
                if let Some(value) = cache.query_contains_key(&keys[i]) {
                    #[cfg(with_metrics)]
                    metrics::CONTAINS_KEY_CACHE_HIT_COUNT
                        .with_label_values(&[])
                        .inc();
                    results[i] = value;
                } else {
                    #[cfg(with_metrics)]
                    metrics::CONTAINS_KEY_CACHE_MISS_COUNT
                        .with_label_values(&[])
                        .inc();
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
                    metrics::READ_VALUE_CACHE_HIT_COUNT
                        .with_label_values(&[])
                        .inc();
                    result.push(value);
                } else {
                    #[cfg(with_metrics)]
                    metrics::READ_VALUE_CACHE_MISS_COUNT
                        .with_label_values(&[])
                        .inc();
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
            return self.store.find_keys_by_prefix(key_prefix).await;
        };
        let has_exclusive_access = {
            let cache = cache.lock().unwrap();
            cache.has_exclusive_access
        };
        if !has_exclusive_access {
            return self.store.find_keys_by_prefix(key_prefix).await;
        }
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.find_key_values_by_prefix(key_prefix).await;
        };
        let has_exclusive_access = {
            let cache = cache.lock().unwrap();
            cache.has_exclusive_access
        };
        if !has_exclusive_access {
            return self.store.find_key_values_by_prefix(key_prefix).await;
        }



        
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

impl<K> WritableKeyValueStore for LruCachingStore<K>
where
    K: WritableKeyValueStore,
{
    // The LRU cache does not change the underlying store's size limits.
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
                        let cache_entry = ValueCacheEntry::Value(value.to_vec());
                        cache.insert_value(key.to_vec(), cache_entry);
                    }
                    WriteOperation::Delete { key } => {
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

/// The configuration type for the `LruCachingStore`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LruCachingConfig<C> {
    /// The inner configuration of the `LruCachingStore`.
    pub inner_config: C,
    /// The cache size being used
    pub storage_cache_config: StorageCacheConfig,
}

impl<D> KeyValueDatabase for LruCachingDatabase<D>
where
    D: KeyValueDatabase,
{
    type Config = LruCachingConfig<D::Config>;

    type Store = LruCachingStore<D::Store>;

    fn get_name() -> String {
        format!("lru caching {}", D::get_name())
    }

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, Self::Error> {
        let database = D::connect(&config.inner_config, namespace).await?;
        Ok(LruCachingDatabase {
            database,
            config: config.storage_cache_config.clone(),
        })
    }

    fn open_shared(&self, root_key: &[u8]) -> Result<Self::Store, Self::Error> {
        let store = self.database.open_shared(root_key)?;
        let store = LruCachingStore::new(
            store,
            self.config.clone(),
            /* has_exclusive_access */ false,
        );
        Ok(store)
    }

    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self::Store, Self::Error> {
        let store = self.database.open_exclusive(root_key)?;
        let store = LruCachingStore::new(
            store,
            self.config.clone(),
            /* has_exclusive_access */ true,
        );
        Ok(store)
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, Self::Error> {
        D::list_all(&config.inner_config).await
    }

    async fn list_root_keys(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<Vec<Vec<u8>>, Self::Error> {
        D::list_root_keys(&config.inner_config, namespace).await
    }

    async fn delete_all(config: &Self::Config) -> Result<(), Self::Error> {
        D::delete_all(&config.inner_config).await
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, Self::Error> {
        D::exists(&config.inner_config, namespace).await
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        D::create(&config.inner_config, namespace).await
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        D::delete(&config.inner_config, namespace).await
    }
}

impl<S> LruCachingStore<S> {
    /// Creates a new key-value store that provides LRU caching at top of the given store.
    fn new(store: S, config: StorageCacheConfig, has_exclusive_access: bool) -> Self {
        let cache = {
            if config.max_cache_entries == 0 {
                None
            } else {
                Some(Arc::new(Mutex::new(LruPrefixCache::new(
                    config,
                    has_exclusive_access,
                ))))
            }
        };
        Self { store, cache }
    }
}

/// A memory darabase with caching.
#[cfg(with_testing)]
pub type LruCachingMemoryDatabase = LruCachingDatabase<MemoryDatabase>;

#[cfg(with_testing)]
impl<D> TestKeyValueDatabase for LruCachingDatabase<D>
where
    D: TestKeyValueDatabase,
{
    async fn new_test_config() -> Result<LruCachingConfig<D::Config>, D::Error> {
        let inner_config = D::new_test_config().await?;
        let storage_cache_config = DEFAULT_STORAGE_CACHE_CONFIG;
        Ok(LruCachingConfig {
            inner_config,
            storage_cache_config,
        })
    }
}
