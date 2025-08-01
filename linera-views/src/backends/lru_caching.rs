// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Add LRU (least recently used) caching to a given store.

use std::{
    collections::{btree_map, hash_map::RandomState, BTreeMap},
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

enum CacheEntry {
    DoesNotExist,
    Exists,
    Value(Vec<u8>),
}

impl CacheEntry {
    fn size(&self) -> usize {
        match self {
            CacheEntry::Value(vec) => vec.len(),
            _ => 0,
        }
    }
}

/// Stores the data for simple `read_values` queries.
///
/// This data structure is inspired by the crate `lru-cache` but was modified to support
/// range deletions.
struct LruPrefixCache {
    map: BTreeMap<Vec<u8>, CacheEntry>,
    queue: LinkedHashMap<Vec<u8>, usize, RandomState>,
    config: StorageCacheConfig,
    total_size: usize,
    /// Whether we have exclusive R/W access to the keys under the root key of the store.
    has_exclusive_access: bool,
}

impl LruPrefixCache {
    /// Creates an `LruPrefixCache`.
    pub fn new(config: StorageCacheConfig, has_exclusive_access: bool) -> Self {
        Self {
            map: BTreeMap::new(),
            queue: LinkedHashMap::new(),
            config,
            total_size: 0,
            has_exclusive_access,
        }
    }

    /// Trim the cache so that it fits within the constraints.
    fn trim_cache(&mut self) {
        while self.total_size > self.config.max_cache_size
            || self.queue.len() > self.config.max_cache_entries
        {
            let Some((key, key_value_size)) = self.queue.pop_front() else {
                break;
            };
            self.map.remove(&key);
            self.total_size -= key_value_size;
        }
    }

    /// Inserts an entry into the cache.
    pub fn insert(&mut self, key: Vec<u8>, cache_entry: CacheEntry) {
        let key_value_size = key.len() + cache_entry.size();
        if (matches!(cache_entry, CacheEntry::DoesNotExist) && !self.has_exclusive_access)
            || key_value_size > self.config.max_entry_size
        {
            // Just forget about the entry.
            if let Some(old_key_value_size) = self.queue.remove(&key) {
                self.total_size -= old_key_value_size;
                self.map.remove(&key);
            };
            return;
        }
        match self.map.entry(key.clone()) {
            btree_map::Entry::Occupied(mut entry) => {
                entry.insert(cache_entry);
                // Put it on first position for LRU
                let old_key_value_size = self.queue.remove(&key).expect("old_key_value_size");
                self.total_size -= old_key_value_size;
                self.queue.insert(key, key_value_size);
                self.total_size += key_value_size;
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(cache_entry);
                self.queue.insert(key, key_value_size);
                self.total_size += key_value_size;
            }
        }
        self.trim_cache();
    }

    /// Inserts a read_value entry into the cache.
    pub fn insert_read_value(&mut self, key: Vec<u8>, value: &Option<Vec<u8>>) {
        let cache_entry = match value {
            None => CacheEntry::DoesNotExist,
            Some(vec) => CacheEntry::Value(vec.to_vec()),
        };
        self.insert(key, cache_entry)
    }

    /// Inserts a read_value entry into the cache.
    pub fn insert_contains_key(&mut self, key: Vec<u8>, result: bool) {
        let cache_entry = match result {
            false => CacheEntry::DoesNotExist,
            true => CacheEntry::Exists,
        };
        self.insert(key, cache_entry)
    }

    /// Marks cached keys that match the prefix as deleted. Importantly, this does not
    /// create new entries in the cache.
    pub fn delete_prefix(&mut self, key_prefix: &[u8]) {
        if self.has_exclusive_access {
            for (key, value) in self.map.range_mut(get_interval(key_prefix.to_vec())) {
                *self.queue.get_mut(key).unwrap() = key.len();
                self.total_size -= value.size();
                *value = CacheEntry::DoesNotExist;
            }
        } else {
            // Just forget about the entries.
            let mut keys = Vec::new();
            for (key, _) in self.map.range(get_interval(key_prefix.to_vec())) {
                keys.push(key.to_vec());
            }
            for key in keys {
                self.map.remove(&key);
                let Some(key_value_size) = self.queue.remove(&key) else {
                    unreachable!("The key should be in the queue");
                };
                self.total_size -= key_value_size;
            }
        }
    }

    /// Returns the cached value, or `Some(None)` if the entry does not exist in the
    /// database. If `None` is returned, the entry might exist in the database but is
    /// not in the cache.
    pub fn query_read_value(&mut self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let result = match self.map.get(key) {
            None => None,
            Some(entry) => match entry {
                CacheEntry::DoesNotExist => Some(None),
                CacheEntry::Exists => None,
                CacheEntry::Value(vec) => Some(Some(vec.clone())),
            },
        };
        if result.is_some() {
            // Put back the key on top
            let key_value_size = self.queue.remove(key).expect("key_value_size");
            self.queue.insert(key.to_vec(), key_value_size);
        }
        result
    }

    /// Returns `Some(true)` or `Some(false)` if we know that the entry does or does not
    /// exist in the database. Returns `None` if that information is not in the cache.
    pub fn query_contains_key(&mut self, key: &[u8]) -> Option<bool> {
        let result = self
            .map
            .get(key)
            .map(|entry| !matches!(entry, CacheEntry::DoesNotExist));
        if result.is_some() {
            // Put back the key on top
            let key_value_size = self.queue.remove(key).expect("key_value_size");
            self.queue.insert(key.to_vec(), key_value_size);
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
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
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
                        let cache_entry = CacheEntry::Value(value.to_vec());
                        cache.insert(key.to_vec(), cache_entry);
                    }
                    WriteOperation::Delete { key } => {
                        let cache_entry = CacheEntry::DoesNotExist;
                        cache.insert(key.to_vec(), cache_entry);
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
    pub fn new(store: S, config: StorageCacheConfig, has_exclusive_access: bool) -> Self {
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
