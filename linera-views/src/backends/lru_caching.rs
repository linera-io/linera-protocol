// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Add LRU (least recently used) caching to a given store.

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

#[cfg(with_testing)]
use crate::memory::MemoryDatabase;
#[cfg(with_testing)]
use crate::store::TestKeyValueDatabase;
use crate::{
    batch::{Batch, WriteOperation},
    lru_prefix_cache::{LruPrefixCache, StorageCacheConfig},
    store::{KeyValueDatabase, ReadableKeyValueStore, WithError, WritableKeyValueStore},
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::register_int_counter_vec;
    use prometheus::IntCounterVec;

    /// The total number of cache read value misses.
    pub static READ_VALUE_CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_read_value_cache_miss",
            "Number of read value cache misses",
            &[],
        )
    });

    /// The total number of read value cache hits.
    pub static READ_VALUE_CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_read_value_cache_hits",
            "Number of read value cache hits",
            &[],
        )
    });

    /// The total number of contains key cache misses.
    pub static CONTAINS_KEY_CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_contains_key_cache_miss",
            "Number of contains key cache misses",
            &[],
        )
    });

    /// The total number of contains key cache hits.
    pub static CONTAINS_KEY_CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_contains_key_cache_hit",
            "Number of contains key cache hits",
            &[],
        )
    });

    /// The total number of find_keys_by_prefix cache misses.
    pub static FIND_KEYS_BY_PREFIX_CACHE_MISS_COUNT: LazyLock<IntCounterVec> =
        LazyLock::new(|| {
            register_int_counter_vec(
                "num_find_keys_by_prefix_cache_miss",
                "Number of find keys by prefix cache misses",
                &[],
            )
        });

    /// The total number of find_keys_by_prefix cache hits.
    pub static FIND_KEYS_BY_PREFIX_CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_find_keys_by_prefix_cache_hit",
            "Number of find keys by prefix cache hits",
            &[],
        )
    });

    /// The total number of find_key_values_by_prefix cache misses.
    pub static FIND_KEY_VALUES_BY_PREFIX_CACHE_MISS_COUNT: LazyLock<IntCounterVec> =
        LazyLock::new(|| {
            register_int_counter_vec(
                "num_find_key_values_by_prefix_cache_miss",
                "Number of find key values by prefix cache misses",
                &[],
            )
        });

    /// The total number of find_key_values_by_prefix cache hits.
    pub static FIND_KEY_VALUES_BY_PREFIX_CACHE_HIT_COUNT: LazyLock<IntCounterVec> =
        LazyLock::new(|| {
            register_int_counter_vec(
                "num_find_key_values_by_prefix_cache_hit",
                "Number of find key values by prefix cache hits",
                &[],
            )
        });

    /// The total number of `Put` operations reaching the underlying store.
    pub static WRITE_BATCH_PUT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_write_batch_puts",
            "Number of put operations written through the LRU caching store",
            &[],
        )
    });

    /// The total value bytes of `Put` operations reaching the underlying store.
    pub static WRITE_BATCH_PUT_BYTES: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "write_batch_put_bytes",
            "Value bytes of put operations written through the LRU caching store",
            &[],
        )
    });

    /// The number of `Put` operations whose value is already known to be in the
    /// underlying store, and which are therefore safe to elide.
    pub static WRITE_BATCH_REDUNDANT_PUT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_write_batch_redundant_puts",
            "Number of put operations whose value already matches the cached value",
            &[],
        )
    });

    /// The value bytes of `Put` operations that are safe to elide.
    pub static WRITE_BATCH_REDUNDANT_PUT_BYTES: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "write_batch_redundant_put_bytes",
            "Value bytes of put operations whose value already matches the cached value",
            &[],
        )
    });
}

/// The maximum number of entries in the cache.
/// If the number of entries in the cache is too large then the underlying maps
/// become the limiting factor.
pub const DEFAULT_STORAGE_CACHE_CONFIG: StorageCacheConfig = StorageCacheConfig {
    max_cache_size: 10000000,
    max_value_entry_size: 1000000,
    max_find_keys_entry_size: 1000000,
    max_find_key_values_entry_size: 1000000,
    max_cache_entries: 1000,
    max_cache_value_size: 10000000,
    max_cache_find_keys_size: 10000000,
    max_cache_find_key_values_size: 10000000,
};

/// How many `Put` operations a batch carried, and how many of those the cache already knows
/// are in the underlying store.
#[cfg(with_metrics)]
#[derive(Debug, Default, PartialEq, Eq)]
struct RedundantPutStats {
    put_count: u64,
    put_bytes: u64,
    redundant_count: u64,
    redundant_bytes: u64,
}

#[cfg(with_metrics)]
fn observe_redundant_puts(cache: &mut LruPrefixCache, batch: &Batch) {
    let stats = count_redundant_puts(cache, batch);
    metrics::WRITE_BATCH_PUT_COUNT
        .with_label_values(&[])
        .inc_by(stats.put_count);
    metrics::WRITE_BATCH_PUT_BYTES
        .with_label_values(&[])
        .inc_by(stats.put_bytes);
    metrics::WRITE_BATCH_REDUNDANT_PUT_COUNT
        .with_label_values(&[])
        .inc_by(stats.redundant_count);
    metrics::WRITE_BATCH_REDUNDANT_PUT_BYTES
        .with_label_values(&[])
        .inc_by(stats.redundant_bytes);
}

/// Counts the `Put` operations of `batch` whose value the cache already knows to be in the
/// underlying store, and which could therefore be elided.
///
/// A `Put` only counts when nothing earlier in the same batch shadows its key: an earlier
/// `Delete`, an earlier `DeletePrefix` covering it, or an earlier `Put` to the same key (whose
/// value the cache does not yet reflect). Ambiguous cases are left uncounted, so the result is
/// a lower bound on what a write-eliding implementation could skip.
#[cfg(with_metrics)]
fn count_redundant_puts(cache: &mut LruPrefixCache, batch: &Batch) -> RedundantPutStats {
    let mut put_count = 0u64;
    let mut put_bytes = 0u64;
    let mut redundant_count = 0u64;
    let mut redundant_bytes = 0u64;
    let mut deleted_prefixes: Vec<&[u8]> = Vec::new();
    let mut shadowed_keys: std::collections::BTreeSet<&[u8]> = std::collections::BTreeSet::new();
    for operation in &batch.operations {
        match operation {
            WriteOperation::Put { key, value } => {
                put_count += 1;
                put_bytes += value.len() as u64;
                let shadowed = shadowed_keys.contains(key.as_slice())
                    || deleted_prefixes
                        .iter()
                        .any(|prefix| key.starts_with(prefix));
                if !shadowed && cache.value_matches(key, value) {
                    redundant_count += 1;
                    redundant_bytes += value.len() as u64;
                }
                shadowed_keys.insert(key.as_slice());
            }
            WriteOperation::Delete { key } => {
                shadowed_keys.insert(key.as_slice());
            }
            WriteOperation::DeletePrefix { key_prefix } => {
                deleted_prefixes.push(key_prefix.as_slice());
            }
        }
    }
    RedundantPutStats {
        put_count,
        put_bytes,
        redundant_count,
        redundant_bytes,
    }
}

/// A key-value database with added LRU caching.
#[derive(Clone)]
pub struct LruCachingDatabase<D> {
    /// The inner store that is called by the LRU cache one.
    database: D,
    /// The configuration.
    config: StorageCacheConfig,
}

/// A key-value store with added LRU caching.
#[derive(Clone)]
pub struct LruCachingStore<S> {
    /// The inner store that is called by the LRU cache one.
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

    fn root_key(&self) -> Result<Vec<u8>, Self::Error> {
        self.store.root_key()
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
        cache.insert_read_value(key, &value);
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
        cache.insert_contains_key(key, result);
        Ok(result)
    }

    async fn contains_keys(&self, keys: &[Vec<u8>]) -> Result<Vec<bool>, Self::Error> {
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
            let key_results = self.store.contains_keys(&key_requests).await?;
            let mut cache = cache.lock().unwrap();
            for ((index, result), key) in indices.into_iter().zip(key_results).zip(key_requests) {
                results[index] = result;
                cache.insert_contains_key(&key, result);
            }
        }
        Ok(results)
    }

    async fn read_multi_values_bytes(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.read_multi_values_bytes(keys).await;
        };

        let mut result = Vec::with_capacity(keys.len());
        let mut cache_miss_indices = Vec::new();
        let mut miss_keys = Vec::new();
        {
            let mut cache = cache.lock().unwrap();
            for (i, key) in keys.iter().enumerate() {
                if let Some(value) = cache.query_read_value(key) {
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
                    miss_keys.push(key.clone());
                }
            }
        }
        if !miss_keys.is_empty() {
            let values = self.store.read_multi_values_bytes(&miss_keys).await?;
            let mut cache = cache.lock().unwrap();
            for (i, (key, value)) in cache_miss_indices
                .into_iter()
                .zip(miss_keys.into_iter().zip(values))
            {
                cache.insert_read_value(&key, &value);
                result[i] = value;
            }
        }
        Ok(result)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let Some(cache) = self.get_exclusive_cache() else {
            return self.store.find_keys_by_prefix(key_prefix).await;
        };
        {
            let mut cache = cache.lock().unwrap();
            if let Some(value) = cache.query_find_keys(key_prefix) {
                #[cfg(with_metrics)]
                metrics::FIND_KEYS_BY_PREFIX_CACHE_HIT_COUNT
                    .with_label_values(&[])
                    .inc();
                return Ok(value);
            }
        }
        #[cfg(with_metrics)]
        metrics::FIND_KEYS_BY_PREFIX_CACHE_MISS_COUNT
            .with_label_values(&[])
            .inc();
        let keys = self.store.find_keys_by_prefix(key_prefix).await?;
        let mut cache = cache.lock().unwrap();
        cache.insert_find_keys(key_prefix.to_vec(), &keys);
        Ok(keys)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
        let Some(cache) = self.get_exclusive_cache() else {
            return self.store.find_key_values_by_prefix(key_prefix).await;
        };
        {
            let mut cache = cache.lock().unwrap();
            if let Some(value) = cache.query_find_key_values(key_prefix) {
                #[cfg(with_metrics)]
                metrics::FIND_KEY_VALUES_BY_PREFIX_CACHE_HIT_COUNT
                    .with_label_values(&[])
                    .inc();
                return Ok(value);
            }
        }
        #[cfg(with_metrics)]
        metrics::FIND_KEY_VALUES_BY_PREFIX_CACHE_MISS_COUNT
            .with_label_values(&[])
            .inc();
        let key_values = self.store.find_key_values_by_prefix(key_prefix).await?;
        let mut cache = cache.lock().unwrap();
        cache.insert_find_key_values(key_prefix.to_vec(), &key_values);
        Ok(key_values)
    }
}

impl<K> WritableKeyValueStore for LruCachingStore<K>
where
    K: WritableKeyValueStore,
{
    // The LRU cache does not change the underlying store's size limits.
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        #[cfg(with_metrics)]
        if let Some(cache) = &self.cache {
            observe_redundant_puts(&mut cache.lock().unwrap(), &batch);
        }
        self.store.write_batch(batch.clone()).await?;
        if let Some(cache) = &self.cache {
            let mut cache = cache.lock().unwrap();
            for operation in &batch.operations {
                match operation {
                    WriteOperation::Put { key, value } => {
                        cache.put_key_value(key, value);
                    }
                    WriteOperation::Delete { key } => {
                        cache.delete_key(key);
                    }
                    WriteOperation::DeletePrefix { key_prefix } => {
                        cache.delete_prefix(key_prefix);
                    }
                }
            }
        }
        Ok(())
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
    /// The cache size being used.
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
        // Caching for immutable data is handled in DbStorage.
        Ok(LruCachingStore { store, cache: None })
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

    async fn list_root_keys(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        self.database.list_root_keys().await
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

    /// Returns a cache with exclusive access if one exists.
    fn get_exclusive_cache(&self) -> Option<&Arc<Mutex<LruPrefixCache>>> {
        let Some(cache) = &self.cache else {
            return None;
        };
        let has_exclusive_access = {
            let cache = cache.lock().unwrap();
            cache.has_exclusive_access()
        };
        if has_exclusive_access {
            Some(cache)
        } else {
            None
        }
    }
}

/// A memory database with caching.
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

#[cfg(with_testing)]
impl<D: crate::backends::DatabaseBackup> crate::backends::DatabaseBackup for LruCachingDatabase<D> {
    fn backup_to(&self, dir: &std::path::Path) -> anyhow::Result<()> {
        self.database.backup_to(dir)
    }
}

#[cfg(all(test, with_metrics))]
mod tests {
    use super::{count_redundant_puts, RedundantPutStats, DEFAULT_STORAGE_CACHE_CONFIG};
    use crate::{batch::Batch, lru_prefix_cache::LruPrefixCache};

    fn cache_holding(key: &[u8], value: &[u8]) -> LruPrefixCache {
        let mut cache = LruPrefixCache::new(DEFAULT_STORAGE_CACHE_CONFIG, true);
        cache.put_key_value(key, value);
        cache
    }

    #[test]
    fn identical_put_is_counted_as_redundant() {
        let mut cache = cache_holding(&[1, 2], &[7, 7, 7]);
        let mut batch = Batch::new();
        batch.put_key_value_bytes(vec![1, 2], vec![7, 7, 7]);
        assert_eq!(
            count_redundant_puts(&mut cache, &batch),
            RedundantPutStats {
                put_count: 1,
                put_bytes: 3,
                redundant_count: 1,
                redundant_bytes: 3,
            }
        );
    }

    #[test]
    fn differing_put_is_not_counted_as_redundant() {
        let mut cache = cache_holding(&[1, 2], &[7, 7, 7]);
        let mut batch = Batch::new();
        batch.put_key_value_bytes(vec![1, 2], vec![7, 7, 8]);
        let stats = count_redundant_puts(&mut cache, &batch);
        assert_eq!(stats.put_count, 1);
        assert_eq!(stats.redundant_count, 0);
        assert_eq!(stats.redundant_bytes, 0);
    }

    #[test]
    fn uncached_put_is_not_counted_as_redundant() {
        let mut cache = LruPrefixCache::new(DEFAULT_STORAGE_CACHE_CONFIG, true);
        let mut batch = Batch::new();
        batch.put_key_value_bytes(vec![1, 2], vec![7, 7, 7]);
        assert_eq!(count_redundant_puts(&mut cache, &batch).redundant_count, 0);
    }

    #[test]
    fn put_shadowed_by_earlier_delete_prefix_is_not_counted() {
        let mut cache = cache_holding(&[1, 2], &[7, 7, 7]);
        let mut batch = Batch::new();
        batch.delete_key_prefix(vec![1]);
        batch.put_key_value_bytes(vec![1, 2], vec![7, 7, 7]);
        assert_eq!(count_redundant_puts(&mut cache, &batch).redundant_count, 0);
    }

    #[test]
    fn put_shadowed_by_earlier_delete_is_not_counted() {
        let mut cache = cache_holding(&[1, 2], &[7, 7, 7]);
        let mut batch = Batch::new();
        batch.delete_key(vec![1, 2]);
        batch.put_key_value_bytes(vec![1, 2], vec![7, 7, 7]);
        assert_eq!(count_redundant_puts(&mut cache, &batch).redundant_count, 0);
    }

    #[test]
    fn delete_prefix_after_the_put_does_not_shadow_it() {
        let mut cache = cache_holding(&[1, 2], &[7, 7, 7]);
        let mut batch = Batch::new();
        batch.put_key_value_bytes(vec![1, 2], vec![7, 7, 7]);
        batch.delete_key_prefix(vec![9]);
        assert_eq!(count_redundant_puts(&mut cache, &batch).redundant_count, 1);
    }
}
