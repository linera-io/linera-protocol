// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A mutex-guarded LRU cache for move-in/move-out value patterns where `V` is not `Clone`.

use std::{
    hash::Hash,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicI64, Ordering},
        Mutex,
    },
};

use allocative::Allocative;
use lru::LruCache;

/// A bounded cache for values that are inserted and then taken out (moved), not cloned.
///
/// Uses `Mutex<LruCache>` internally. Suitable for caching values that don't implement
/// `Clone`, where the access pattern is insert → remove rather than insert → get.
///
/// When built with the `metrics` feature, reports hit, miss, entry-count, byte-weight,
/// and invalidation counters to Prometheus.
pub struct UniqueValueCache<K, V>
where
    K: Hash + Eq,
{
    cache: Mutex<LruCache<K, V>>,
    total_bytes: AtomicI64,
    #[cfg(with_metrics)]
    name: &'static str,
}

impl<K, V> UniqueValueCache<K, V>
where
    K: Hash + Eq + Copy,
    V: Allocative,
{
    /// Creates a new `UniqueValueCache`.
    ///
    /// * `name` – human-readable label for Prometheus metrics (e.g. `"execution_state"`).
    /// * `capacity` – maximum number of entries (LRU eviction by item count).
    ///
    /// Byte weights are computed automatically via [`allocative::size_of_unique`].
    #[cfg_attr(not(with_metrics), allow(unused_variables))]
    pub fn new(name: &'static str, capacity: usize) -> Self {
        let capacity =
            NonZeroUsize::try_from(capacity).expect("Cache capacity must be greater than zero");
        UniqueValueCache {
            cache: Mutex::new(LruCache::new(capacity)),
            total_bytes: AtomicI64::new(0),
            #[cfg(with_metrics)]
            name,
        }
    }

    /// Inserts a value into the cache if the key is not already present.
    ///
    /// Returns `true` if the value was newly inserted.
    pub fn insert(&self, key: &K, value: V) -> bool {
        let mut cache = self.cache.lock().unwrap();
        if cache.contains(key) {
            cache.promote(key);
            false
        } else {
            let added_weight = allocative::size_of_unique(&value) as i64;
            let evicted = cache.push(*key, value);
            let mut delta = added_weight;
            if let Some((_evicted_key, evicted_value)) = evicted {
                delta -= allocative::size_of_unique(&evicted_value) as i64;
                #[cfg(with_metrics)]
                crate::metrics::CACHE_EVICTION
                    .with_label_values(&[self.name, "unique"])
                    .inc();
            }
            self.total_bytes.fetch_add(delta, Ordering::Relaxed);
            #[cfg(with_metrics)]
            self.update_gauges(&cache);
            true
        }
    }

    /// Removes a value from the cache and returns it, if present.
    pub fn remove(&self, key: &K) -> Option<V> {
        let mut cache = self.cache.lock().unwrap();
        let result = cache.pop(key);
        #[cfg(with_metrics)]
        {
            let metric = if result.is_some() {
                &crate::metrics::CACHE_HIT
            } else {
                &crate::metrics::CACHE_MISS
            };
            metric.with_label_values(&[self.name, "unique"]).inc();
        }
        if let Some(ref value) = result {
            let removed_weight = allocative::size_of_unique(value) as i64;
            self.total_bytes
                .fetch_sub(removed_weight, Ordering::Relaxed);
            #[cfg(with_metrics)]
            self.update_gauges(&cache);
        }
        result
    }

    /// Clears all entries and increments the invalidation counter.
    pub fn invalidate(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
        self.total_bytes.store(0, Ordering::Relaxed);
        #[cfg(with_metrics)]
        {
            crate::metrics::CACHE_INVALIDATION
                .with_label_values(&[self.name, "unique"])
                .inc();
            self.update_gauges(&cache);
        }
    }

    #[cfg(with_metrics)]
    fn update_gauges(&self, cache: &LruCache<K, V>) {
        crate::metrics::CACHE_ENTRIES
            .with_label_values(&[self.name, "unique"])
            .set(cache.len() as i64);
        crate::metrics::CACHE_BYTES
            .with_label_values(&[self.name, "unique"])
            .set(self.total_bytes.load(Ordering::Relaxed));
    }
}

#[cfg(test)]
mod tests {
    use super::UniqueValueCache;

    const TEST_CACHE_SIZE: usize = 10;

    fn new_test_cache() -> UniqueValueCache<u64, String> {
        UniqueValueCache::new("test", TEST_CACHE_SIZE)
    }

    #[test]
    fn test_retrieve_missing_value() {
        let cache = new_test_cache();
        assert!(cache.remove(&42).is_none());
    }

    #[test]
    fn test_insert_and_remove() {
        let cache = new_test_cache();

        assert!(cache.insert(&1, "hello".to_string()));
        assert_eq!(cache.remove(&1), Some("hello".to_string()));
        // Value is gone after removal.
        assert!(cache.remove(&1).is_none());
    }

    #[test]
    fn test_insert_duplicate_returns_false() {
        let cache = new_test_cache();

        assert!(cache.insert(&1, "first".to_string()));
        assert!(!cache.insert(&1, "second".to_string()));

        // Original value is preserved.
        assert_eq!(cache.remove(&1), Some("first".to_string()));
    }

    #[test]
    fn test_insert_many_values() {
        let cache = new_test_cache();

        for i in 0..TEST_CACHE_SIZE as u64 {
            assert!(cache.insert(&i, format!("value-{i}")));
        }

        for i in 0..TEST_CACHE_SIZE as u64 {
            assert_eq!(cache.remove(&i), Some(format!("value-{i}")));
        }
    }

    #[test]
    fn test_lru_eviction() {
        let cache = new_test_cache();

        // Fill cache to capacity.
        for i in 0..TEST_CACHE_SIZE as u64 {
            cache.insert(&i, format!("value-{i}"));
        }

        // Insert one more — should evict the LRU entry (key 0).
        cache.insert(&(TEST_CACHE_SIZE as u64), "extra".to_string());

        assert!(cache.remove(&0).is_none(), "LRU entry should be evicted");
        assert_eq!(
            cache.remove(&(TEST_CACHE_SIZE as u64)),
            Some("extra".to_string()),
            "newest entry should be present"
        );
    }

    #[test]
    fn test_reinsertion_promotes_entry() {
        let cache = new_test_cache();

        // Fill cache.
        for i in 0..TEST_CACHE_SIZE as u64 {
            cache.insert(&i, format!("value-{i}"));
        }

        // Re-insert key 0 to promote it (move to most-recently-used).
        assert!(!cache.insert(&0, "should-be-ignored".to_string()));

        // Insert one more — should evict key 1 (now the LRU), not key 0.
        cache.insert(&(TEST_CACHE_SIZE as u64), "extra".to_string());

        assert!(
            cache.remove(&0).is_some(),
            "re-inserted entry should survive eviction"
        );
        assert!(
            cache.remove(&1).is_none(),
            "entry after promoted one should be evicted"
        );
    }

    #[test]
    fn test_invalidate_clears_all_entries() {
        let cache = new_test_cache();

        for i in 0..5u64 {
            cache.insert(&i, format!("value-{i}"));
        }

        cache.invalidate();

        for i in 0..5u64 {
            assert!(
                cache.remove(&i).is_none(),
                "cache should be empty after invalidation"
            );
        }
    }
}
