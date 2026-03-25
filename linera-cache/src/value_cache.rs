// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A concurrent cache with efficient eviction and Prometheus metrics.

use std::{borrow::Cow, hash::Hash};

use allocative::Allocative;
use linera_base::{crypto::CryptoHash, hashed::Hashed};
use quick_cache::{sync::Cache, DefaultHashBuilder, Lifecycle, Weighter};

/// A [`Weighter`] that uses [`allocative::size_of_unique`] to compute the byte weight
/// of cached entries. This measures `mem::size_of::<T>()` plus all uniquely-owned heap
/// allocations — correct for any type that derives or implements [`Allocative`].
#[derive(Clone)]
struct AllocativeWeighter;

impl<K: Allocative, V: Allocative> Weighter<K, V> for AllocativeWeighter {
    fn weight(&self, key: &K, val: &V) -> u64 {
        (allocative::size_of_unique(key) + allocative::size_of_unique(val)) as u64
    }
}

/// Lifecycle hook that increments the `cache_eviction` Prometheus counter on each eviction.
#[derive(Clone)]
struct EvictionTracker {
    #[cfg(with_metrics)]
    name: &'static str,
}

impl<K, V> Lifecycle<K, V> for EvictionTracker {
    type RequestState = ();

    fn begin_request(&self) -> Self::RequestState {}

    #[cfg_attr(not(with_metrics), allow(unused_variables))]
    fn on_evict(&self, _state: &mut Self::RequestState, _key: K, _val: V) {
        #[cfg(with_metrics)]
        crate::metrics::CACHE_EVICTION
            .with_label_values(&[self.name, "value"])
            .inc();
    }
}

/// A concurrent cache with efficient eviction.
///
/// Backed by `quick_cache` which uses sharded locks for concurrent reads
/// and S3-FIFO eviction for better hit ratios than LRU.
///
/// When built with the `metrics` feature, every instance reports hit, miss,
/// entry-count, byte-weight, and invalidation counters to Prometheus, labeled
/// by the human-readable `name` provided at construction time.
pub struct ValueCache<K, V> {
    cache: Cache<K, V, AllocativeWeighter, DefaultHashBuilder, EvictionTracker>,
    #[cfg(with_metrics)]
    name: &'static str,
}

impl<K, V> ValueCache<K, V>
where
    K: Hash + Eq + Clone + Allocative + Send + Sync + 'static,
    V: Clone + Allocative + Send + Sync + 'static,
{
    /// Creates a new weight-based `ValueCache`.
    ///
    /// * `name` – human-readable label for Prometheus metrics (e.g. `"block"`).
    /// * `weight_capacity` – maximum total weight (bytes) before eviction kicks in.
    ///
    /// Entry weights are computed automatically via [`allocative::size_of_unique`],
    /// which measures stack size plus all uniquely-owned heap allocations.
    #[cfg_attr(not(with_metrics), allow(unused_variables))]
    pub fn new(name: &'static str, weight_capacity: u64) -> Self {
        // quick_cache REQUIRES estimated_items_capacity (no default — options.rs:134 errors).
        // Controls: shard count (≥32 items/shard) and ghost entry space (50% of estimate).
        // Docs say "within 1-2 orders of magnitude is often good enough" (options.rs:76-77).
        //
        // Heuristic: weight_capacity / 4096 (assume ~4 KiB average entry).
        // Ghost overhead = estimated * 0.5 * ~16 bytes ≈ 0.2% of weight_capacity (negligible).
        // Shard count on 16-core machine = 64 shards (determined by core count, not this).
        let estimated_items = (weight_capacity / 4096).clamp(1024, 1_000_000) as usize;
        ValueCache {
            cache: Cache::with(
                estimated_items,
                weight_capacity,
                AllocativeWeighter,
                DefaultHashBuilder::default(),
                EvictionTracker {
                    #[cfg(with_metrics)]
                    name,
                },
            ),
            #[cfg(with_metrics)]
            name,
        }
    }

    /// Inserts a value into the cache if the key is not already present.
    ///
    /// Returns `true` if the value was newly inserted.
    pub fn insert(&self, key: &K, value: V) -> bool {
        if self.cache.peek(key).is_some() {
            false
        } else {
            self.cache.insert(key.clone(), value);
            self.update_gauges();
            true
        }
    }

    /// Removes a value from the cache and returns it, if present.
    pub fn remove(&self, key: &K) -> Option<V> {
        let value = self.cache.peek(key);
        if value.is_some() {
            self.cache.remove(key);
            self.update_gauges();
        }
        value
    }

    /// Returns a clone of the value from the cache, if present.
    pub fn get(&self, key: &K) -> Option<V> {
        self.track_hit_miss(self.cache.get(key))
    }

    /// Returns `true` if the cache contains an entry for the given key.
    pub fn contains(&self, key: &K) -> bool {
        self.cache.peek(key).is_some()
    }

    /// Clears all entries and increments the invalidation counter.
    pub fn invalidate(&self) {
        self.cache.clear();
        #[cfg(with_metrics)]
        {
            crate::metrics::CACHE_INVALIDATION
                .with_label_values(&[self.name, "value"])
                .inc();
        }
        self.update_gauges();
    }

    fn track_hit_miss(&self, maybe_value: Option<V>) -> Option<V> {
        #[cfg(with_metrics)]
        {
            let metric = if maybe_value.is_some() {
                &crate::metrics::CACHE_HIT
            } else {
                &crate::metrics::CACHE_MISS
            };
            metric.with_label_values(&[self.name, "value"]).inc();
        }
        maybe_value
    }

    #[cfg(with_metrics)]
    fn update_gauges(&self) {
        crate::metrics::CACHE_ENTRIES
            .with_label_values(&[self.name, "value"])
            .set(self.cache.len() as i64);
        crate::metrics::CACHE_BYTES
            .with_label_values(&[self.name, "value"])
            .set(self.cache.weight() as i64);
    }

    #[cfg(not(with_metrics))]
    fn update_gauges(&self) {}
}

impl<T: Clone + Allocative + Send + Sync + 'static> ValueCache<CryptoHash, Hashed<T>> {
    /// Inserts a [`Hashed<T>`] into the cache if its hash is not already present.
    ///
    /// The `value` is wrapped in a [`Cow`] so that it is only cloned if it needs to be
    /// inserted in the cache.
    ///
    /// Returns `true` if the value was not already present in the cache.
    pub fn insert_hashed(&self, value: Cow<Hashed<T>>) -> bool {
        let hash = (*value).hash();
        if self.cache.peek(&hash).is_some() {
            false
        } else {
            self.cache.insert(hash, value.into_owned());
            self.update_gauges();
            true
        }
    }

    /// Inserts multiple [`Hashed<T>`]s into the cache.
    #[cfg(with_testing)]
    pub fn insert_all_hashed<'a>(&self, values: impl IntoIterator<Item = Cow<'a, Hashed<T>>>)
    where
        T: 'a,
    {
        for value in values {
            let hash = (*value).hash();
            if self.cache.peek(&hash).is_none() {
                self.cache.insert(hash, value.into_owned());
            }
        }
        self.update_gauges();
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use allocative::Allocative;
    use linera_base::{crypto::CryptoHash, hashed::Hashed};
    use serde::{Deserialize, Serialize};

    use super::ValueCache;

    /// Weight capacity large enough to never trigger weight-based eviction in tests.
    const TEST_WEIGHT_CAPACITY: u64 = 10_000_000;

    /// Number of items used in multi-entry tests.
    const TEST_ITEM_COUNT: usize = 10;

    /// Small weight capacity for eviction tests.
    /// Each `(CryptoHash, Hashed<TestValue>)` weighs ~72 bytes via allocative.
    const SMALL_WEIGHT_CAPACITY: u64 = 5000;

    fn new_test_cache() -> ValueCache<CryptoHash, Hashed<TestValue>> {
        ValueCache::new("test", TEST_WEIGHT_CAPACITY)
    }

    fn new_small_cache() -> ValueCache<CryptoHash, Hashed<TestValue>> {
        ValueCache::new("test_small", SMALL_WEIGHT_CAPACITY)
    }

    /// A minimal hashable value for testing `ValueCache<CryptoHash, Hashed<T>>`.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Allocative)]
    struct TestValue(u64);

    impl linera_base::crypto::BcsHashable<'_> for TestValue {}

    fn create_test_value(n: u64) -> Hashed<TestValue> {
        Hashed::new(TestValue(n))
    }

    fn create_test_values(iter: impl IntoIterator<Item = u64>) -> Vec<Hashed<TestValue>> {
        iter.into_iter().map(create_test_value).collect()
    }

    #[test]
    fn test_retrieve_missing_value() {
        let cache = new_test_cache();
        let hash = CryptoHash::test_hash("Missing value");

        assert!(cache.get(&hash).is_none());
        assert!(!cache.contains(&hash));
    }

    #[test]
    fn test_insert_single_value() {
        let cache = new_test_cache();
        let value = create_test_value(0);
        let hash = value.hash();

        assert!(cache.insert_hashed(Cow::Borrowed(&value)));
        assert!(cache.contains(&hash));
        assert_eq!(cache.get(&hash), Some(value));
    }

    #[test]
    fn test_insert_many_values_individually() {
        let cache = new_test_cache();
        let values = create_test_values(0..TEST_ITEM_COUNT as u64);

        for value in &values {
            assert!(cache.insert_hashed(Cow::Borrowed(value)));
        }

        for value in &values {
            assert!(cache.contains(&value.hash()));
            assert_eq!(cache.get(&value.hash()).as_ref(), Some(value));
        }
    }

    #[test]
    fn test_insert_many_values_together() {
        let cache = new_test_cache();
        let values = create_test_values(0..TEST_ITEM_COUNT as u64);

        cache.insert_all_hashed(values.iter().map(Cow::Borrowed));

        for value in &values {
            assert!(cache.contains(&value.hash()));
            assert_eq!(cache.get(&value.hash()).as_ref(), Some(value));
        }
    }

    #[test]
    fn test_reinsertion_of_values() {
        let cache = new_test_cache();
        let values = create_test_values(0..TEST_ITEM_COUNT as u64);

        cache.insert_all_hashed(values.iter().map(Cow::Borrowed));

        for value in &values {
            assert!(!cache.insert_hashed(Cow::Borrowed(value)));
        }

        for value in &values {
            assert!(cache.contains(&value.hash()));
            assert_eq!(cache.get(&value.hash()).as_ref(), Some(value));
        }
    }

    #[test]
    fn test_eviction() {
        let cache = new_small_cache();
        // Insert many more entries than the cache can hold (1000 bytes / ~72 bytes per entry ≈ 13).
        let total = 50;
        let values = create_test_values(0..total);

        for value in &values {
            cache.insert_hashed(Cow::Borrowed(value));
        }

        let present_count = values.iter().filter(|v| cache.contains(&v.hash())).count();
        assert!(
            present_count < total as usize,
            "cache should have evicted some entries, but all {total} are still present"
        );
        assert!(present_count > 0, "cache should still hold some entries");
    }

    #[test]
    fn test_accessed_entry_survives_eviction() {
        let cache = new_small_cache();
        let promoted = create_test_value(0);
        let promoted_hash = promoted.hash();

        // Insert the promoted entry first, then access it to mark it as "hot".
        cache.insert_hashed(Cow::Borrowed(&promoted));
        cache.get(&promoted_hash);

        // Insert many more entries to force evictions.
        let extras = create_test_values(1..=TEST_ITEM_COUNT as u64 * 2);
        for value in &extras {
            cache.insert_hashed(Cow::Borrowed(value));
        }

        assert!(
            cache.contains(&promoted_hash),
            "recently accessed entry should survive eviction"
        );
    }

    #[test]
    fn test_promotion_of_reinsertion() {
        let cache = new_small_cache();
        let promoted = create_test_value(0);
        let promoted_hash = promoted.hash();

        // Insert the promoted entry, then re-insert it to mark it as "hot".
        cache.insert_hashed(Cow::Borrowed(&promoted));
        assert!(!cache.insert_hashed(Cow::Borrowed(&promoted)));

        // Insert many more entries to force evictions.
        let extras = create_test_values(1..=TEST_ITEM_COUNT as u64 * 2);
        for value in &extras {
            cache.insert_hashed(Cow::Borrowed(value));
        }

        assert!(
            cache.contains(&promoted_hash),
            "re-inserted entry should survive eviction"
        );
    }

    #[test]
    fn test_invalidate_clears_all_entries() {
        let cache = new_test_cache();
        let values = create_test_values(0..5);

        for value in &values {
            cache.insert_hashed(Cow::Borrowed(value));
        }

        cache.invalidate();

        for value in &values {
            assert!(
                !cache.contains(&value.hash()),
                "cache should be empty after invalidation"
            );
        }
    }
}
