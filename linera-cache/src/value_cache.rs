// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A concurrent cache with efficient eviction and single-allocation guarantees.
//!
//! Values are stored internally as `Arc<V>`, so cache hits return a cheap
//! `Arc` clone instead of cloning the underlying data. A secondary weak
//! index (`papaya::HashMap<K, Weak<V>>`) ensures that at most one allocation
//! exists per key: if the bounded cache evicts an entry while a consumer
//! still holds an `Arc`, re-requesting the same key returns the same
//! allocation instead of creating a duplicate.

#[cfg(with_metrics)]
use std::any::type_name;
use std::{
    borrow::Cow,
    hash::Hash,
    sync::{Arc, Weak},
};

use linera_base::{crypto::CryptoHash, hashed::Hashed};
use papaya::{Compute, Operation};
use quick_cache::sync::Cache;

/// Default interval between dead-entry cleanup sweeps of the weak index.
pub const DEFAULT_CLEANUP_INTERVAL_SECS: u64 = 30;

/// A concurrent cache with efficient eviction and single-allocation guarantees.
///
/// Backed by `quick_cache` (S3-FIFO eviction) for bounded hot-path caching, plus
/// a lock-free `papaya::HashMap` weak index for deduplication. Together they
/// guarantee that at most one `Arc<V>` allocation exists per key at any time.
///
/// A background task periodically sweeps dead `Weak` entries from the index
/// to prevent unbounded memory growth.
pub struct ValueCache<K, V> {
    cache: Cache<K, Arc<V>>,
    weak_index: Arc<papaya::HashMap<K, Weak<V>>>,
}

impl<K, V> ValueCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Creates a new `ValueCache` with the given bounded-cache capacity
    /// and cleanup interval for the weak-reference index.
    #[cfg(not(web))]
    pub fn new(size: usize, cleanup_interval_secs: u64) -> Self {
        let weak_index = Arc::new(papaya::HashMap::new());
        Self::spawn_cleanup_task(
            Arc::clone(&weak_index),
            std::time::Duration::from_secs(cleanup_interval_secs),
        );
        ValueCache {
            cache: Cache::new(size),
            weak_index,
        }
    }

    /// Creates a new `ValueCache` (web variant, no background cleanup task).
    #[cfg(web)]
    pub fn new(size: usize, _cleanup_interval_secs: u64) -> Self {
        ValueCache {
            cache: Cache::new(size),
            weak_index: Arc::new(papaya::HashMap::new()),
        }
    }

    /// Inserts a value into the cache, returning the canonical `Arc`.
    ///
    /// The value is wrapped in `Arc` internally. If a live `Arc` for this key
    /// already exists (held by another consumer), the existing allocation is
    /// reused and the new value is dropped.
    pub fn insert(&self, key: &K, value: V) -> Arc<V> {
        self.dedup_insert(key, Arc::new(value))
    }

    /// Inserts a pre-wrapped `Arc<V>` into the cache, returning the canonical `Arc`.
    #[cfg(with_testing)]
    pub fn insert_arc(&self, key: &K, value: Arc<V>) -> Arc<V> {
        self.dedup_insert(key, value)
    }

    /// Removes a value from the bounded cache.
    ///
    /// The weak index entry is intentionally kept — another consumer may
    /// still hold an `Arc` to this value, and the weak index must be able
    /// to deduplicate against it. Dead weak entries are cleaned up by the
    /// background task.
    pub fn remove(&self, key: &K) -> Option<Arc<V>> {
        let value = self.cache.peek(key);
        if value.is_some() {
            self.cache.remove(key);
        }
        Self::track_cache_usage(value)
    }

    /// Returns an `Arc` reference to the value, checking both the bounded
    /// cache and the weak index.
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        // Tier 1: bounded cache (hot path)
        if let Some(arc) = self.cache.get(key) {
            return Self::track_cache_usage(Some(arc));
        }

        // Tier 2: weak index (catches evicted-but-still-held entries)
        let guard = self.weak_index.guard();
        if let Some(weak) = self.weak_index.get(key, &guard) {
            if let Some(arc) = weak.upgrade() {
                // Re-insert into bounded cache for future fast lookups
                self.cache.insert(key.clone(), arc.clone());
                return Self::track_cache_usage(Some(arc));
            }
        }

        Self::track_cache_usage(None)
    }

    /// Returns `true` if the value exists in either the bounded cache or
    /// the weak index (with a live allocation).
    pub fn contains(&self, key: &K) -> bool {
        if self.cache.peek(key).is_some() {
            return true;
        }
        let guard = self.weak_index.guard();
        self.weak_index
            .get(key, &guard)
            .is_some_and(|weak| weak.strong_count() > 0)
    }

    /// Removes all dead `Weak` entries from the weak index.
    #[cfg(with_testing)]
    pub fn cleanup_dead_entries(&self) {
        let guard = self.weak_index.guard();
        self.weak_index
            .retain(|_, weak| weak.strong_count() > 0, &guard);
    }

    /// Spawns a background task that periodically sweeps dead weak entries.
    ///
    /// No-op if no tokio runtime is available (e.g. in unit tests).
    #[cfg(not(web))]
    fn spawn_cleanup_task(
        weak_index: Arc<papaya::HashMap<K, Weak<V>>>,
        cleanup_interval: std::time::Duration,
    ) {
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);
            loop {
                interval.tick().await;
                let guard = weak_index.guard();
                weak_index.retain(|_, weak| weak.strong_count() > 0, &guard);
            }
        });
    }

    /// Core dedup logic: atomically checks the weak index for an existing
    /// live allocation. If found, reuses it. Otherwise inserts the new Arc.
    /// Returns the canonical `Arc`.
    fn dedup_insert(&self, key: &K, new_arc: Arc<V>) -> Arc<V> {
        let guard = self.weak_index.guard();
        let weak = Arc::downgrade(&new_arc);

        let result = self.weak_index.compute(
            key.clone(),
            |entry| match entry {
                Some((_k, existing_weak)) => match existing_weak.upgrade() {
                    Some(existing_arc) => Operation::Abort(existing_arc),
                    None => Operation::Insert(weak.clone()),
                },
                None => Operation::Insert(weak.clone()),
            },
            &guard,
        );

        let canonical_arc = match result {
            Compute::Inserted(..) | Compute::Updated { .. } => new_arc,
            Compute::Aborted(existing_arc) => existing_arc,
            _ => unreachable!(),
        };

        self.cache.insert(key.clone(), canonical_arc.clone());
        canonical_arc
    }

    fn track_cache_usage(maybe_value: Option<Arc<V>>) -> Option<Arc<V>> {
        #[cfg(with_metrics)]
        {
            let metric = if maybe_value.is_some() {
                &metrics::CACHE_HIT_COUNT
            } else {
                &metrics::CACHE_MISS_COUNT
            };

            metric
                .with_label_values(&[type_name::<K>(), type_name::<V>()])
                .inc();
        }
        maybe_value
    }
}

impl<T: Clone + Send + Sync + 'static> ValueCache<CryptoHash, T> {
    /// Inserts a [`Hashed`] value into the cache, storing only the inner value.
    ///
    /// The hash from the [`Hashed`] wrapper is used as the cache key, avoiding
    /// redundant storage of the hash in both key and value.
    ///
    /// The `value` is wrapped in a [`Cow`] so that it is only cloned if it
    /// needs to be inserted in the cache.
    ///
    /// Returns `true` if a new allocation was created.
    pub fn insert_hashed(&self, value: Cow<Hashed<T>>) -> bool {
        let hash = (*value).hash();
        // Fast path: already in bounded cache
        if self.cache.peek(&hash).is_some() {
            return false;
        }
        // Check weak index before cloning from Cow
        let guard = self.weak_index.guard();
        if let Some(weak) = self.weak_index.get(&hash, &guard) {
            if let Some(arc) = weak.upgrade() {
                self.cache.insert(hash, arc);
                return false;
            }
        }
        drop(guard);
        // Cache only the inner value; the hash is already stored as the key.
        self.dedup_insert(&hash, Arc::new(value.into_owned().into_inner()));
        true
    }

    /// Retrieves a value from the cache and reconstructs the [`Hashed`] wrapper.
    ///
    /// The hash used as the cache key is combined with the stored value to
    /// reconstruct the [`Hashed<T>`] without redundant storage.
    pub fn get_hashed(&self, hash: &CryptoHash) -> Option<Arc<Hashed<T>>> {
        let arc = self.get(hash)?;
        Some(Arc::new(Hashed::unchecked_new((*arc).clone(), *hash)))
    }

    /// Inserts multiple [`Hashed<T>`]s into the cache.
    #[cfg(with_testing)]
    pub fn insert_all_hashed<'a>(&self, values: impl IntoIterator<Item = Cow<'a, Hashed<T>>>)
    where
        T: 'a,
    {
        for value in values {
            self.insert_hashed(value);
        }
    }
}

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::register_int_counter_vec;
    use prometheus::IntCounterVec;

    pub static CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "value_cache_hit",
            "Cache hits in `ValueCache`",
            &["key_type", "value_type"],
        )
    });

    pub static CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "value_cache_miss",
            "Cache misses in `ValueCache`",
            &["key_type", "value_type"],
        )
    });
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, sync::Arc};

    use linera_base::{crypto::CryptoHash, hashed::Hashed};
    use serde::{Deserialize, Serialize};

    use super::{ValueCache, DEFAULT_CLEANUP_INTERVAL_SECS};

    /// Test cache size for unit tests.
    const TEST_CACHE_SIZE: usize = 10;

    /// A minimal hashable value for testing `ValueCache<CryptoHash, T>`.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestValue(u64);

    impl linera_base::crypto::BcsHashable<'_> for TestValue {}

    fn create_test_value(n: u64) -> Hashed<TestValue> {
        Hashed::new(TestValue(n))
    }

    fn create_test_values(iter: impl IntoIterator<Item = u64>) -> Vec<Hashed<TestValue>> {
        iter.into_iter().map(create_test_value).collect()
    }

    fn new_hashed_cache(size: usize) -> ValueCache<CryptoHash, TestValue> {
        ValueCache::new(size, DEFAULT_CLEANUP_INTERVAL_SECS)
    }

    fn new_string_cache(size: usize) -> ValueCache<u64, String> {
        ValueCache::new(size, DEFAULT_CLEANUP_INTERVAL_SECS)
    }

    #[test]
    fn test_retrieve_missing_value() {
        let cache = new_hashed_cache(TEST_CACHE_SIZE);
        let hash = CryptoHash::test_hash("Missing value");

        assert!(cache.get_hashed(&hash).is_none());
        assert!(!cache.contains(&hash));
    }

    #[test]
    fn test_insert_and_get() {
        let cache = new_hashed_cache(TEST_CACHE_SIZE);
        let value = create_test_value(0);
        let hash = value.hash();

        assert!(cache.insert_hashed(Cow::Borrowed(&value)));
        assert!(cache.contains(&hash));
        assert_eq!(cache.get_hashed(&hash).as_deref(), Some(&value));
    }

    #[test]
    fn test_insert_many_values() {
        let cache = new_hashed_cache(TEST_CACHE_SIZE);
        let values = create_test_values(0..TEST_CACHE_SIZE as u64);

        for value in &values {
            assert!(cache.insert_hashed(Cow::Borrowed(value)));
        }

        for value in &values {
            assert!(cache.contains(&value.hash()));
            assert_eq!(cache.get_hashed(&value.hash()).as_deref(), Some(value));
        }

        // Batch insert
        let cache2 = new_hashed_cache(TEST_CACHE_SIZE);
        cache2.insert_all_hashed(values.iter().map(Cow::Borrowed));
        for value in &values {
            assert_eq!(cache2.get_hashed(&value.hash()).as_deref(), Some(value));
        }
    }

    #[test]
    fn test_reinsertion_returns_false() {
        let cache = new_hashed_cache(TEST_CACHE_SIZE);
        let values = create_test_values(0..TEST_CACHE_SIZE as u64);

        cache.insert_all_hashed(values.iter().map(Cow::Borrowed));

        for value in &values {
            assert!(!cache.insert_hashed(Cow::Borrowed(value)));
        }

        for value in &values {
            assert!(cache.contains(&value.hash()));
            assert_eq!(cache.get_hashed(&value.hash()).as_deref(), Some(value));
        }
    }

    #[test]
    fn test_eviction() {
        let cache = new_hashed_cache(TEST_CACHE_SIZE);
        let total = TEST_CACHE_SIZE * 3;
        let values = create_test_values(0..total as u64);

        for value in &values {
            cache.insert_hashed(Cow::Borrowed(value));
        }

        let present_count = values.iter().filter(|v| cache.contains(&v.hash())).count();
        assert!(
            present_count <= TEST_CACHE_SIZE + 1,
            "cache should not hold significantly more than its capacity, \
             but has {present_count} entries for capacity {TEST_CACHE_SIZE}"
        );
        assert!(present_count > 0, "cache should still hold some entries");
    }

    #[test]
    fn test_accessed_entry_survives_eviction() {
        let cache = new_hashed_cache(TEST_CACHE_SIZE);
        let promoted = create_test_value(0);
        let promoted_hash = promoted.hash();

        cache.insert_hashed(Cow::Borrowed(&promoted));
        cache.get_hashed(&promoted_hash); // mark as hot

        let extras = create_test_values(1..=TEST_CACHE_SIZE as u64 * 2);
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
        let cache = new_hashed_cache(TEST_CACHE_SIZE);
        let promoted = create_test_value(0);
        let promoted_hash = promoted.hash();

        cache.insert_hashed(Cow::Borrowed(&promoted));
        assert!(!cache.insert_hashed(Cow::Borrowed(&promoted)));

        let extras = create_test_values(1..=TEST_CACHE_SIZE as u64 * 2);
        for value in &extras {
            cache.insert_hashed(Cow::Borrowed(value));
        }

        assert!(
            cache.contains(&promoted_hash),
            "re-inserted entry should survive eviction"
        );
    }

    #[test]
    fn test_weak_index_dedup_after_eviction() {
        let cache = new_string_cache(2);

        // Insert and hold onto the Arc
        let held = cache.insert(&1, "hello".to_string());

        // Force eviction by filling the cache
        cache.insert(&2, "world".to_string());
        cache.insert(&3, "foo".to_string());
        cache.insert(&4, "bar".to_string());

        // Weak index should find it via the held Arc
        let retrieved = cache
            .get(&1)
            .expect("held Arc should keep entry findable via weak index");
        assert!(
            Arc::ptr_eq(&retrieved, &held),
            "must return same allocation, not a duplicate"
        );

        // Re-inserting should also return the same Arc
        let reinserted = cache.insert(&1, "replacement".to_string());
        assert!(Arc::ptr_eq(&reinserted, &held));
        assert_eq!(&*reinserted, "hello");
    }

    #[test]
    fn test_remove_preserves_weak_for_held_arcs() {
        let cache = new_string_cache(TEST_CACHE_SIZE);

        let held = cache.insert(&1, "hello".to_string());

        // remove() evicts from bounded cache but NOT the weak index
        cache.remove(&1);

        // Still findable via weak index since we hold an Arc
        let retrieved = cache.get(&1).expect("weak index should find held Arc");
        assert!(Arc::ptr_eq(&retrieved, &held));
    }

    #[test]
    fn test_remove_without_holder() {
        let cache = new_string_cache(TEST_CACHE_SIZE);

        cache.insert(&1, "hello".to_string());

        // remove() without anyone holding an Arc — weak entry becomes dead
        cache.remove(&1);
        assert!(!cache.contains(&1));
        assert!(cache.get(&1).is_none());
    }

    #[test]
    fn test_cleanup_dead_entries() {
        let cache = new_string_cache(2);

        cache.insert(&1, "alive".to_string());
        let _held = cache.get(&1).expect("just inserted"); // keep alive

        cache.insert(&2, "dead".to_string());
        // Don't hold key 2

        // Force eviction of both by filling the cache
        cache.insert(&3, "a".to_string());
        cache.insert(&4, "b".to_string());
        cache.insert(&5, "c".to_string());

        cache.cleanup_dead_entries();

        // Key 1 still findable (we hold an Arc)
        assert!(cache.contains(&1));
    }

    #[test]
    fn test_insert_arc_dedup() {
        let cache = new_string_cache(TEST_CACHE_SIZE);
        let value = Arc::new("hello".to_string());

        let first = cache.insert_arc(&1, value.clone());
        assert!(Arc::ptr_eq(&first, &value));

        let second = cache.insert_arc(&1, Arc::new("other".to_string()));
        assert!(Arc::ptr_eq(&second, &value));

        assert_eq!(&*cache.get(&1).expect("just inserted"), "hello");
    }
}
