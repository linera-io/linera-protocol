// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A concurrent cache with efficient eviction.

#[cfg(with_metrics)]
use std::any::type_name;
use std::{borrow::Cow, hash::Hash};

use linera_base::{crypto::CryptoHash, hashed::Hashed};
use quick_cache::sync::Cache;

/// A concurrent cache with efficient eviction.
///
/// Backed by `quick_cache` which uses sharded locks for concurrent reads
/// and S3-FIFO eviction for better hit ratios than LRU.
pub struct ValueCache<K, V> {
    cache: Cache<K, V>,
}

impl<K, V> ValueCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Creates a new `ValueCache` with the given capacity.
    pub fn new(size: usize) -> Self {
        ValueCache {
            cache: Cache::new(size),
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
            true
        }
    }

    /// Removes a value from the cache and returns it, if present.
    pub fn remove(&self, key: &K) -> Option<V> {
        let value = self.cache.peek(key);
        if value.is_some() {
            self.cache.remove(key);
        }
        Self::track_cache_usage(value)
    }

    /// Returns a clone of the value from the cache, if present.
    pub fn get(&self, key: &K) -> Option<V> {
        Self::track_cache_usage(self.cache.get(key))
    }

    /// Returns `true` if the cache contains an entry for the given key.
    pub fn contains(&self, key: &K) -> bool {
        self.cache.peek(key).is_some()
    }

    fn track_cache_usage(maybe_value: Option<V>) -> Option<V> {
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
    /// The `value` is wrapped in a [`Cow`] so that it is only cloned if it needs to be
    /// inserted in the cache.
    ///
    /// Returns `true` if the value was not already present in the cache.
    pub fn insert_hashed(&self, value: Cow<Hashed<T>>) -> bool {
        let hash = (*value).hash();
        if self.cache.peek(&hash).is_some() {
            false
        } else {
            // Cache only the inner value; the hash is already stored as the key.
            self.cache.insert(hash, value.into_owned().into_inner());
            true
        }
    }

    /// Retrieves a value from the cache and reconstructs the [`Hashed`] wrapper.
    ///
    /// The hash used as the cache key is combined with the stored value to
    /// reconstruct the [`Hashed<T>`] without redundant storage.
    pub fn get_hashed(&self, hash: &CryptoHash) -> Option<Hashed<T>> {
        let value = Self::track_cache_usage(self.cache.get(hash))?;
        Some(Hashed::unchecked_new(value, *hash))
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
                self.cache.insert(hash, value.into_owned().into_inner());
            }
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
    use std::borrow::Cow;

    use linera_base::{crypto::CryptoHash, hashed::Hashed};
    use serde::{Deserialize, Serialize};

    use super::ValueCache;

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

    #[test]
    fn test_retrieve_missing_value() {
        let cache = ValueCache::<CryptoHash, TestValue>::new(TEST_CACHE_SIZE);
        let hash = CryptoHash::test_hash("Missing value");

        assert!(cache.get_hashed(&hash).is_none());
        assert!(!cache.contains(&hash));
    }

    #[test]
    fn test_insert_single_value() {
        let cache = ValueCache::<CryptoHash, TestValue>::new(TEST_CACHE_SIZE);
        let value = create_test_value(0);
        let hash = value.hash();

        assert!(cache.insert_hashed(Cow::Borrowed(&value)));
        assert!(cache.contains(&hash));
        assert_eq!(cache.get_hashed(&hash), Some(value));
    }

    #[test]
    fn test_insert_many_values_individually() {
        let cache = ValueCache::<CryptoHash, TestValue>::new(TEST_CACHE_SIZE);
        let values = create_test_values(0..TEST_CACHE_SIZE as u64);

        for value in &values {
            assert!(cache.insert_hashed(Cow::Borrowed(value)));
        }

        for value in &values {
            assert!(cache.contains(&value.hash()));
            assert_eq!(cache.get_hashed(&value.hash()).as_ref(), Some(value));
        }
    }

    #[test]
    fn test_insert_many_values_together() {
        let cache = ValueCache::<CryptoHash, TestValue>::new(TEST_CACHE_SIZE);
        let values = create_test_values(0..TEST_CACHE_SIZE as u64);

        cache.insert_all_hashed(values.iter().map(Cow::Borrowed));

        for value in &values {
            assert!(cache.contains(&value.hash()));
            assert_eq!(cache.get_hashed(&value.hash()).as_ref(), Some(value));
        }
    }

    #[test]
    fn test_reinsertion_of_values() {
        let cache = ValueCache::<CryptoHash, TestValue>::new(TEST_CACHE_SIZE);
        let values = create_test_values(0..TEST_CACHE_SIZE as u64);

        cache.insert_all_hashed(values.iter().map(Cow::Borrowed));

        for value in &values {
            assert!(!cache.insert_hashed(Cow::Borrowed(value)));
        }

        for value in &values {
            assert!(cache.contains(&value.hash()));
            assert_eq!(cache.get_hashed(&value.hash()).as_ref(), Some(value));
        }
    }

    #[test]
    fn test_eviction() {
        let cache = ValueCache::<CryptoHash, TestValue>::new(TEST_CACHE_SIZE);
        // Insert 3x capacity to guarantee evictions (quick_cache capacity is approximate).
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
        let cache = ValueCache::<CryptoHash, TestValue>::new(TEST_CACHE_SIZE);
        let promoted = create_test_value(0);
        let promoted_hash = promoted.hash();

        // Insert the promoted entry first, then access it to mark it as "hot".
        cache.insert_hashed(Cow::Borrowed(&promoted));
        cache.get_hashed(&promoted_hash);

        // Insert many more entries to force evictions.
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
        let cache = ValueCache::<CryptoHash, TestValue>::new(TEST_CACHE_SIZE);
        let promoted = create_test_value(0);
        let promoted_hash = promoted.hash();

        // Insert the promoted entry, then re-insert it to mark it as "hot".
        cache.insert_hashed(Cow::Borrowed(&promoted));
        assert!(!cache.insert_hashed(Cow::Borrowed(&promoted)));

        // Insert many more entries to force evictions.
        let extras = create_test_values(1..=TEST_CACHE_SIZE as u64 * 2);
        for value in &extras {
            cache.insert_hashed(Cow::Borrowed(value));
        }

        assert!(
            cache.contains(&promoted_hash),
            "re-inserted entry should survive eviction"
        );
    }
}
