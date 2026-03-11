// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A mutex-guarded LRU cache for move-in/move-out value patterns where `V` is not `Clone`.

use std::{hash::Hash, num::NonZeroUsize, sync::Mutex};

use lru::LruCache;

/// A bounded cache for values that are inserted and then taken out (moved), not cloned.
///
/// Uses `Mutex<LruCache>` internally. Suitable for caching values that don't implement
/// `Clone`, where the access pattern is insert → remove rather than insert → get.
pub struct UniqueValueCache<K, V>
where
    K: Hash + Eq,
{
    cache: Mutex<LruCache<K, V>>,
}

impl<K, V> UniqueValueCache<K, V>
where
    K: Hash + Eq + Copy,
{
    /// Creates a new `UniqueValueCache` with the given capacity.
    pub fn new(size: usize) -> Self {
        let size = NonZeroUsize::try_from(size).expect("Cache size must be greater than zero");
        UniqueValueCache {
            cache: Mutex::new(LruCache::new(size)),
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
            cache.push(*key, value);
            true
        }
    }

    /// Removes a value from the cache and returns it, if present.
    pub fn remove(&self, key: &K) -> Option<V> {
        self.cache.lock().unwrap().pop(key)
    }
}

#[cfg(test)]
mod tests {
    use super::UniqueValueCache;

    const TEST_CACHE_SIZE: usize = 10;

    #[test]
    fn test_retrieve_missing_value() {
        let cache = UniqueValueCache::<u64, String>::new(TEST_CACHE_SIZE);
        assert!(cache.remove(&42).is_none());
    }

    #[test]
    fn test_insert_and_remove() {
        let cache = UniqueValueCache::<u64, String>::new(TEST_CACHE_SIZE);

        assert!(cache.insert(&1, "hello".to_string()));
        assert_eq!(cache.remove(&1), Some("hello".to_string()));
        // Value is gone after removal.
        assert!(cache.remove(&1).is_none());
    }

    #[test]
    fn test_insert_duplicate_returns_false() {
        let cache = UniqueValueCache::<u64, String>::new(TEST_CACHE_SIZE);

        assert!(cache.insert(&1, "first".to_string()));
        assert!(!cache.insert(&1, "second".to_string()));

        // Original value is preserved.
        assert_eq!(cache.remove(&1), Some("first".to_string()));
    }

    #[test]
    fn test_insert_many_values() {
        let cache = UniqueValueCache::<u64, String>::new(TEST_CACHE_SIZE);

        for i in 0..TEST_CACHE_SIZE as u64 {
            assert!(cache.insert(&i, format!("value-{i}")));
        }

        for i in 0..TEST_CACHE_SIZE as u64 {
            assert_eq!(cache.remove(&i), Some(format!("value-{i}")));
        }
    }

    #[test]
    fn test_lru_eviction() {
        let cache = UniqueValueCache::<u64, String>::new(TEST_CACHE_SIZE);

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
        let cache = UniqueValueCache::<u64, String>::new(TEST_CACHE_SIZE);

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
}
