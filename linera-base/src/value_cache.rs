// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Concurrent caches for values.

#[cfg(with_metrics)]
use std::any::type_name;
use std::{borrow::Cow, hash::Hash, num::NonZeroUsize, sync::Mutex};

use lru::LruCache;
use quick_cache::sync::Cache;

use crate::{crypto::CryptoHash, hashed::Hashed};

/// A counter metric for the number of cache hits in the [`ValueCache`].
#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use prometheus::IntCounterVec;

    use crate::prometheus_util::register_int_counter_vec;

    pub static CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "value_cache_hit",
            "Cache hits in `ValueCache`",
            &["key_type", "value_type"],
        )
    });

    /// A counter metric for the number of cache misses in the [`ValueCache`].
    pub static CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "value_cache_miss",
            "Cache misses in `ValueCache`",
            &["key_type", "value_type"],
        )
    });
}

/// A concurrent cache of values using S3-FIFO eviction policy.
/// Thread-safe without explicit locking - uses sharded internal locks for high concurrency.
/// Requires `V: Clone` because values are cloned on fetch.
pub struct ValueCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    cache: Cache<K, V>,
}

impl<K, V> ValueCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    /// Creates a new `ValueCache` with the given size.
    pub fn new(size: usize) -> Self {
        assert!(size > 0, "Cache size must be larger than zero");
        ValueCache {
            cache: Cache::new(size),
        }
    }

    /// Inserts a value into the cache with the given key.
    /// Returns `true` if the value was newly inserted, `false` if it already existed.
    pub fn insert(&self, key: K, value: V) -> bool {
        if self.contains_key(&key) {
            false
        } else {
            self.cache.insert(key, value);
            true
        }
    }

    /// Returns a `V` from the cache, if present.
    pub fn get(&self, key: &K) -> Option<V> {
        Self::track_cache_usage(self.cache.get(key))
    }

    /// Returns `true` if the cache contains the given key.
    pub fn contains_key(&self, key: &K) -> bool {
        self.cache.get(key).is_some()
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

impl<T: Clone> ValueCache<CryptoHash, Hashed<T>> {
    /// Inserts a [`Hashed`] value into the cache, if it's not already present.
    ///
    /// The `value` is wrapped in a [`Cow`] so that it is only cloned if it needs to be
    /// inserted in the cache.
    ///
    /// Returns [`true`] if the value was not already present in the cache.
    pub fn insert_hashed(&self, value: Cow<Hashed<T>>) -> bool {
        let hash = (*value).hash();
        if self.cache.get(&hash).is_some() {
            false
        } else {
            self.cache.insert(hash, value.into_owned());
            true
        }
    }

    /// Inserts multiple [`Hashed`] values into the cache if they're not already present.
    ///
    /// The `values` are wrapped in [`Cow`]s so that each `value` is only cloned if it
    /// needs to be inserted in the cache.
    #[cfg(with_testing)]
    pub fn insert_all<'a>(&self, values: impl IntoIterator<Item = Cow<'a, Hashed<T>>>)
    where
        T: 'a,
    {
        for value in values {
            let hash = (*value).hash();
            if self.cache.get(&hash).is_none() {
                self.cache.insert(hash, value.into_owned());
            }
        }
    }
}

#[cfg(with_testing)]
impl<K, V> ValueCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    /// Returns [`true`] if the cache contains the `V` with the requested `K`.
    pub fn contains(&self, key: &K) -> bool {
        self.cache.get(key).is_some()
    }

    /// Returns the number of items in the cache.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Returns [`true`] if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.len() == 0
    }
}

/// A cache for values that need to be "parked" temporarily and taken out for exclusive use.
/// Uses LRU eviction. Does NOT require `Clone` on `V` since values are moved out, not cloned.
/// This is useful for caching expensive-to-create objects that will be mutated after retrieval.
pub struct ParkingCache<K, V>
where
    K: Hash + Eq + Copy,
{
    cache: Mutex<LruCache<K, V>>,
}

impl<K, V> ParkingCache<K, V>
where
    K: Hash + Eq + Copy,
{
    /// Creates a new `ParkingCache` with the given size.
    pub fn new(size: usize) -> Self {
        let size = NonZeroUsize::try_from(size).expect("Cache size is larger than zero");
        ParkingCache {
            cache: Mutex::new(LruCache::new(size)),
        }
    }

    /// Inserts a `V` into the cache, if it's not already present.
    /// Returns `true` if the value was newly inserted, `false` if it already existed.
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

    /// Removes a `V` from the cache and returns it, if present.
    /// This is the primary way to retrieve values - they are removed for exclusive use.
    pub fn remove(&self, key: &K) -> Option<V> {
        #[cfg(with_metrics)]
        {
            let maybe_value = self.cache.lock().unwrap().pop(key);
            let metric = if maybe_value.is_some() {
                &metrics::CACHE_HIT_COUNT
            } else {
                &metrics::CACHE_MISS_COUNT
            };
            metric
                .with_label_values(&[type_name::<K>(), type_name::<V>()])
                .inc();
            maybe_value
        }
        #[cfg(not(with_metrics))]
        self.cache.lock().unwrap().pop(key)
    }
}
