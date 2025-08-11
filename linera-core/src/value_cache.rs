// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A least-recently used cache of values.

#[cfg(test)]
#[path = "unit_tests/value_cache_tests.rs"]
mod unit_tests;

#[cfg(with_metrics)]
use std::any::type_name;
use std::{borrow::Cow, hash::Hash, num::NonZeroUsize, sync::Mutex};

use linera_base::{crypto::CryptoHash, hashed::Hashed};
use lru::LruCache;

/// The default cache size.
pub const DEFAULT_VALUE_CACHE_SIZE: usize = 10_000;

/// A counter metric for the number of cache hits in the [`ValueCache`].
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

    /// A counter metric for the number of cache misses in the [`ValueCache`].
    pub static CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "value_cache_miss",
            "Cache misses in `ValueCache`",
            &["key_type", "value_type"],
        )
    });
}

/// A least-recently used cache of a value.
pub struct ValueCache<K, V>
where
    K: Hash + Eq + PartialEq + Copy,
{
    cache: Mutex<LruCache<K, V>>,
}

impl<K, V> Default for ValueCache<K, V>
where
    K: Hash + Eq + PartialEq + Copy,
{
    fn default() -> Self {
        let size = NonZeroUsize::try_from(DEFAULT_VALUE_CACHE_SIZE)
            .expect("Default cache size is larger than zero");

        ValueCache {
            cache: Mutex::new(LruCache::new(size)),
        }
    }
}

impl<K, V> ValueCache<K, V>
where
    K: Hash + Eq + PartialEq + Copy,
{
    /// Inserts a `V` into the cache, if it's not already present.
    pub fn insert_owned(&self, key: &K, value: V) -> bool {
        let mut cache = self.cache.lock().unwrap();
        if cache.contains(key) {
            // Promote the re-inserted value in the cache, as if it was accessed again.
            cache.promote(key);
            false
        } else {
            // Cache the value so that clients don't have to send it again.
            cache.push(*key, value);
            true
        }
    }

    /// Removes a `V` from the cache and returns it, if present.
    pub fn remove(&self, hash: &K) -> Option<V> {
        Self::track_cache_usage(self.cache.lock().unwrap().pop(hash))
    }

    /// Returns a `V` from the cache, if present.
    pub fn get(&self, hash: &K) -> Option<V>
    where
        V: Clone,
    {
        Self::track_cache_usage(self.cache.lock().unwrap().get(hash).cloned())
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
    /// Inserts a [`HashedCertificateValue`] into the cache, if it's not already present.
    ///
    /// The `value` is wrapped in a [`Cow`] so that it is only cloned if it needs to be
    /// inserted in the cache.
    ///
    /// Returns [`true`] if the value was not already present in the cache.
    pub fn insert(&self, value: Cow<Hashed<T>>) -> bool {
        let hash = (*value).hash();
        let mut cache = self.cache.lock().unwrap();
        if cache.contains(&hash) {
            // Promote the re-inserted value in the cache, as if it was accessed again.
            cache.promote(&hash);
            false
        } else {
            // Cache the certificate so that clients don't have to send the value again.
            cache.push(hash, value.into_owned());
            true
        }
    }

    /// Inserts multiple [`HashedCertificateValue`]s into the cache. If they're not
    /// already present.
    ///
    /// The `values` are wrapped in [`Cow`]s so that each `value` is only cloned if it
    /// needs to be inserted in the cache.
    #[cfg(test)]
    pub fn insert_all<'a>(&self, values: impl IntoIterator<Item = Cow<'a, Hashed<T>>>)
    where
        T: 'a,
    {
        let mut cache = self.cache.lock().unwrap();
        for value in values {
            let hash = (*value).hash();
            if !cache.contains(&hash) {
                cache.push(hash, value.into_owned());
            }
        }
    }
}

#[cfg(test)]
impl<K, V> ValueCache<K, V>
where
    K: Hash + Eq + PartialEq + Copy,
{
    /// Returns a `Collection` of the hashes in the cache.
    pub fn keys<Collection>(&self) -> Collection
    where
        Collection: FromIterator<K>,
    {
        self.cache
            .lock()
            .unwrap()
            .iter()
            .map(|(key, _)| *key)
            .collect()
    }

    /// Returns [`true`] if the cache contains the `V` with the
    /// requested `K`.
    pub fn contains(&self, key: &K) -> bool {
        self.cache.lock().unwrap().contains(key)
    }
}
