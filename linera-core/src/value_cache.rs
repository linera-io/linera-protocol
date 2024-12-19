// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A least-recently used cache of values.

#[cfg(test)]
#[path = "unit_tests/value_cache_tests.rs"]
mod unit_tests;

#[cfg(with_metrics)]
use std::{any::type_name, sync::LazyLock};
use std::{borrow::Cow, hash::Hash, num::NonZeroUsize};

use linera_base::{crypto::CryptoHash, data_types::Blob, hashed::Hashed, identifiers::BlobId};
use lru::LruCache;
use tokio::sync::Mutex;
#[cfg(with_metrics)]
use {linera_base::prometheus_util::register_int_counter_vec, prometheus::IntCounterVec};

/// The default cache size.
pub const DEFAULT_VALUE_CACHE_SIZE: usize = 1000;

/// A counter metric for the number of cache hits in the [`ValueCache`].
#[cfg(with_metrics)]
static CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "value_cache_hit",
        "Cache hits in `ValueCache`",
        &["key_type", "value_type"],
    )
});

/// A counter metric for the number of cache misses in the [`ValueCache`].
#[cfg(with_metrics)]
static CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "value_cache_miss",
        "Cache misses in `ValueCache`",
        &["key_type", "value_type"],
    )
});

/// A least-recently used cache of a value.
pub struct ValueCache<K, V>
where
    K: Hash + Eq + PartialEq + Copy,
    V: Clone,
{
    cache: Mutex<LruCache<K, V>>,
}

impl<K, V> Default for ValueCache<K, V>
where
    K: Hash + Eq + PartialEq + Copy,
    V: Clone,
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
    V: Clone,
{
    /// Returns a `Collection` of the hashes in the cache.
    pub async fn keys<Collection>(&self) -> Collection
    where
        Collection: FromIterator<K>,
    {
        self.cache
            .lock()
            .await
            .iter()
            .map(|(key, _)| *key)
            .collect()
    }

    /// Returns [`true`] if the cache contains the `V` with the
    /// requested `K`.
    pub async fn contains(&self, key: &K) -> bool {
        self.cache.lock().await.contains(key)
    }

    /// Returns a `Collection` created from a set of `items` minus the items that have an
    /// equivalent entry in the cache.
    ///
    /// This is useful for selecting a sub-set of `items` which don't have an entry in the cache.
    ///
    /// An `Item` has an entry in the cache if `key_extractor` executed for the item returns a
    /// `K` key that has an entry in the cache.
    pub async fn subtract_cached_items_from<Item, Collection>(
        &self,
        items: impl IntoIterator<Item = Item>,
        key_extractor: impl Fn(&Item) -> &K,
    ) -> Collection
    where
        Collection: FromIterator<Item>,
    {
        let cache = self.cache.lock().await;

        items
            .into_iter()
            .filter(|item| !cache.contains(key_extractor(item)))
            .collect()
    }

    /// Returns a `V` from the cache, if present.
    pub async fn get(&self, hash: &K) -> Option<V> {
        let maybe_value = self.cache.lock().await.get(hash).cloned();

        #[cfg(with_metrics)]
        {
            let metric = if maybe_value.is_some() {
                &CACHE_HIT_COUNT
            } else {
                &CACHE_MISS_COUNT
            };

            metric
                .with_label_values(&[type_name::<K>(), type_name::<V>()])
                .inc();
        }

        maybe_value
    }

    /// Tries to retrieve many values from the cache.
    ///
    /// Returns one collection with the values found, and another collection with the keys that
    /// aren't present in the cache.
    pub async fn try_get_many<FoundCollection, NotFoundCollection>(
        &self,
        keys: NotFoundCollection,
    ) -> (FoundCollection, NotFoundCollection)
    where
        FoundCollection: FromIterator<(K, V)>,
        NotFoundCollection: IntoIterator<Item = K> + FromIterator<K> + Default + Extend<K>,
    {
        let mut cache = self.cache.lock().await;
        let (found_keys, not_found_keys): (NotFoundCollection, NotFoundCollection) =
            keys.into_iter().partition(|key| cache.contains(key));

        let found_pairs = found_keys
            .into_iter()
            .map(|key| {
                let value = cache
                    .get(&key)
                    .expect("Key should be in cache after the partitioning above");
                (key, value.clone())
            })
            .collect();

        (found_pairs, not_found_keys)
    }
}

impl<T: Clone> ValueCache<CryptoHash, Hashed<T>> {
    /// Inserts a [`HashedCertificateValue`] into the cache, if it's not already present.
    ///
    /// The `value` is wrapped in a [`Cow`] so that it is only cloned if it needs to be
    /// inserted in the cache.
    ///
    /// Returns [`true`] if the value was not already present in the cache.
    pub async fn insert<'a>(&self, value: Cow<'a, Hashed<T>>) -> bool {
        let hash = (*value).hash();
        let mut cache = self.cache.lock().await;
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
    #[cfg(with_testing)]
    pub async fn insert_all<'a>(&self, values: impl IntoIterator<Item = Cow<'a, Hashed<T>>>)
    where
        T: 'a,
    {
        let mut cache = self.cache.lock().await;
        for value in values {
            let hash = (*value).hash();
            if !cache.contains(&hash) {
                cache.push(hash, value.into_owned());
            }
        }
    }
}

impl ValueCache<BlobId, Blob> {
    /// Inserts a [`Blob`] into the cache, if it's not already present.
    ///
    /// The `value` is wrapped in a [`Cow`] so that it is only cloned if it needs to be
    /// inserted in the cache.
    ///
    /// Returns [`true`] if the value was not already present in the cache.
    pub async fn insert<'a>(&self, value: Cow<'a, Blob>) -> bool {
        let blob_id = (*value).id();
        let mut cache = self.cache.lock().await;
        if cache.contains(&blob_id) {
            // Promote the re-inserted value in the cache, as if it was accessed again.
            cache.promote(&blob_id);
            false
        } else {
            // Cache the blob so that clients don't have to send it again.
            cache.push(blob_id, value.into_owned());
            true
        }
    }
}
