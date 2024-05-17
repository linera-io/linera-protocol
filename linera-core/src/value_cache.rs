// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A least-recently used cache of [`HashedCertificateValue`]s.

#[cfg(test)]
#[path = "unit_tests/value_cache_tests.rs"]
mod unit_tests;

use std::{borrow::Cow, num::NonZeroUsize};

use linera_base::crypto::CryptoHash;
use linera_chain::data_types::{Certificate, HashedCertificateValue, LiteCertificate};
use lru::LruCache;
use tokio::sync::Mutex;
#[cfg(with_metrics)]
use {
    linera_base::{prometheus_util, sync::Lazy},
    prometheus::IntCounterVec,
};

use crate::worker::WorkerError;

/// The default cache size.
const DEFAULT_VALUE_CACHE_SIZE: usize = 1000;

/// A counter metric for the number of cache hits in the [`CertificateValueCache`].
#[cfg(with_metrics)]
static CACHE_HIT_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "certificate_value_cache_hit",
        "Cache hits in `CertificateValueCache`",
        &[],
    )
    .expect("Counter creation should not fail")
});

/// A counter metric for the number of cache misses in the [`CertificateValueCache`].
#[cfg(with_metrics)]
static CACHE_MISS_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    prometheus_util::register_int_counter_vec(
        "certificate_value_cache_miss",
        "Cache misses in `CertificateValueCache`",
        &[],
    )
    .expect("Counter creation should not fail")
});

/// A least-recently used cache of [`HashedCertificateValue`]s.
pub struct CertificateValueCache {
    cache: Mutex<LruCache<CryptoHash, HashedCertificateValue>>,
}

impl Default for CertificateValueCache {
    fn default() -> Self {
        let size = NonZeroUsize::try_from(DEFAULT_VALUE_CACHE_SIZE)
            .expect("Default cache size is larger than zero");

        CertificateValueCache {
            cache: Mutex::new(LruCache::new(size)),
        }
    }
}

impl CertificateValueCache {
    /// Returns a `Collection` of the hashes in the cache.
    pub async fn keys<Collection>(&self) -> Collection
    where
        Collection: FromIterator<CryptoHash>,
    {
        self.cache
            .lock()
            .await
            .iter()
            .map(|(key, _)| *key)
            .collect()
    }

    /// Returns [`true`] if the cache contains the [`HashedCertificateValue`] with the
    /// requested [`CryptoHash`].
    pub async fn contains(&self, hash: &CryptoHash) -> bool {
        self.cache.lock().await.contains(hash)
    }

    /// Returns a `Collection` created from a set of `items` minus the items that have an
    /// equivalent entry in the cache.
    ///
    /// This is useful for selecting a sub-set of `items` which don't have an entry in the cache.
    ///
    /// An `Item` has an entry in the cache if `key_extractor` executed for the item returns a
    /// [`CryptoHash`] key that has an entry in the cache.
    pub async fn subtract_cached_items_from<Item, Collection>(
        &self,
        items: impl IntoIterator<Item = Item>,
        key_extractor: impl Fn(&Item) -> &CryptoHash,
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

    /// Returns a [`HashedCertificateValue`] from the cache, if present.
    pub async fn get(&self, hash: &CryptoHash) -> Option<HashedCertificateValue> {
        let maybe_value = self.cache.lock().await.get(hash).cloned();

        #[cfg(with_metrics)]
        {
            let metric = if maybe_value.is_some() {
                &CACHE_HIT_COUNT
            } else {
                &CACHE_MISS_COUNT
            };

            metric.with_label_values(&[]).inc();
        }

        maybe_value
    }

    /// Populates a [`LiteCertificate`] with its [`CertificateValue`], if it's present in
    /// the cache.
    pub async fn full_certificate(
        &self,
        certificate: LiteCertificate<'_>,
    ) -> Result<Certificate, WorkerError> {
        let value = self
            .get(&certificate.value.value_hash)
            .await
            .ok_or(WorkerError::MissingCertificateValue)?;
        certificate
            .with_value(value)
            .ok_or(WorkerError::InvalidLiteCertificate)
    }

    /// Inserts a [`HashedCertificateValue`] into the cache, if it's not already present.
    ///
    /// The `value` is wrapped in a [`Cow`] so that it is only cloned if it needs to be
    /// inserted in the cache.
    ///
    /// Returns [`true`] if the value was not already present in the cache.
    ///
    /// # Notes
    ///
    /// If the `value` is a [`HashedCertificateValue::ValidatedBlock`], its respective
    /// [`HashedCertificateValue::ConfirmedBlock`] is also cached.
    pub async fn insert<'a>(&self, value: Cow<'a, HashedCertificateValue>) -> bool {
        let hash = value.hash();
        let mut cache = self.cache.lock().await;
        if cache.contains(&hash) {
            return false;
        }
        if let Some(confirmed_value) = value.validated_to_confirmed() {
            // Cache the certificate for the confirmed block in advance, so that the clients don't
            // have to send it.
            cache.push(confirmed_value.hash(), confirmed_value);
        }
        // Cache the certificate so that clients don't have to send the value again.
        cache.push(hash, value.into_owned());
        true
    }

    /// Inserts multiple [`HashedCertificateValue`]s into the cache. If they're not
    /// already present.
    ///
    /// The `values` are wrapped in [`Cow`]s so that each `value` is only cloned if it
    /// needs to be inserted in the cache.
    pub async fn insert_all<'a>(
        &self,
        values: impl IntoIterator<Item = Cow<'a, HashedCertificateValue>>,
    ) {
        let mut cache = self.cache.lock().await;
        for value in values {
            let hash = value.hash();
            if !cache.contains(&hash) {
                cache.push(hash, value.into_owned());
            }
        }
    }
}
