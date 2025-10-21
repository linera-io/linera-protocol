// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use linera_base::time::{Duration, Instant};

#[cfg(with_metrics)]
use super::manager::metrics;

/// Cached result entry with timestamp for TTL expiration
#[derive(Debug, Clone)]
pub(super) struct CacheEntry<R> {
    result: Arc<R>,
    cached_at: Instant,
}

/// Cache for request results with TTL-based expiration and LRU eviction.
///
/// This cache supports:
/// - Exact match lookups
/// - Subsumption-based lookups (larger requests can satisfy smaller ones)
/// - TTL-based expiration
/// - Size-based LRU eviction
#[derive(Debug, Clone)]
pub(super) struct RequestsCache<K, R> {
    /// Cache of recently completed requests with their results and timestamps.
    /// Used to avoid re-executing requests for the same data within the TTL window.
    cache: Arc<tokio::sync::RwLock<HashMap<K, CacheEntry<R>>>>,
    /// Time-to-live for cached entries. Entries older than this duration are considered expired.
    cache_ttl: Duration,
    /// Maximum number of entries to store in the cache. When exceeded, oldest entries are evicted (LRU).
    max_cache_size: usize,
}

impl<K, R> RequestsCache<K, R>
where
    K: Eq + std::hash::Hash + std::fmt::Debug + Clone + SubsumingKey<R>,
    R: Clone + std::fmt::Debug,
{
    /// Creates a new `RequestsCache` with the specified TTL and maximum size.
    ///
    /// # Arguments
    /// - `cache_ttl`: Time-to-live for cached entries
    /// - `max_cache_size`: Maximum number of entries in the cache
    pub(super) fn new(cache_ttl: Duration, max_cache_size: usize) -> Self {
        Self {
            cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            cache_ttl,
            max_cache_size,
        }
    }

    /// Attempts to retrieve a cached result for the given key.
    ///
    /// This method performs both exact match lookups and subsumption-based lookups.
    /// If a larger request that contains all the data needed by this request is cached,
    /// we can extract the subset result instead of making a new request.
    ///
    /// # Returns
    /// - `Some(T)` if a cached result is found (either exact or subsumed)
    /// - `None` if no suitable cached result exists
    pub(super) async fn get<T>(&self, key: &K) -> Option<T>
    where
        T: From<R>,
    {
        let cache = self.cache.read().await;

        // Check cache for exact match first
        if let Some(entry) = cache.get(key) {
            tracing::trace!(
                key = ?key,
                "cache hit (exact match) - returning cached result"
            );
            #[cfg(with_metrics)]
            metrics::REQUEST_CACHE_HIT.inc();
            return Some(T::from((*entry.result).clone()));
        }

        // Check cache for subsuming requests
        for (cached_key, entry) in cache.iter() {
            if cached_key.subsumes(key) {
                if let Some(extracted) = key.try_extract_result(cached_key, &entry.result) {
                    tracing::trace!(
                        key = ?key,
                        "cache hit (subsumption) - extracted result from larger cached request"
                    );
                    #[cfg(with_metrics)]
                    metrics::REQUEST_CACHE_HIT.inc();
                    return Some(T::from(extracted));
                }
            }
        }

        None
    }

    /// Stores a result in the cache with LRU eviction if cache is full.
    ///
    /// If the cache is at capacity, this method removes the oldest expired entries first.
    /// Entries are considered "oldest" based on their cached_at timestamp.
    ///
    /// # Arguments
    /// - `key`: The request key to cache
    /// - `result`: The result to cache
    pub(super) async fn store(&self, key: K, result: Arc<R>) {
        self.evict_expired_entries().await; // Clean up expired entries first
        let mut cache = self.cache.write().await;
        // Insert new entry
        cache.insert(
            key.clone(),
            CacheEntry {
                result,
                cached_at: Instant::now(),
            },
        );
        tracing::trace!(
            key = ?key,
            "stored result in cache"
        );
    }

    /// Removes all cache entries that are older than the configured cache TTL.
    ///
    /// This method scans the cache and removes entries where the time elapsed since
    /// `cached_at` exceeds `cache_ttl`. It's useful for explicitly cleaning up stale
    /// cache entries rather than relying on lazy expiration checks.
    ///
    /// # Returns
    /// The number of entries that were evicted
    async fn evict_expired_entries(&self) -> usize {
        let mut cache = self.cache.write().await;
        let now = Instant::now();
        if cache.len() <= self.max_cache_size {
            return 0; // No need to evict if under max size
        }

        let expired_keys: Vec<_> = cache
            .iter()
            .filter_map(|(key, entry)| {
                if now.duration_since(entry.cached_at) > self.cache_ttl {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        for key in &expired_keys {
            cache.remove(key);
        }

        if !expired_keys.is_empty() {
            tracing::trace!(count = expired_keys.len(), "evicted expired cache entries");
        }
        expired_keys.len()
    }
}

/// Trait for request keys that support subsumption-based matching and result extraction.
pub(super) trait SubsumingKey<R> {
    /// Checks if this request fully subsumes another request.
    ///
    /// Request A subsumes request B if A's result would contain all the data that
    /// B's result would contain. This means B's request is redundant if A is already
    /// in-flight or cached.
    fn subsumes(&self, other: &Self) -> bool;

    /// Attempts to extract a subset result for this request from a larger request's result.
    ///
    /// This is used when a request A subsumes this request B. We can extract B's result
    /// from A's result by filtering the certificates to only those requested by B.
    ///
    /// # Arguments
    /// - `from`: The key of the larger request that subsumes this one
    /// - `result`: The result from the larger request
    ///
    /// # Returns
    /// - `Some(result)` with the extracted subset if possible
    /// - `None` if extraction is not possible (wrong variant, different chain, etc.)
    fn try_extract_result(&self, from: &Self, result: &R) -> Option<R>;
}
