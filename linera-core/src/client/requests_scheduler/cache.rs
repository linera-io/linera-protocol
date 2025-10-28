// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use linera_base::time::{Duration, Instant};

#[cfg(with_metrics)]
use super::scheduler::metrics;

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
/// - LRU eviction
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
        T: TryFrom<R>,
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
            return T::try_from((*entry.result).clone()).ok();
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
                    return T::try_from(extracted).ok();
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
        // Not strictly smaller b/c we want to add a new entry after eviction.
        if cache.len() < self.max_cache_size {
            return 0; // No need to evict if under max size
        }
        let mut expired_keys = 0usize;

        cache.retain(|_key, entry| {
            if now.duration_since(entry.cached_at) > self.cache_ttl {
                expired_keys += 1;
                false
            } else {
                true
            }
        });

        if expired_keys > 0 {
            tracing::trace!(count = expired_keys, "evicted expired cache entries");
        }

        expired_keys
    }
}

/// Trait for request keys that support subsumption-based matching and result extraction.
pub(super) trait SubsumingKey<R> {
    /// Checks if this request fully subsumes another request.
    ///
    /// Request `self` subsumes request `other` if `self`'s result would contain all the data that
    /// `other`'s result would contain. This means `other`'s request is redundant if `self` is already
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use linera_base::time::Duration;

    use super::*;

    // Mock key type for testing: represents a range request [start, end]
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct RangeKey {
        start: u64,
        end: u64,
    }

    // Mock result type: vector of values in the range
    #[derive(Debug, Clone, PartialEq)]
    struct RangeResult(Vec<u64>);

    impl SubsumingKey<RangeResult> for RangeKey {
        fn subsumes(&self, other: &Self) -> bool {
            // This range subsumes another if it contains the other's range
            self.start <= other.start && self.end >= other.end
        }

        fn try_extract_result(&self, from: &Self, result: &RangeResult) -> Option<RangeResult> {
            if !from.subsumes(self) {
                return None;
            }
            // Extract values that fall within our range
            let filtered: Vec<u64> = result
                .0
                .iter()
                .filter(|&&v| v >= self.start && v <= self.end)
                .copied()
                .collect();
            Some(RangeResult(filtered))
        }
    }

    #[tokio::test]
    async fn test_cache_miss_on_empty_cache() {
        let cache: RequestsCache<RangeKey, RangeResult> =
            RequestsCache::new(Duration::from_secs(60), 10);
        let key = RangeKey { start: 0, end: 5 };
        let result: Option<RangeResult> = cache.get(&key).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_exact_match_hit() {
        let cache = RequestsCache::new(Duration::from_secs(60), 10);
        let key = RangeKey { start: 0, end: 5 };
        let result = RangeResult(vec![0, 1, 2, 3, 4, 5]);

        cache.store(key.clone(), Arc::new(result.clone())).await;
        let retrieved: Option<RangeResult> = cache.get(&key).await;

        assert_eq!(retrieved, Some(result));
    }

    #[tokio::test]
    async fn test_exact_match_takes_priority_over_subsumption() {
        let cache = RequestsCache::new(Duration::from_secs(60), 10);

        // Store a larger range
        let large_key = RangeKey { start: 0, end: 10 };
        let large_result = RangeResult(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        cache
            .store(large_key.clone(), Arc::new(large_result.clone()))
            .await;

        // Store an exact match
        let exact_key = RangeKey { start: 2, end: 5 };
        let exact_result = RangeResult(vec![2, 3, 4, 5]);
        cache
            .store(exact_key.clone(), Arc::new(exact_result.clone()))
            .await;

        // Should get exact match, not extracted from larger range
        let retrieved: Option<RangeResult> = cache.get(&exact_key).await;
        assert_eq!(retrieved, Some(exact_result));
    }

    #[tokio::test]
    async fn test_subsumption_hit() {
        let cache = RequestsCache::new(Duration::from_secs(60), 10);

        // Store a larger range
        let large_key = RangeKey { start: 0, end: 10 };
        let large_result = RangeResult(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        cache.store(large_key, Arc::new(large_result.clone())).await;

        // Request a subset
        let subset_key = RangeKey { start: 3, end: 7 };
        let retrieved: Option<RangeResult> = cache.get(&subset_key).await;

        assert_eq!(retrieved, Some(RangeResult(vec![3, 4, 5, 6, 7])));
    }

    #[tokio::test]
    async fn test_subsumption_miss_when_no_overlap() {
        let cache = RequestsCache::new(Duration::from_secs(60), 10);

        let key1 = RangeKey { start: 0, end: 5 };
        let result1 = RangeResult(vec![0, 1, 2, 3, 4, 5]);
        cache.store(key1, Arc::new(result1)).await;

        // Non-overlapping range
        let key2 = RangeKey { start: 10, end: 15 };
        let retrieved: Option<RangeResult> = cache.get(&key2).await;

        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_eviction_when_exceeding_max_size() {
        let cache_size = 3u64;
        let cache = RequestsCache::new(Duration::from_millis(50), cache_size as usize);

        // Fill cache to max size
        for i in 0..cache_size {
            let key = RangeKey {
                start: i * 10,
                end: i * 10,
            };
            cache.store(key, Arc::new(RangeResult(vec![i * 10]))).await;
            // Small delay to ensure different timestamps
            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
        }

        // Wait for first entry to expire
        tokio::time::sleep(tokio::time::Duration::from_millis(60)).await;

        // Cache is now at max size (3) with expired entries, so next store triggers eviction.
        let key_4 = RangeKey {
            start: 100,
            end: 100,
        };
        cache
            .store(key_4.clone(), Arc::new(RangeResult(vec![100])))
            .await;

        let cache_guard = cache.cache.read().await;
        // Expired entries should have been evicted
        let first_key = RangeKey { start: 0, end: 5 };
        assert!(!cache_guard.contains_key(&first_key));

        // Latest entries should still be there
        assert!(cache_guard.contains_key(&key_4));
    }
    #[tokio::test]
    async fn test_subsumption_with_extraction_failure_tries_next() {
        // Mock key that subsumes but extraction returns None
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct FailingKey {
            id: u64,
            always_fail_extraction: bool,
        }

        #[derive(Debug, Clone, PartialEq)]
        struct SimpleResult(u64);

        impl SubsumingKey<SimpleResult> for FailingKey {
            fn subsumes(&self, other: &Self) -> bool {
                self.id >= other.id
            }

            fn try_extract_result(
                &self,
                from: &Self,
                _result: &SimpleResult,
            ) -> Option<SimpleResult> {
                if from.always_fail_extraction {
                    None
                } else {
                    Some(SimpleResult(self.id))
                }
            }
        }

        let cache = RequestsCache::<FailingKey, SimpleResult>::new(Duration::from_secs(60), 10);

        // Store entry that subsumes but fails extraction
        let failing_key = FailingKey {
            id: 10,
            always_fail_extraction: true,
        };
        cache.store(failing_key, Arc::new(SimpleResult(10))).await;

        // Store entry that subsumes and succeeds extraction
        let working_key = FailingKey {
            id: 20,
            always_fail_extraction: false,
        };
        cache.store(working_key, Arc::new(SimpleResult(20))).await;

        // Request should find the working one
        let target_key = FailingKey {
            id: 5,
            always_fail_extraction: false,
        };
        let retrieved: Option<SimpleResult> = cache.get(&target_key).await;

        assert!(retrieved.is_some());
    }
}
