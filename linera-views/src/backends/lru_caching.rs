// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Add LRU (least recently used) caching to a given store.

use std::{
    collections::{btree_map, hash_map::RandomState, BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};

#[cfg(with_testing)]
use crate::memory::MemoryDatabase;
#[cfg(with_testing)]
use crate::store::TestKeyValueDatabase;
use crate::{
    batch::{Batch, WriteOperation},
    common::get_interval,
    store::{KeyValueDatabase, ReadableKeyValueStore, WithError, WritableKeyValueStore},
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::register_int_counter_vec;
    use prometheus::IntCounterVec;

    /// The total number of cache read value misses
    pub static READ_VALUE_CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_read_value_cache_miss",
            "Number of read value cache misses",
            &[],
        )
    });

    /// The total number of read value cache hits
    pub static READ_VALUE_CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_read_value_cache_hits",
            "Number of read value cache hits",
            &[],
        )
    });

    /// The total number of contains key cache misses
    pub static CONTAINS_KEY_CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_contains_key_cache_miss",
            "Number of contains key cache misses",
            &[],
        )
    });

    /// The total number of contains key cache hits
    pub static CONTAINS_KEY_CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_contains_key_cache_hit",
            "Number of contains key cache hits",
            &[],
        )
    });

    /// The total number of find_keys_by_prefix cache misses
    pub static FIND_KEYS_BY_PREFIX_CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_find_keys_by_prefix_cache_miss",
            "Number of find keys by prefix cache misses",
            &[],
        )
    });

    /// The total number of find_keys_by_prefix cache hits
    pub static FIND_KEYS_BY_PREFIX_CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_find_keys_by_prefix_cache_hit",
            "Number of find keys by prefix cache hits",
            &[],
        )
    });

    /// The total number of find_key_values_by_prefix cache misses
    pub static FIND_KEY_VALUES_BY_PREFIX_CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_find_key_values_by_prefix_cache_miss",
            "Number of find key values by prefix cache misses",
            &[],
        )
    });

    /// The total number of find_key_values_by_prefix cache hits
    pub static FIND_KEY_VALUES_BY_PREFIX_CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "num_find_key_values_by_prefix_cache_hit",
            "Number of find key values by prefix cache hits",
            &[],
        )
    });
}

/// The parametrization of the cache.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageCacheConfig {
    /// The maximum size of the cache, in bytes (keys size + value sizes)
    pub max_cache_size: usize,
    /// The maximum size of an entry size, in bytes
    pub max_entry_size: usize,
    /// The maximum number of entries in the cache.
    pub max_cache_entries: usize,
}

/// The maximum number of entries in the cache.
/// If the number of entries in the cache is too large then the underlying maps
/// become the limiting factor
pub const DEFAULT_STORAGE_CACHE_CONFIG: StorageCacheConfig = StorageCacheConfig {
    max_cache_size: 10000000,
    max_entry_size: 1000000,
    max_cache_entries: 1000,
};

#[derive(Eq, Hash, PartialEq)]
enum CacheKey {
    Value(Vec<u8>),
    FindKeys(Vec<u8>),
    FindKeyValues(Vec<u8>),
}

enum ValueCacheEntry {
    DoesNotExist,
    Exists,
    Value(Vec<u8>),
}

impl ValueCacheEntry {
    fn size(&self) -> usize {
        match self {
            ValueCacheEntry::Value(vec) => vec.len(),
            _ => 0,
        }
    }
}

struct FindKeysEntry(BTreeSet<Vec<u8>>);

impl FindKeysEntry {
    fn size(&self) -> usize {
        self.0.iter().map(|key| key.len()).sum()
    }

    fn get_find_keys(&self, key_prefix: &[u8]) -> Vec<Vec<u8>> {
        let key_prefix = key_prefix.to_vec();
        let delta = key_prefix.len();
        self.0
            .range(get_interval(key_prefix))
            .map(|key| key[delta..].to_vec())
            .collect()
    }

    fn get_contains_key(&self, key: &[u8]) -> bool {
        self.0.contains(key)
    }

    fn update_cache_entry(&mut self, key: &[u8], new_value: Option<Vec<u8>>) {
        match new_value {
            None => {
                self.0.remove(key);
            },
            Some(_) => {
                self.0.insert(key.to_vec());
            },
        }
    }

    fn delete_prefix(&mut self, key_prefix: &[u8]) {
        let keys = self.0
            .range(get_interval(key_prefix.to_vec()))
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            self.0.remove(&key);
        }
    }
}

struct FindKeyValuesEntry(BTreeMap<Vec<u8>,Vec<u8>>);

impl FindKeyValuesEntry {
    fn size(&self) -> usize {
        self.0
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum()
    }

    fn get_find_keys(&self, key_prefix: &[u8]) -> Vec<Vec<u8>> {
        let key_prefix = key_prefix.to_vec();
        let delta = key_prefix.len();
        self.0
            .range(get_interval(key_prefix))
            .map(|(key, _)| key[delta..].to_vec())
            .collect()
    }

    fn get_find_key_values(&self, key_prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let key_prefix = key_prefix.to_vec();
        let delta = key_prefix.len();
        self.0
            .range(get_interval(key_prefix))
            .map(|(key, value)| (key[delta..].to_vec(), value.to_vec()))
            .collect()
    }

    fn get_contains_key(&self, key: &[u8]) -> bool {
        self.0.contains_key(key)
    }

    fn get_read_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.get(key).cloned()
    }

    fn update_cache_entry(&mut self, key: &[u8], new_value: Option<Vec<u8>>) {
        match new_value {
            None => {
                self.0.remove(key);
            },
            Some(new_value) => {
                self.0.insert(key.to_vec(), new_value);
            },
	}
    }

    fn delete_prefix(&mut self, key_prefix: &[u8]) {
        let keys = self.0
            .range(get_interval(key_prefix.to_vec()))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in keys {
            self.0.remove(&key);
        }
    }
}






/// Stores the data for simple `read_values` queries.
///
/// This data structure is inspired by the crate `lru-cache` but was modified to support
/// range deletions.
struct LruPrefixCache {
    value_map: BTreeMap<Vec<u8>, ValueCacheEntry>,
    find_keys_map: BTreeMap<Vec<u8>, FindKeysEntry>,
    find_key_values_map: BTreeMap<Vec<u8>, FindKeyValuesEntry>,
    queue: LinkedHashMap<CacheKey, usize, RandomState>,
    config: StorageCacheConfig,
    total_size: usize,
    total_value_size: usize,
    total_find_keys_size: usize,
    total_find_key_values_size: usize,
    /// Whether we have exclusive R/W access to the keys under the root key of the store.
    has_exclusive_access: bool,
}

impl LruPrefixCache {
    /// Creates an `LruPrefixCache`.
    fn new(config: StorageCacheConfig, has_exclusive_access: bool) -> Self {
        Self {
            value_map: BTreeMap::new(),
            find_keys_map: BTreeMap::new(),
            find_key_values_map: BTreeMap::new(),
            queue: LinkedHashMap::new(),
            config,
            total_size: 0,
            total_value_size: 0,
            total_find_keys_size: 0,
            total_find_key_values_size: 0,
            has_exclusive_access,
        }
    }

    /// A used key needs to be put on top
    fn put_cache_key_on_top(&mut self, cache_key: CacheKey) {
        let size = self.queue.remove(&cache_key).unwrap();
        self.queue.insert(cache_key, size);
    }

    /// Remove an entry from the queue
    fn remove_cache_key(&mut self, cache_key: &CacheKey) {
        let size = self.queue.remove(&cache_key).unwrap();
        self.total_size -= size;
        match cache_key {
            CacheKey::Value(_) => {
                self.total_value_size -= size;
            },
            CacheKey::FindKeys(_) => {
                self.total_find_keys_size -= size;
            },
            CacheKey::FindKeyValues(_) => {
                self.total_find_key_values_size -= size;
            },
        }
    }

    /// Update the cache size to the new size without changing position
    fn update_cache_key_size(&mut self, cache_key: &CacheKey, new_size: usize) {
    	let size = self.queue.get_mut(&cache_key).unwrap();
        let old_size = *size;
        *size = new_size;
        self.total_size -= old_size;
        self.total_size += new_size;
        match cache_key {
            CacheKey::Value(_) => {
                self.total_value_size -= old_size;
                self.total_value_size += new_size;
            },
            CacheKey::FindKeys(_) => {
                self.total_find_keys_size -= old_size;
                self.total_find_keys_size += new_size;
            },
            CacheKey::FindKeyValues(_) => {
                self.total_find_key_values_size -= old_size;
                self.total_find_key_values_size += new_size;
            },
        }
    }

    /// Insert a cache entry into the key
    fn insert_cache_key(&mut self, cache_key: CacheKey, size: usize) {
        self.total_size += size;
        match cache_key {
            CacheKey::Value(_) => {
                self.total_value_size += size;
            },
            CacheKey::FindKeys(_) => {
                self.total_find_keys_size += size;
            },
            CacheKey::FindKeyValues(_) => {
                self.total_find_key_values_size += size;
            },
        }
        self.queue.insert(cache_key, size);
    }

    fn get_find_keys_lower_bound(&self, key: &[u8]) -> Option<(&Vec<u8>, &FindKeysEntry)> {
        match self.find_keys_map.range(..=key.to_vec()).next_back() {
            None => None,
            Some((key_store, value)) => {
                if key.starts_with(key_store) {
                    Some((key_store, value))
                } else {
                    None
                }
            }
        }
    }

    fn get_find_keys_lower_bound_update(&mut self, key: &[u8]) -> Option<(&Vec<u8>, &mut FindKeysEntry)> {
        match self.find_keys_map.range_mut(..=key.to_vec()).next_back() {
            None => None,
            Some((key_store, value)) => {
                if key.starts_with(key_store) {
                    Some((key_store, value))
                } else {
                    None
                }
            }
        }
    }

    fn get_find_key_values_lower_bound(&self, key: &[u8]) -> Option<(&Vec<u8>, &FindKeyValuesEntry)> {
        match self.find_key_values_map.range(..=key.to_vec()).next_back() {
            None => None,
            Some((key_store, value)) => {
                if key.starts_with(key_store) {
                    Some((key_store, value))
                } else {
                    None
                }
            }
        }
    }

    fn get_find_key_values_lower_bound_update(&mut self, key: &[u8]) -> Option<(&Vec<u8>, &mut FindKeyValuesEntry)> {
        match self.find_key_values_map.range_mut(..=key.to_vec()).next_back() {
            None => None,
            Some((key_store, value)) => {
                if key.starts_with(key_store) {
                    Some((key_store, value))
                } else {
                    None
                }
            }
        }
    }

    /// Trim the cache so that it fits within the constraints.
    fn trim_cache(&mut self) {
        // NOTE: The removal of entries might be too aggressive for the
        // total_size / total_value_size / total_find_size
        while self.total_size > self.config.max_cache_size
            || self.queue.len() > self.config.max_cache_entries
        {
            let Some((cache_key, size)) = self.queue.pop_front() else {
                break;
            };
            match cache_key {
                CacheKey::Value(key) => {
                    self.value_map.remove(&key);
                    self.total_size -= size;
                    self.total_value_size -= size;
                },
                CacheKey::FindKeys(key) => {
                    self.find_keys_map.remove(&key);
                    self.total_size -= size;
                    self.total_find_keys_size -= size;
                },
                CacheKey::FindKeyValues(key) => {
                    self.find_key_values_map.remove(&key);
                    self.total_size -= size;
                    self.total_find_key_values_size -= size;
                },
            }
        }
    }

    /// Inserts an entry into the cache.
    fn insert_value(&mut self, key: Vec<u8>, cache_entry: ValueCacheEntry) {
        let size = key.len() + cache_entry.size();
        if (matches!(cache_entry, ValueCacheEntry::DoesNotExist) && !self.has_exclusive_access)
            || size > self.config.max_entry_size
        {
            if self.value_map.remove(&key).is_some() {
                let cache_key = CacheKey::Value(key);
                self.remove_cache_key(&cache_key);
            };
            return;
        }
        match self.value_map.entry(key.clone()) {
            btree_map::Entry::Occupied(mut entry) => {
                entry.insert(cache_entry);
                // Put it on first position for LRU with the new size
                let cache_key = CacheKey::Value(key);
                self.remove_cache_key(&cache_key);
                self.insert_cache_key(cache_key, size);
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(cache_entry);
                let cache_key = CacheKey::Value(key);
                self.insert_cache_key(cache_key, size);
            }
        }
        self.trim_cache();
    }

    /// Invalidates corresponding find entries
    pub fn correct_find_entry(&mut self, key: &[u8], new_value: Option<Vec<u8>>) {
        let lower_bound = self.get_find_keys_lower_bound_update(key);
	if let Some((lower_bound, cache_entry)) = lower_bound {
            let key_red = &key[lower_bound.len()..];
            cache_entry.update_cache_entry(key_red, new_value.clone());
        }
        let lower_bound = self.get_find_key_values_lower_bound_update(key);
	if let Some((lower_bound, cache_entry)) = lower_bound {
            let key_red = &key[lower_bound.len()..];
            cache_entry.update_cache_entry(key_red, new_value);
        }
    }

    /// Inserts a read_value entry into the cache.
    fn insert_read_value(&mut self, key: Vec<u8>, value: &Option<Vec<u8>>) {
        let cache_entry = match value {
            None => ValueCacheEntry::DoesNotExist,
            Some(vec) => ValueCacheEntry::Value(vec.to_vec()),
        };
        self.insert_value(key, cache_entry)
    }

    /// Inserts a read_value entry into the cache.
    fn insert_contains_key(&mut self, key: Vec<u8>, result: bool) {
        let cache_entry = match result {
            false => ValueCacheEntry::DoesNotExist,
            true => ValueCacheEntry::Exists,
        };
        self.insert_value(key, cache_entry)
    }

    /*
    pub fn insert_find(&mut self, key_prefix: Vec<u8>, find_entry: FindCacheEntry) {
        let size = cache_entry.size() + key_prefix.len();
        if cache_size > self.storage_cache_policy.max_entry_size {
            // The entry is too large, we do not insert it,
            return;
        }
        // Clearing up the find entries
        let keys = self
            .find_map
            .range(get_interval(key_prefix.clone()))
            .map(|(x, _)| x.clone())
            .collect::<Vec<_>>();
        for key in keys {
            self.map_find.remove(&key);
            let cache_key = CacheKey::Find(key);
            let old_size = self.queue.remove(&cache_key).unwrap();
            self.total_size -= old_size;
            self.total_find_size -= old_size;
        }
        if let FindCacheEntry::KeyValues(_) = find_entry {
            // Clearing up the value entries if it is a KeyValues as
            // it is obsoletes
            let keys = self
                .value_map
                .range(get_interval(key_prefix.clone()))
                .map(|(x, _)| x.clone())
                .collect::<Vec<_>>();
            for key in keys {
                self.value_map.remove(&key);
                let full_key = CacheKey::Value(key);
                let old_size = self.queue.remove(&cache_key).unwrap();
                self.total_size -= old_size;
                self.total_value_size -= old_size;
            }
        }
        let cache_key = CacheKey::Find(key_prefix.clone());
        match self.map_find.entry(key_prefix.clone()) {
            btree_map::Entry::Occupied(mut entry) => {
                entry.insert(find_entry);
                let old_size = self.queue.remove(&cache_key).unwrap();
                self.total_size -= old_size;
                self.total_value_size -= old_size;
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(find_entry);
            }
        }
        self.insert_queue(full_prefix, cache_size);
        self.trim_cache();
    }
    */

    /// Marks cached keys that match the prefix as deleted. Importantly, this does not
    /// create new entries in the cache.
    fn delete_prefix(&mut self, key_prefix: &[u8]) {
        if self.has_exclusive_access {
            for (key, value) in self.value_map.range_mut(get_interval(key_prefix.to_vec())) {
                let cache_key = CacheKey::Value(key.to_vec());
                *self.queue.get_mut(&cache_key).unwrap() = key.len();
                self.total_size -= value.size();
                self.total_value_size -= value.size();
                *value = ValueCacheEntry::DoesNotExist;
            }
            // Remove the find_keys_by_prefix(es) that are covered by that key_prefix
            let mut prefixes = Vec::new();
            for (prefix, _) in self.find_keys_map.range(get_interval(key_prefix.to_vec())) {
                prefixes.push(prefix.to_vec());
            }
            for prefix in prefixes {
                self.find_keys_map.remove(&prefix);
                let cache_key = CacheKey::FindKeys(prefix);
                self.remove_cache_key(&cache_key);
            }
            // Remove the find_key_values_by_prefix(es) that are covered by that key_prefix
            let mut prefixes = Vec::new();
            for (prefix, _) in self.find_key_values_map.range(get_interval(key_prefix.to_vec())) {
                prefixes.push(prefix.to_vec());
            }
            for prefix in prefixes {
                self.find_key_values_map.remove(&prefix);
                let cache_key = CacheKey::FindKeyValues(prefix);
                self.remove_cache_key(&cache_key);
            }
            // Finding a lower bound. If existing update, if not insert.
            let lower_bound = self.get_find_keys_lower_bound_update(key_prefix);
            let result = if let Some((lower_bound, find_entry)) = lower_bound {
                // Delete the keys in the entry
                let key_prefix_red = &key_prefix[lower_bound.len()..];
                find_entry.delete_prefix(key_prefix_red);
                let new_cache_size = find_entry.size() + key_prefix.len();
                Some((new_cache_size, lower_bound.clone()))
            } else {
                None
            };
	    if let Some((new_cache_size, lower_bound)) = result {
                // Update the size without changing the position.
                let cache_key = CacheKey::FindKeys(lower_bound.clone());
                self.update_cache_key_size(&cache_key, new_cache_size);
            }
            // Finding a lower bound. If existing update, if not insert.
            let lower_bound = self.get_find_key_values_lower_bound_update(key_prefix);
            let result = if let Some((lower_bound, find_entry)) = lower_bound {
                // Delete the keys (or key/values) in the entry
                let key_prefix_red = &key_prefix[lower_bound.len()..];
                find_entry.delete_prefix(key_prefix_red);
                let new_cache_size = find_entry.size() + key_prefix.len();
                Some((new_cache_size, lower_bound.clone()))
            } else {
                None
            };
	    if let Some((new_cache_size, lower_bound)) = result {
                // Update the size without changing the position.
                let cache_key = CacheKey::FindKeyValues(lower_bound.clone());
                self.update_cache_key_size(&cache_key, new_cache_size);
            } else {
                // There is no lower bound. Therefore we can insert
                // the deleted prefix in the cache.
                let size = key_prefix.len();
                let cache_key = CacheKey::FindKeyValues(key_prefix.to_vec());
                let find_entry = FindKeyValuesEntry(BTreeMap::new());
                self.find_key_values_map.insert(key_prefix.to_vec(), find_entry);
                self.insert_cache_key(cache_key, size);
            }
        } else {
            // Just forget about the entries.
            let mut keys = Vec::new();
            for (key, _) in self.value_map.range(get_interval(key_prefix.to_vec())) {
                keys.push(key.to_vec());
            }
            for key in keys {
                self.value_map.remove(&key);
                let cache_key = CacheKey::Value(key);
                let Some(size) = self.queue.remove(&cache_key) else {
                    unreachable!("The key should be in the queue");
                };
                self.total_size -= size;
                self.total_value_size -= size;
            }
            // No find keys for shared access
        }
    }

    /// Returns the cached value, or `Some(None)` if the entry does not exist in the
    /// database. If `None` is returned, the entry might exist in the database but is
    /// not in the cache.
    fn query_read_value(&mut self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        // First querying the value_map
        let result = match self.value_map.get(key) {
            None => None,
            Some(entry) => match entry {
                ValueCacheEntry::DoesNotExist => Some(None),
                ValueCacheEntry::Exists => None,
                ValueCacheEntry::Value(vec) => Some(Some(vec.clone())),
            },
        };
        if result.is_some() {
            let cache_key = CacheKey::Value(key.to_vec());
            self.put_cache_key_on_top(cache_key);
            return result;
        }
        if self.has_exclusive_access {
            // Now trying the find maps.
            let lower_bound = self.get_find_key_values_lower_bound(key);
            let (lower_bound, result) = if let Some((lower_bound, find_entry)) = lower_bound {
                let key_red = &key[lower_bound.len()..];
                (lower_bound, find_entry.get_read_value(key_red))
            } else {
                return None;
            };
            let cache_key = CacheKey::FindKeyValues(lower_bound.clone());
            self.put_cache_key_on_top(cache_key);
            return Some(result);
        }
        result
    }

    /// Returns `Some(true)` or `Some(false)` if we know that the entry does or does not
    /// exist in the database. Returns `None` if that information is not in the cache.
    fn query_contains_key(&mut self, key: &[u8]) -> Option<bool> {
        // First try on the value_map
        let result = self
            .value_map
            .get(key)
            .map(|entry| !matches!(entry, ValueCacheEntry::DoesNotExist));
        if result.is_some() {
            let cache_key = CacheKey::Value(key.to_vec());
            self.put_cache_key_on_top(cache_key);
            return result;
        }
        if self.has_exclusive_access {
            // Now trying the find keys maps.
            let lower_bound = self.get_find_keys_lower_bound(key);
            let result = if let Some((lower_bound, find_entry)) = lower_bound {
                let key_red = &key[lower_bound.len()..];
                Some((lower_bound, find_entry.get_contains_key(key_red)))
            } else {
                None
            };
            if let Some((lower_bound, result)) = result {
                let cache_key = CacheKey::FindKeys(lower_bound.clone());
                self.put_cache_key_on_top(cache_key);
                return Some(result);
            }
            // Now trying the find key-values maps.
            let lower_bound = self.get_find_key_values_lower_bound(key);
            let (lower_bound, result) = if let Some((lower_bound, find_entry)) = lower_bound {
                let key_red = &key[lower_bound.len()..];
                (lower_bound, find_entry.get_contains_key(key_red))
            } else {
                return None;
            };
            let cache_key = CacheKey::FindKeyValues(lower_bound.clone());
            self.put_cache_key_on_top(cache_key);
            return Some(result);
        }
        result
    }

    /// Gets the find_keys entry from the key prefix
    pub fn query_find_keys(&mut self, key_prefix: &[u8]) -> Option<Vec<Vec<u8>>> {
        // Trying first the find_keys_by_prefix cache
        let result = match self.get_find_keys_lower_bound(key_prefix) {
            None => {
                None
            },
            Some((lower_bound, cache_entry)) => {
                let key_prefix_red = &key_prefix[lower_bound.len()..];
                Some((lower_bound, cache_entry.get_find_keys(key_prefix_red)))
            }
        };
        if let Some((lower_bound, keys)) = result {
            let cache_key = CacheKey::FindKeys(lower_bound.clone());
            self.put_cache_key_on_top(cache_key);
            return Some(keys);
        }
        // Trying first the find_keys_by_prefix cache
        let (lower_bound, result) = match self.get_find_key_values_lower_bound(key_prefix) {
            None => {
                return None;
            },
            Some((lower_bound, cache_entry)) => {
                let key_prefix_red = &key_prefix[lower_bound.len()..];
                (lower_bound, cache_entry.get_find_keys(key_prefix_red))
            }
        };
        let cache_key = CacheKey::FindKeyValues(lower_bound.clone());
        self.put_cache_key_on_top(cache_key);
        Some(result)
    }

    /// Gets the find key values entry from the key prefix
    pub fn query_find_key_values(&mut self, key_prefix: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let (lower_bound, result) = match self.get_find_key_values_lower_bound(key_prefix) {
            None => {
                return None;
            },
            Some((lower_bound, cache_entry)) => {
                let key_prefix_red = &key_prefix[lower_bound.len()..];
                (
                    lower_bound,
                    cache_entry.get_find_key_values(key_prefix_red),
                )
            }
        };
        let cache_key = CacheKey::FindKeyValues(lower_bound.to_vec());
        self.put_cache_key_on_top(cache_key);
        Some(result)
    }
}

/// A key-value database with added LRU caching.
#[derive(Clone)]
pub struct LruCachingDatabase<D> {
    /// The inner store that is called by the LRU cache one
    database: D,
    /// The configuration.
    config: StorageCacheConfig,
}

/// A key-value store with added LRU caching.
#[derive(Clone)]
pub struct LruCachingStore<S> {
    /// The inner store that is called by the LRU cache one
    store: S,
    /// The LRU cache of values.
    cache: Option<Arc<Mutex<LruPrefixCache>>>,
}

impl<D> WithError for LruCachingDatabase<D>
where
    D: WithError,
{
    type Error = D::Error;
}

impl<S> WithError for LruCachingStore<S>
where
    S: WithError,
{
    type Error = S::Error;
}

impl<K> ReadableKeyValueStore for LruCachingStore<K>
where
    K: ReadableKeyValueStore,
{
    // The LRU cache does not change the underlying store's size limits.
    const MAX_KEY_SIZE: usize = K::MAX_KEY_SIZE;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.read_value_bytes(key).await;
        };
        // First inquiring in the read_value_bytes LRU
        {
            let mut cache = cache.lock().unwrap();
            if let Some(value) = cache.query_read_value(key) {
                #[cfg(with_metrics)]
                metrics::READ_VALUE_CACHE_HIT_COUNT
                    .with_label_values(&[])
                    .inc();
                return Ok(value);
            }
        }
        #[cfg(with_metrics)]
        metrics::READ_VALUE_CACHE_MISS_COUNT
            .with_label_values(&[])
            .inc();
        let value = self.store.read_value_bytes(key).await?;
        let mut cache = cache.lock().unwrap();
        cache.insert_read_value(key.to_vec(), &value);
        Ok(value)
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.contains_key(key).await;
        };
        {
            let mut cache = cache.lock().unwrap();
            if let Some(value) = cache.query_contains_key(key) {
                #[cfg(with_metrics)]
                metrics::CONTAINS_KEY_CACHE_HIT_COUNT
                    .with_label_values(&[])
                    .inc();
                return Ok(value);
            }
        }
        #[cfg(with_metrics)]
        metrics::CONTAINS_KEY_CACHE_MISS_COUNT
            .with_label_values(&[])
            .inc();
        let result = self.store.contains_key(key).await?;
        let mut cache = cache.lock().unwrap();
        cache.insert_contains_key(key.to_vec(), result);
        Ok(result)
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.contains_keys(keys).await;
        };
        let size = keys.len();
        let mut results = vec![false; size];
        let mut indices = Vec::new();
        let mut key_requests = Vec::new();
        {
            let mut cache = cache.lock().unwrap();
            for i in 0..size {
                if let Some(value) = cache.query_contains_key(&keys[i]) {
                    #[cfg(with_metrics)]
                    metrics::CONTAINS_KEY_CACHE_HIT_COUNT
                        .with_label_values(&[])
                        .inc();
                    results[i] = value;
                } else {
                    #[cfg(with_metrics)]
                    metrics::CONTAINS_KEY_CACHE_MISS_COUNT
                        .with_label_values(&[])
                        .inc();
                    indices.push(i);
                    key_requests.push(keys[i].clone());
                }
            }
        }
        if !key_requests.is_empty() {
            let key_results = self.store.contains_keys(key_requests.clone()).await?;
            let mut cache = cache.lock().unwrap();
            for ((index, result), key) in indices.into_iter().zip(key_results).zip(key_requests) {
                results[index] = result;
                cache.insert_contains_key(key, result);
            }
        }
        Ok(results)
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.read_multi_values_bytes(keys).await;
        };

        let mut result = Vec::with_capacity(keys.len());
        let mut cache_miss_indices = Vec::new();
        let mut miss_keys = Vec::new();
        {
            let mut cache = cache.lock().unwrap();
            for (i, key) in keys.into_iter().enumerate() {
                if let Some(value) = cache.query_read_value(&key) {
                    #[cfg(with_metrics)]
                    metrics::READ_VALUE_CACHE_HIT_COUNT
                        .with_label_values(&[])
                        .inc();
                    result.push(value);
                } else {
                    #[cfg(with_metrics)]
                    metrics::READ_VALUE_CACHE_MISS_COUNT
                        .with_label_values(&[])
                        .inc();
                    result.push(None);
                    cache_miss_indices.push(i);
                    miss_keys.push(key);
                }
            }
        }
        if !miss_keys.is_empty() {
            let values = self
                .store
                .read_multi_values_bytes(miss_keys.clone())
                .await?;
            let mut cache = cache.lock().unwrap();
            for (i, (key, value)) in cache_miss_indices
                .into_iter()
                .zip(miss_keys.into_iter().zip(values))
            {
                cache.insert_read_value(key, &value);
                result[i] = value;
            }
        }
        Ok(result)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.find_keys_by_prefix(key_prefix).await;
        };
        let has_exclusive_access = {
            let cache = cache.lock().unwrap();
            cache.has_exclusive_access
        };
        if !has_exclusive_access {
            return self.store.find_keys_by_prefix(key_prefix).await;
        }
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.find_key_values_by_prefix(key_prefix).await;
        };
        let has_exclusive_access = {
            let cache = cache.lock().unwrap();
            cache.has_exclusive_access
        };
        if !has_exclusive_access {
            return self.store.find_key_values_by_prefix(key_prefix).await;
        }



        
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

impl<K> WritableKeyValueStore for LruCachingStore<K>
where
    K: WritableKeyValueStore,
{
    // The LRU cache does not change the underlying store's size limits.
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.write_batch(batch).await;
        };

        {
            let mut cache = cache.lock().unwrap();
            for operation in &batch.operations {
                match operation {
                    WriteOperation::Put { key, value } => {
                        cache.correct_find_entry(key, Some(value.to_vec()));
                        let cache_entry = ValueCacheEntry::Value(value.to_vec());
                        cache.insert_value(key.to_vec(), cache_entry);
                    }
                    WriteOperation::Delete { key } => {
                        cache.correct_find_entry(key, None);
                        let cache_entry = ValueCacheEntry::DoesNotExist;
                        cache.insert_value(key.to_vec(), cache_entry);
                    }
                    WriteOperation::DeletePrefix { key_prefix } => {
                        cache.delete_prefix(key_prefix);
                    }
                }
            }
        }
        self.store.write_batch(batch).await
    }

    async fn clear_journal(&self) -> Result<(), Self::Error> {
        self.store.clear_journal().await
    }
}

/// The configuration type for the `LruCachingStore`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LruCachingConfig<C> {
    /// The inner configuration of the `LruCachingStore`.
    pub inner_config: C,
    /// The cache size being used
    pub storage_cache_config: StorageCacheConfig,
}

impl<D> KeyValueDatabase for LruCachingDatabase<D>
where
    D: KeyValueDatabase,
{
    type Config = LruCachingConfig<D::Config>;

    type Store = LruCachingStore<D::Store>;

    fn get_name() -> String {
        format!("lru caching {}", D::get_name())
    }

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, Self::Error> {
        let database = D::connect(&config.inner_config, namespace).await?;
        Ok(LruCachingDatabase {
            database,
            config: config.storage_cache_config.clone(),
        })
    }

    fn open_shared(&self, root_key: &[u8]) -> Result<Self::Store, Self::Error> {
        let store = self.database.open_shared(root_key)?;
        let store = LruCachingStore::new(
            store,
            self.config.clone(),
            /* has_exclusive_access */ false,
        );
        Ok(store)
    }

    fn open_exclusive(&self, root_key: &[u8]) -> Result<Self::Store, Self::Error> {
        let store = self.database.open_exclusive(root_key)?;
        let store = LruCachingStore::new(
            store,
            self.config.clone(),
            /* has_exclusive_access */ true,
        );
        Ok(store)
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, Self::Error> {
        D::list_all(&config.inner_config).await
    }

    async fn list_root_keys(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<Vec<Vec<u8>>, Self::Error> {
        D::list_root_keys(&config.inner_config, namespace).await
    }

    async fn delete_all(config: &Self::Config) -> Result<(), Self::Error> {
        D::delete_all(&config.inner_config).await
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, Self::Error> {
        D::exists(&config.inner_config, namespace).await
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        D::create(&config.inner_config, namespace).await
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        D::delete(&config.inner_config, namespace).await
    }
}

impl<S> LruCachingStore<S> {
    /// Creates a new key-value store that provides LRU caching at top of the given store.
    fn new(store: S, config: StorageCacheConfig, has_exclusive_access: bool) -> Self {
        let cache = {
            if config.max_cache_entries == 0 {
                None
            } else {
                Some(Arc::new(Mutex::new(LruPrefixCache::new(
                    config,
                    has_exclusive_access,
                ))))
            }
        };
        Self { store, cache }
    }
}

/// A memory darabase with caching.
#[cfg(with_testing)]
pub type LruCachingMemoryDatabase = LruCachingDatabase<MemoryDatabase>;

#[cfg(with_testing)]
impl<D> TestKeyValueDatabase for LruCachingDatabase<D>
where
    D: TestKeyValueDatabase,
{
    async fn new_test_config() -> Result<LruCachingConfig<D::Config>, D::Error> {
        let inner_config = D::new_test_config().await?;
        let storage_cache_config = DEFAULT_STORAGE_CACHE_CONFIG;
        Ok(LruCachingConfig {
            inner_config,
            storage_cache_config,
        })
    }
}
