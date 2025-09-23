// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A prefix based cache for the key/value store.

use std::collections::{btree_map::Entry, hash_map::RandomState, BTreeMap, BTreeSet};

use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};

use crate::common::get_interval;

/// The parametrization of the cache.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageCacheConfig {
    /// The maximum size of the cache, in bytes (keys size + value sizes).
    pub max_cache_size: usize,
    /// The maximum size of an entry size, in bytes.
    pub max_entry_size: usize,
    /// The maximum number of entries in the cache.
    pub max_cache_entries: usize,
    /// The maximum size of cached values.
    pub max_cache_value_size: usize,
    /// The maximum size of cached `find_keys_by_prefix` results.
    pub max_cache_find_keys_size: usize,
    /// The maximum size of cached `find_key_values_by_prefix` results.
    pub max_cache_find_key_values_size: usize,
}

#[derive(Eq, Hash, PartialEq, Debug)]
enum CacheKey {
    Value(Vec<u8>),
    FindKeys(Vec<u8>),
    FindKeyValues(Vec<u8>),
}

pub(crate) enum ValueCacheEntry {
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

    fn get_keys_by_prefix(&self, key_prefix: &[u8]) -> Vec<Vec<u8>> {
        let key_prefix = key_prefix.to_vec();
        let delta = key_prefix.len();
        self.0
            .range(get_interval(key_prefix))
            .map(|key| key[delta..].to_vec())
            .collect()
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        self.0.contains(key)
    }

    fn update_entry(&mut self, key: &[u8], new_value: Option<Vec<u8>>) {
        match new_value {
            None => {
                self.0.remove(key);
            }
            Some(_) => {
                self.0.insert(key.to_vec());
            }
        }
    }

    fn delete_prefix(&mut self, key_prefix: &[u8]) {
        let keys = self
            .0
            .range(get_interval(key_prefix.to_vec()))
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            self.0.remove(&key);
        }
    }
}

struct FindKeyValuesEntry(BTreeMap<Vec<u8>, Vec<u8>>);

impl FindKeyValuesEntry {
    fn size(&self) -> usize {
        self.0
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum()
    }

    fn get_keys_by_prefix(&self, key_prefix: &[u8]) -> Vec<Vec<u8>> {
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

    fn contains_key(&self, key: &[u8]) -> bool {
        self.0.contains_key(key)
    }

    fn get_read_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.get(key).cloned()
    }

    fn update_entry(&mut self, key: &[u8], new_value: Option<Vec<u8>>) {
        match new_value {
            None => {
                self.0.remove(key);
            }
            Some(new_value) => {
                self.0.insert(key.to_vec(), new_value);
            }
        }
    }

    fn delete_prefix(&mut self, key_prefix: &[u8]) {
        let keys = self
            .0
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
pub(crate) struct LruPrefixCache {
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
    pub has_exclusive_access: bool,
}

impl LruPrefixCache {
    /// Creates an `LruPrefixCache`.
    pub fn new(config: StorageCacheConfig, has_exclusive_access: bool) -> Self {
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

    /// A used key needs to be put on top.
    fn put_cache_key_on_top(&mut self, cache_key: CacheKey) {
        let size = self
            .queue
            .remove(&cache_key)
            .expect("cache_key should be present");
        self.queue.insert(cache_key, size);
    }

    /// Decrease the sizes of the keys.
    fn decrease_sizes(&mut self, cache_key: &CacheKey, size: usize) {
        self.total_size -= size;
        match cache_key {
            CacheKey::Value(_) => {
                self.total_value_size -= size;
            }
            CacheKey::FindKeys(_) => {
                self.total_find_keys_size -= size;
            }
            CacheKey::FindKeyValues(_) => {
                self.total_find_key_values_size -= size;
            }
        }
    }

    /// Increase the sizes of the keys.
    fn increase_sizes(&mut self, cache_key: &CacheKey, size: usize) {
        self.total_size += size;
        match cache_key {
            CacheKey::Value(_) => {
                self.total_value_size += size;
            }
            CacheKey::FindKeys(_) => {
                self.total_find_keys_size += size;
            }
            CacheKey::FindKeyValues(_) => {
                self.total_find_key_values_size += size;
            }
        }
    }

    /// Remove an entry from the queue and update the sizes.
    fn remove_cache_key(&mut self, cache_key: &CacheKey) {
        let size = self
            .queue
            .remove(cache_key)
            .expect("cache_key should be present");
        self.decrease_sizes(cache_key, size);
    }

    /// Update the cache size to the new size without changing position.
    fn update_cache_key_size(&mut self, cache_key: &CacheKey, new_size: usize) {
        let size = self
            .queue
            .get_mut(cache_key)
            .expect("cache_key should be present");
        let old_size = *size;
        *size = new_size;
        self.decrease_sizes(cache_key, old_size);
        self.increase_sizes(cache_key, new_size);
    }

    /// Insert a cache_key into the queue and update sizes.
    fn insert_cache_key(&mut self, cache_key: CacheKey, size: usize) {
        self.increase_sizes(&cache_key, size);
        self.queue.insert(cache_key, size);
    }

    fn get_keys_by_prefix_lower_bound(&self, key: &[u8]) -> Option<(&Vec<u8>, &FindKeysEntry)> {
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

    fn get_keys_by_prefix_lower_bound_mut(
        &mut self,
        key: &[u8],
    ) -> Option<(&Vec<u8>, &mut FindKeysEntry)> {
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

    fn get_find_key_values_lower_bound(
        &self,
        key: &[u8],
    ) -> Option<(&Vec<u8>, &FindKeyValuesEntry)> {
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

    fn get_find_key_values_lower_bound_mut(
        &mut self,
        key: &[u8],
    ) -> Option<(&Vec<u8>, &mut FindKeyValuesEntry)> {
        match self
            .find_key_values_map
            .range_mut(..=key.to_vec())
            .next_back()
        {
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

    /// Trim value cache so that it fits within bounds.
    fn trim_value_cache(&mut self) {
        let mut keys = Vec::new();
        let mut total_size = self.total_value_size;
        let mut iter = self.queue.iter();
        loop {
            let value = iter.next();
            let Some((cache_key, size)) = value else {
                break;
            };
            if total_size < self.config.max_cache_value_size {
                break;
            }
            if let CacheKey::Value(key) = cache_key {
                total_size -= size;
                keys.push(key.to_vec());
            }
        }
        for key in keys {
            self.value_map.remove(&key);
            let cache_key = CacheKey::Value(key);
            self.remove_cache_key(&cache_key);
        }
    }

    /// Trim `find_keys_by_prefix` cache so that it fits within bounds.
    fn trim_find_keys_cache(&mut self) {
        let mut prefixes = Vec::new();
        let mut total_size = self.total_find_keys_size;
        let mut iter = self.queue.iter();
        loop {
            let value = iter.next();
            let Some((cache_key, size)) = value else {
                break;
            };
            if total_size < self.config.max_cache_find_keys_size {
                break;
            }
            if let CacheKey::FindKeys(prefix) = cache_key {
                total_size -= size;
                prefixes.push(prefix.to_vec());
            }
        }
        for prefix in prefixes {
            self.find_keys_map.remove(&prefix);
            let cache_key = CacheKey::FindKeys(prefix);
            self.remove_cache_key(&cache_key);
        }
    }

    /// Trim `find_key_values_by_prefix` cache so that it fits within bounds.
    fn trim_find_key_values_cache(&mut self) {
        let mut prefixes = Vec::new();
        let mut total_size = self.total_find_key_values_size;
        let mut iter = self.queue.iter();
        loop {
            let value = iter.next();
            let Some((cache_key, size)) = value else {
                break;
            };
            if total_size < self.config.max_cache_find_key_values_size {
                break;
            }
            if let CacheKey::FindKeyValues(prefix) = cache_key {
                total_size -= size;
                prefixes.push(prefix.to_vec());
            }
        }
        for prefix in prefixes {
            self.find_key_values_map.remove(&prefix);
            let cache_key = CacheKey::FindKeyValues(prefix);
            self.remove_cache_key(&cache_key);
        }
    }

    /// Trim the cache so that it fits within the constraints.
    fn trim_cache(&mut self) {
        while self.total_size > self.config.max_cache_size
            || self.queue.len() > self.config.max_cache_entries
        {
            let Some((cache_key, size)) = self.queue.pop_front() else {
                break;
            };
            self.decrease_sizes(&cache_key, size);
        }
    }

    /// Inserts an entry into the cache.
    fn insert_value(&mut self, key: &[u8], cache_entry: ValueCacheEntry) {
        let size = key.len() + cache_entry.size();
        if (matches!(cache_entry, ValueCacheEntry::DoesNotExist) && !self.has_exclusive_access)
            || size > self.config.max_entry_size
        {
            if self.value_map.remove(key).is_some() {
                let cache_key = CacheKey::Value(key.to_vec());
                self.remove_cache_key(&cache_key);
            };
            return;
        }
        match self.value_map.entry(key.to_vec()) {
            Entry::Occupied(mut entry) => {
                entry.insert(cache_entry);
                // Put it on first position for LRU with the new size
                let cache_key = CacheKey::Value(key.to_vec());
                self.remove_cache_key(&cache_key);
                self.insert_cache_key(cache_key, size);
            }
            Entry::Vacant(entry) => {
                entry.insert(cache_entry);
                let cache_key = CacheKey::Value(key.to_vec());
                self.insert_cache_key(cache_key, size);
            }
        }
        self.trim_value_cache();
        self.trim_cache();
    }

    /// Puts a key/value in the cache.
    pub fn put_key_value(&mut self, key: &[u8], value: &[u8]) {
        self.correct_find_entry(key, Some(value.to_vec()));
        let cache_entry = ValueCacheEntry::Value(value.to_vec());
        self.insert_value(key, cache_entry);
    }

    /// Deletes a key from the cache.
    pub fn delete_key(&mut self, key: &[u8]) {
        self.correct_find_entry(key, None);
        let cache_entry = ValueCacheEntry::DoesNotExist;
        self.insert_value(key, cache_entry);
    }

    /// Updates the find entries.
    fn correct_find_entry(&mut self, key: &[u8], new_value: Option<Vec<u8>>) {
        let lower_bound = self.get_keys_by_prefix_lower_bound_mut(key);
        if let Some((lower_bound, cache_entry)) = lower_bound {
            let key_red = &key[lower_bound.len()..];
            cache_entry.update_entry(key_red, new_value.clone());
        }
        let lower_bound = self.get_find_key_values_lower_bound_mut(key);
        if let Some((lower_bound, cache_entry)) = lower_bound {
            let key_red = &key[lower_bound.len()..];
            cache_entry.update_entry(key_red, new_value);
        }
    }

    /// Inserts a read_value entry into the cache.
    pub fn insert_read_value(&mut self, key: &[u8], value: &Option<Vec<u8>>) {
        let cache_entry = match value {
            None => ValueCacheEntry::DoesNotExist,
            Some(vec) => ValueCacheEntry::Value(vec.to_vec()),
        };
        self.insert_value(key, cache_entry)
    }

    /// Inserts a read_value entry into the cache.
    pub fn insert_contains_key(&mut self, key: &[u8], result: bool) {
        let cache_entry = match result {
            false => ValueCacheEntry::DoesNotExist,
            true => ValueCacheEntry::Exists,
        };
        self.insert_value(key, cache_entry)
    }

    pub fn insert_find_keys(&mut self, key_prefix: Vec<u8>, keys: &[Vec<u8>]) {
        let find_entry = FindKeysEntry(keys.iter().map(|x| x.to_vec()).collect());
        let size = find_entry.size() + key_prefix.len();
        if size > self.config.max_entry_size {
            // The entry is too large, we do not insert it,
            return;
        }
        // Clearing up the find entries
        let keys = self
            .find_keys_map
            .range(get_interval(key_prefix.clone()))
            .map(|(x, _)| x.clone())
            .collect::<Vec<_>>();
        for key in keys {
            self.find_keys_map.remove(&key);
            let cache_key = CacheKey::FindKeys(key);
            self.remove_cache_key(&cache_key);
        }
        let cache_key = CacheKey::FindKeys(key_prefix.clone());
        // The entry has to be missing otherwise, it would have been found
        assert!(self
            .find_keys_map
            .insert(key_prefix.clone(), find_entry)
            .is_none());
        self.insert_cache_key(cache_key, size);
        self.trim_find_keys_cache();
        self.trim_cache();
    }

    pub fn insert_find_key_values(
        &mut self,
        key_prefix: Vec<u8>,
        key_values: &[(Vec<u8>, Vec<u8>)],
    ) {
        let find_entry = FindKeyValuesEntry(
            key_values
                .iter()
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect(),
        );
        let size = find_entry.size() + key_prefix.len();
        if size > self.config.max_entry_size {
            // The entry is too large, we do not insert it,
            return;
        }
        // Clearing up the FindKeys entries
        let prefixes = self
            .find_keys_map
            .range(get_interval(key_prefix.clone()))
            .map(|(x, _)| x.clone())
            .collect::<Vec<_>>();
        for prefix in prefixes {
            self.find_keys_map.remove(&prefix);
            let cache_key = CacheKey::FindKeys(prefix);
            self.remove_cache_key(&cache_key);
        }
        // Clearing up the FindKeys entries
        let prefixes = self
            .find_key_values_map
            .range(get_interval(key_prefix.clone()))
            .map(|(x, _)| x.clone())
            .collect::<Vec<_>>();
        for prefix in prefixes {
            self.find_key_values_map.remove(&prefix);
            let cache_key = CacheKey::FindKeyValues(prefix);
            self.remove_cache_key(&cache_key);
        }
        // Clearing up the value entries if it is a KeyValues as
        // it is obsoletes
        let keys = self
            .value_map
            .range(get_interval(key_prefix.clone()))
            .map(|(x, _)| x.clone())
            .collect::<Vec<_>>();
        for key in keys {
            self.value_map.remove(&key);
            let cache_key = CacheKey::Value(key);
            self.remove_cache_key(&cache_key);
        }
        let cache_key = CacheKey::FindKeyValues(key_prefix.clone());
        // The entry has to be missing otherwise, it would have been found
        assert!(self
            .find_key_values_map
            .insert(key_prefix.clone(), find_entry)
            .is_none());
        self.insert_cache_key(cache_key, size);
        self.trim_find_key_values_cache();
        self.trim_cache();
    }

    /// Marks cached keys that match the prefix as deleted. Importantly, this does not
    /// create new entries in the cache.
    pub fn delete_prefix(&mut self, key_prefix: &[u8]) {
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
            for (prefix, _) in self
                .find_key_values_map
                .range(get_interval(key_prefix.to_vec()))
            {
                prefixes.push(prefix.to_vec());
            }
            for prefix in prefixes {
                self.find_key_values_map.remove(&prefix);
                let cache_key = CacheKey::FindKeyValues(prefix);
                self.remove_cache_key(&cache_key);
            }
            // Finding a lower bound. If existing update, if not insert.
            let lower_bound = self.get_keys_by_prefix_lower_bound_mut(key_prefix);
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
            let lower_bound = self.get_find_key_values_lower_bound_mut(key_prefix);
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
                self.find_key_values_map
                    .insert(key_prefix.to_vec(), find_entry);
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
    pub fn query_read_value(&mut self, key: &[u8]) -> Option<Option<Vec<u8>>> {
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
    pub fn query_contains_key(&mut self, key: &[u8]) -> Option<bool> {
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
            let lower_bound = self.get_keys_by_prefix_lower_bound(key);
            let result = if let Some((lower_bound, find_entry)) = lower_bound {
                let key_red = &key[lower_bound.len()..];
                Some((lower_bound, find_entry.contains_key(key_red)))
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
                (lower_bound, find_entry.contains_key(key_red))
            } else {
                return None;
            };
            let cache_key = CacheKey::FindKeyValues(lower_bound.clone());
            self.put_cache_key_on_top(cache_key);
            return Some(result);
        }
        result
    }

    /// Gets the find_keys entry from the key prefix.
    pub fn query_find_keys(&mut self, key_prefix: &[u8]) -> Option<Vec<Vec<u8>>> {
        // Trying first the find_keys_by_prefix cache
        let result = match self.get_keys_by_prefix_lower_bound(key_prefix) {
            None => None,
            Some((lower_bound, cache_entry)) => {
                let key_prefix_red = &key_prefix[lower_bound.len()..];
                Some((lower_bound, cache_entry.get_keys_by_prefix(key_prefix_red)))
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
            }
            Some((lower_bound, cache_entry)) => {
                let key_prefix_red = &key_prefix[lower_bound.len()..];
                (lower_bound, cache_entry.get_keys_by_prefix(key_prefix_red))
            }
        };
        let cache_key = CacheKey::FindKeyValues(lower_bound.clone());
        self.put_cache_key_on_top(cache_key);
        Some(result)
    }

    /// Gets the find key values entry from the key prefix.
    pub fn query_find_key_values(&mut self, key_prefix: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let (lower_bound, result) = match self.get_find_key_values_lower_bound(key_prefix) {
            None => {
                return None;
            }
            Some((lower_bound, cache_entry)) => {
                let key_prefix_red = &key_prefix[lower_bound.len()..];
                (lower_bound, cache_entry.get_find_key_values(key_prefix_red))
            }
        };
        let cache_key = CacheKey::FindKeyValues(lower_bound.to_vec());
        self.put_cache_key_on_top(cache_key);
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn create_test_cache(has_exclusive_access: bool) -> LruPrefixCache {
        let config = StorageCacheConfig {
            max_cache_size: 1000,
            max_entry_size: 100,
            max_cache_entries: 10,
            max_cache_value_size: 500,
            max_cache_find_keys_size: 500,
            max_cache_find_key_values_size: 500,
        };
        LruPrefixCache::new(config, has_exclusive_access)
    }

    #[test]
    fn test_new_cache_creation() {
        let cache = create_test_cache(true);
        assert_eq!(cache.total_size, 0);
        assert_eq!(cache.total_value_size, 0);
        assert_eq!(cache.total_find_keys_size, 0);
        assert_eq!(cache.total_find_key_values_size, 0);
        assert!(cache.has_exclusive_access);
        assert!(cache.value_map.is_empty());
        assert!(cache.find_keys_map.is_empty());
        assert!(cache.find_key_values_map.is_empty());
        assert!(cache.queue.is_empty());
    }

    #[test]
    fn test_insert_and_query_read_value() {
        let mut cache = create_test_cache(true);
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];

        // Insert a value
        cache.insert_read_value(&key, &Some(value.clone()));

        // Query the value
        let result = cache.query_read_value(&key);
        assert_eq!(result, Some(Some(value)));

        // Query non-existent key
        let result = cache.query_read_value(&[9, 9, 9]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_insert_and_query_contains_key() {
        let mut cache = create_test_cache(true);
        let key = vec![1, 2, 3];

        // Insert a key that exists
        cache.insert_contains_key(&key, true);

        // Query the key
        let result = cache.query_contains_key(&key);
        assert_eq!(result, Some(true));

        // Insert a key that doesn't exist
        let key2 = vec![4, 5, 6];
        cache.insert_contains_key(&key2, false);

        let result = cache.query_contains_key(&key2);
        assert_eq!(result, Some(false));
    }

    #[test]
    fn test_insert_and_query_find_keys() {
        let mut cache = create_test_cache(true);
        let prefix = vec![1, 2];
        let keys = vec![vec![3], vec![4], vec![5]];

        // Insert find_keys entry
        cache.insert_find_keys(prefix.clone(), &keys);

        // Query with exact prefix
        let result = cache.query_find_keys(&prefix);
        assert_eq!(result, Some(keys.clone()));

        // Query with longer prefix that should find the suffix
        let longer_prefix = vec![1, 2, 3];
        let result = cache.query_find_keys(&longer_prefix);
        assert_eq!(result, Some(vec![vec![]])); // Should return empty vec since [1,2,3] matches exactly

        // Query with non-matching prefix
        let result = cache.query_find_keys(&[9, 9]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_insert_and_query_find_key_values() {
        let mut cache = create_test_cache(true);
        let prefix = vec![1, 2];
        let key_values = vec![(vec![3], vec![10]), (vec![4], vec![20])];

        // Insert find_key_values entry
        cache.insert_find_key_values(prefix.clone(), &key_values);

        // Query with exact prefix
        let result = cache.query_find_key_values(&prefix);
        assert_eq!(result, Some(key_values.clone()));

        // Query with non-matching prefix
        let result = cache.query_find_key_values(&[9, 9]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_lru_eviction_by_cache_size() {
        let mut cache = create_test_cache(true);

        // Fill cache beyond max_cache_size
        for i in 0..20 {
            let key = vec![i];
            let value = vec![0; 100]; // Large value to trigger size-based eviction
            cache.insert_read_value(&key, &Some(value));
        }

        // Should have evicted some entries to stay within limits
        assert!(cache.total_size <= cache.config.max_cache_size);
        assert!(cache.queue.len() <= cache.config.max_cache_entries);
    }

    #[test]
    fn test_lru_eviction_by_entry_count() {
        let mut cache = create_test_cache(true);

        // Fill cache beyond max_cache_entries
        for i in 0..15 {
            let key = vec![i];
            let value = vec![i]; // Small values
            cache.insert_read_value(&key, &Some(value));
        }

        // Should have evicted entries to stay within max_cache_entries
        assert!(cache.queue.len() <= cache.config.max_cache_entries);
    }

    #[test]
    fn test_cache_entry_promotion() {
        let mut cache = create_test_cache(true);
        let key1 = vec![1];
        let key2 = vec![2];
        let key3 = vec![3];
        let value = vec![42];

        // Insert two entries
        cache.insert_read_value(&key1, &Some(value.clone()));
        cache.insert_read_value(&key2, &Some(value.clone()));

        // Access key1 to promote it
        assert_eq!(Some(Some(value)), cache.query_read_value(&key1));

        // Access key3 to promote it
        assert_eq!(None, cache.query_read_value(&key3));

        // The queue should have key1 at the end (most recently used)
        let queue_keys = cache.queue.keys().collect::<Vec<_>>();
        assert_eq!(queue_keys[queue_keys.len() - 1], &CacheKey::Value(key1));
    }

    #[test]
    fn test_correct_find_entry_mut() {
        let mut cache = create_test_cache(true);
        let prefix = vec![1];
        let key = vec![1, 2];
        let original_keys = vec![vec![2], vec![3]];

        // Insert find_keys entry
        cache.insert_find_keys(prefix.clone(), &original_keys);

        // Update an entry
        cache.correct_find_entry(&key, Some(vec![42]));

        // The find_keys entry should now contain the updated key
        let result = cache.query_find_keys(&prefix);
        assert!(result.is_some());
        let keys = result.unwrap();
        assert!(keys.contains(&vec![2]));
    }

    #[test]
    fn test_delete_prefix_with_exclusive_access() {
        let mut cache = create_test_cache(true);
        let prefix = vec![1];
        let key1 = vec![1, 2];
        let key2 = vec![1, 3];
        let key3 = vec![2, 4]; // Should not be affected
        let value = vec![42];

        // Insert some values
        cache.insert_read_value(&key1, &Some(value.clone()));
        cache.insert_read_value(&key2, &Some(value.clone()));
        cache.insert_read_value(&key3, &Some(value.clone()));

        // Delete prefix [1]
        cache.delete_prefix(&prefix);

        // Keys with prefix [1] should be marked as DoesNotExist
        let result1 = cache.query_read_value(&key1);
        assert_eq!(result1, Some(None));

        let result2 = cache.query_read_value(&key2);
        assert_eq!(result2, Some(None));

        // Key with different prefix should be unaffected
        let result3 = cache.query_read_value(&key3);
        assert_eq!(result3, Some(Some(value)));
    }

    #[test]
    fn test_delete_prefix_without_exclusive_access() {
        let mut cache = create_test_cache(false);
        let prefix = vec![1];
        let key1 = vec![1, 2];
        let key2 = vec![2, 3]; // Different prefix
        let value = vec![42];

        // Insert some values
        cache.insert_read_value(&key1, &Some(value.clone()));
        cache.insert_read_value(&key2, &Some(value.clone()));

        // Delete prefix [1]
        cache.delete_prefix(&prefix);

        // Key with prefix [1] should be removed from cache
        let result1 = cache.query_read_value(&key1);
        assert_eq!(result1, None);

        // Key with different prefix should be unaffected
        let result2 = cache.query_read_value(&key2);
        assert_eq!(result2, Some(Some(value)));
    }

    #[test]
    fn test_entry_size_limits() {
        let mut cache = create_test_cache(true);
        let key = vec![1];
        let large_value = vec![0; cache.config.max_entry_size + 1];

        // Insert value larger than max_entry_size
        cache.insert_read_value(&key, &Some(large_value));

        // Should not be cached
        let result = cache.query_read_value(&key);
        assert_eq!(result, None);
    }

    #[test]
    fn test_does_not_exist_entry_without_exclusive_access() {
        let mut cache = create_test_cache(false);
        let key = vec![1];

        // Insert DoesNotExist entry without exclusive access
        cache.insert_read_value(&key, &None);

        // Should not be cached due to lack of exclusive access
        let result = cache.query_read_value(&key);
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_keys_entry_operations() {
        let mut find_entry = FindKeysEntry(BTreeSet::new());
        let key1 = vec![1, 2];
        let key2 = vec![1, 3];
        let key3 = vec![1, 3, 4];

        // Add keys
        find_entry.update_entry(&key1, Some(vec![42]));
        find_entry.update_entry(&key2, Some(vec![43]));

        // Test contains_key
        assert!(find_entry.contains_key(&key1));
        assert!(find_entry.contains_key(&key2));
        assert!(!find_entry.contains_key(&key3));
        assert!(!find_entry.contains_key(&[9, 9]));

        // Test get_keys_by_prefix
        let keys = find_entry.get_keys_by_prefix(&[1]);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&vec![2]));
        assert!(keys.contains(&vec![3]));

        // Remove a key
        find_entry.update_entry(&key1, None);
        assert!(!find_entry.contains_key(&key1));
        assert!(find_entry.contains_key(&key2));
    }

    #[test]
    fn test_find_key_values_entry_operations() {
        let mut find_entry = FindKeyValuesEntry(BTreeMap::new());
        let key1 = vec![1, 2];
        let key2 = vec![1, 3];
        let value1 = vec![42];
        let value2 = vec![43];

        // Add key-value pairs
        find_entry.update_entry(&key1, Some(value1.clone()));
        find_entry.update_entry(&key2, Some(value2.clone()));

        // Test contains_key
        assert!(find_entry.contains_key(&key1));
        assert!(find_entry.contains_key(&key2));

        // Test get_read_value
        assert_eq!(find_entry.get_read_value(&key1), Some(value1.clone()));
        assert_eq!(find_entry.get_read_value(&key2), Some(value2.clone()));

        // Test get_find_key_values with prefix
        let key_values = find_entry.get_find_key_values(&[1]);
        assert_eq!(key_values.len(), 2);
        assert!(key_values.contains(&(vec![2], value1.clone())));
        assert!(key_values.contains(&(vec![3], value2.clone())));

        // Remove a key
        find_entry.update_entry(&key1, None);
        assert!(!find_entry.contains_key(&key1));
        assert_eq!(find_entry.get_read_value(&key1), None);
    }

    #[test]
    fn test_cache_size_tracking() {
        let mut cache = create_test_cache(true);
        let initial_size = cache.total_size;
        let initial_value_size = cache.total_value_size;

        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        cache.insert_read_value(&key, &Some(value.clone()));

        // Size should increase
        assert!(cache.total_size > initial_size);
        assert!(cache.total_value_size > initial_value_size);

        let value_size_with_entry = cache.total_value_size;

        // Insert DoesNotExist entry (None value)
        cache.insert_read_value(&key, &None);

        // Value size should be less than when we had a real value,
        // since DoesNotExist entries have size 0 for the value part
        assert!(cache.total_value_size < value_size_with_entry);
    }

    #[test]
    fn test_trim_value_cache() {
        let mut cache = LruPrefixCache::new(
            StorageCacheConfig {
                max_cache_size: 10000,
                max_entry_size: 1000,
                max_cache_entries: 100,
                max_cache_value_size: 50, // Small limit to trigger trimming
                max_cache_find_keys_size: 1000,
                max_cache_find_key_values_size: 1000,
            },
            true,
        );

        // Insert multiple values to exceed max_cache_value_size
        for i in 0..10 {
            let key = vec![i];
            let value = vec![0; 20]; // Each entry ~20 bytes
            cache.insert_read_value(&key, &Some(value));
        }

        // Should have trimmed to stay within value cache limit
        assert!(cache.total_value_size <= cache.config.max_cache_value_size);
    }

    #[test]
    fn test_lower_bound_queries() {
        let mut cache = create_test_cache(true);
        let prefix1 = vec![1];
        let prefix2 = vec![1, 2];
        let keys1 = vec![vec![2], vec![3]];
        let keys2 = vec![vec![4]];

        // Insert entries with different prefixes
        cache.insert_find_keys(prefix1.clone(), &keys1);
        cache.insert_find_keys(prefix2.clone(), &keys2);

        // Query with exact prefix1 - should return the keys we inserted
        let result = cache.query_find_keys(&prefix1);
        assert_eq!(result, Some(keys1));

        // Query with exact prefix2 - should return the keys we inserted
        let result = cache.query_find_keys(&prefix2);
        assert_eq!(result, Some(keys2));

        // Query with prefix that doesn't match any cached entry
        let query_key = vec![9];
        let result = cache.query_find_keys(&query_key);
        assert_eq!(result, None);
    }
}
