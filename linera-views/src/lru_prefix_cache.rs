// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An LRU cache that supports prefix-search APIs.

use std::collections::{btree_map::Entry, hash_map::RandomState, BTreeMap, BTreeSet};

use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};

use crate::common::get_key_range_for_prefix;

/// The parametrization of the cache.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageCacheConfig {
    /// The maximum size of the cache, in bytes (keys size + value sizes).
    pub max_cache_size: usize,
    /// The maximum size of a value entry size, in bytes.
    pub max_value_entry_size: usize,
    /// The maximum size of a find-keys entry size, in bytes.
    pub max_find_keys_entry_size: usize,
    /// The maximum size of a find-key-values entry size, in bytes.
    pub max_find_key_values_entry_size: usize,
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

enum ValueEntry {
    DoesNotExist,
    Exists,
    Value(Vec<u8>),
}

impl ValueEntry {
    fn size(&self) -> usize {
        match self {
            ValueEntry::Value(vec) => vec.len(),
            _ => 0,
        }
    }
}

struct FindKeysEntry(BTreeSet<Vec<u8>>);

impl FindKeysEntry {
    fn size(&self) -> usize {
        self.0.iter().map(Vec::len).sum()
    }

    fn get_keys_by_prefix(&self, key_prefix: Vec<u8>) -> Vec<Vec<u8>> {
        let prefix_len = key_prefix.len();
        self.0
            .range(get_key_range_for_prefix(key_prefix))
            .map(|key| key[prefix_len..].to_vec())
            .collect()
    }

    fn contains_key(&self, key: &[u8]) -> bool {
        self.0.contains(key)
    }

    fn update_entry(&mut self, key: &[u8], is_some: bool) {
        if is_some {
            self.0.insert(key.to_vec());
        } else {
            self.0.remove(key);
        }
    }

    fn delete_prefix(&mut self, key_prefix: &[u8]) {
        let keys = self
            .0
            .range(get_key_range_for_prefix(key_prefix.to_vec()))
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

    fn get_keys_by_prefix(&self, key_prefix: Vec<u8>) -> Vec<Vec<u8>> {
        let prefix_len = key_prefix.len();
        self.0
            .range(get_key_range_for_prefix(key_prefix))
            .map(|(key, _)| key[prefix_len..].to_vec())
            .collect()
    }

    fn get_find_key_values(&self, key_prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let key_prefix = key_prefix.to_vec();
        let prefix_len = key_prefix.len();
        self.0
            .range(get_key_range_for_prefix(key_prefix))
            .map(|(key, value)| (key[prefix_len..].to_vec(), value.to_vec()))
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
            .range(get_key_range_for_prefix(key_prefix.to_vec()))
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
    value_map: BTreeMap<Vec<u8>, ValueEntry>,
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
    pub(crate) fn new(config: StorageCacheConfig, has_exclusive_access: bool) -> Self {
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

    /// Gets the `has_exclusive_access`.
    pub(crate) fn has_exclusive_access(&self) -> bool {
        self.has_exclusive_access
    }

    /// A used key needs to be put on top.
    fn move_cache_key_on_top(&mut self, cache_key: CacheKey) {
        let size = self
            .queue
            .remove(&cache_key)
            .expect("cache_key should be present");
        self.queue.insert(cache_key, size);
    }

    /// Update sizes by decreasing and increasing.
    fn update_sizes(&mut self, cache_key: &CacheKey, old_size: usize, new_size: usize) {
        use std::cmp::Ordering;
        match new_size.cmp(&old_size) {
            Ordering::Greater => {
                let increase_size = new_size - old_size;
                self.total_size += increase_size;
                match cache_key {
                    CacheKey::Value(_) => {
                        self.total_value_size += increase_size;
                    }
                    CacheKey::FindKeys(_) => {
                        self.total_find_keys_size += increase_size;
                    }
                    CacheKey::FindKeyValues(_) => {
                        self.total_find_key_values_size += increase_size;
                    }
                }
            }
            Ordering::Less => {
                let decrease_size = old_size - new_size;
                self.total_size -= decrease_size;
                match cache_key {
                    CacheKey::Value(_) => {
                        self.total_value_size -= decrease_size;
                    }
                    CacheKey::FindKeys(_) => {
                        self.total_find_keys_size -= decrease_size;
                    }
                    CacheKey::FindKeyValues(_) => {
                        self.total_find_key_values_size -= decrease_size;
                    }
                }
            }
            Ordering::Equal => {
                // Nothing to be done
            }
        }
    }

    /// Increase the sizes of the keys.
    fn increase_sizes(&mut self, cache_key: &CacheKey, size: usize) {
        self.update_sizes(cache_key, 0, size);
    }

    /// Decrease the sizes of the keys.
    fn decrease_sizes(&mut self, cache_key: &CacheKey, size: usize) {
        self.update_sizes(cache_key, size, 0);
    }

    /// Removes the `CacheKey` from the maps.
    fn remove_cache_key_from_map(&mut self, cache_key: &CacheKey) {
        match cache_key {
            CacheKey::Value(key) => {
                assert!(self.value_map.remove(key).is_some());
            }
            CacheKey::FindKeys(key) => {
                assert!(self.find_keys_map.remove(key).is_some());
            }
            CacheKey::FindKeyValues(key) => {
                assert!(self.find_key_values_map.remove(key).is_some());
            }
        }
    }

    /// Removes an entry from the queue and updates the sizes.
    fn remove_cache_key(&mut self, cache_key: &CacheKey) {
        let size = self
            .queue
            .remove(cache_key)
            .expect("cache_key should be present");
        self.decrease_sizes(cache_key, size);
    }

    /// Removes an entry from the queue if it exists.
    fn remove_cache_key_if_exists(&mut self, cache_key: &CacheKey) {
        let size = self.queue.remove(cache_key);
        if let Some(size) = size {
            self.decrease_sizes(cache_key, size);
            self.remove_cache_key_from_map(cache_key);
        }
    }

    /// Update the cache size to the new size without changing position.
    fn update_cache_key_sizes(&mut self, cache_key: &CacheKey, new_size: usize) {
        let size = self
            .queue
            .get_mut(cache_key)
            .expect("cache_key should be present");
        let old_size = *size;
        *size = new_size;
        self.update_sizes(cache_key, old_size, new_size);
    }

    /// Inserts a cache key into the queue and updates sizes.
    fn insert_cache_key(&mut self, cache_key: CacheKey, size: usize) {
        self.increase_sizes(&cache_key, size);
        assert!(self.queue.insert(cache_key, size).is_none());
    }

    /// If the FindKeys map contains a prefix that is a prefix of key in argument,
    /// then returns it and the corresponding FindKeys. Otherwise `None`.
    ///
    /// The algorithm of this function is tested in `test_lower_bounds`.
    /// However, due to its importance we provide here a proof of correctness.
    /// What makes this functionality work is that the set of keys of the `find_keys_map`
    /// is prefix free.
    ///
    /// Claim: `self.get_existing_find_keys_entry(key)` finds a prefix of key in `self.find_keys_map.keys()` iff such prefix exists.
    ///
    /// Note that when it exists, such a prefix is unique because `self.find_keys_map.keys()` is prefix-free.
    ///
    /// Proof: For the map `find_keys_map` in question, let us define `set` to be the `BTreeSet` of
    /// the keys of the map.
    /// Then define `S = { s in set | s <= key }` for the lexicographic ordering.
    /// First of all the expression `self.find_keys_map.range(..=key.to_vec())` corresponds
    /// to S and `self.find_keys_map.range(..=key.to_vec()).next_back()` is
    /// * `None` if S is empty.
    /// * `Some(M)` with M the maximum of S if S is non-empty.
    ///
    /// First, if `self.get_existing_find_keys_entry(key) == Some(stored_key)` then given the code,
    /// clearly `stored_key` is a prefix of key.
    ///
    /// Conversely, let us assume that `self.find_keys_map.keys()` contains a certain prefix `p` of key.
    /// Because in particular `p <= key`, we have `self.find_keys_map.range(..=key.to_vec()).next_back() == Some((stored_key, _))`
    /// for some value `stored_key` such that `p <= stored_key <= key`.
    ///
    /// Next, let us prove that `p` is a prefix of `stored_key`. (This will entail `stored_key == p` due to
    /// the prefix-free property of `self.find_keys_map`, therefore the algorithm's answer is correct.)
    ///
    /// By contradiction. Let `w` be the longest common prefix between `p` and `stored_key`. If `p = w p2` and
    /// `stored_key = w s2` with `p2` non-empty, then `p <= stored_key` implies `s2 > p2`. But `p` is also a
    /// prefix of `key` therefore `stored_key > key`, contradiction.
    fn get_existing_find_keys_entry(&self, key: &[u8]) -> Option<(&Vec<u8>, &FindKeysEntry)> {
        match self.find_keys_map.range(..=key.to_vec()).next_back() {
            None => None,
            Some((stored_key, value)) => {
                if key.starts_with(stored_key) {
                    Some((stored_key, value))
                } else {
                    None
                }
            }
        }
    }

    /// Same as above but returns a mutable reference.
    fn get_existing_keys_entry_mut(
        &mut self,
        key: &[u8],
    ) -> Option<(&Vec<u8>, &mut FindKeysEntry)> {
        match self.find_keys_map.range_mut(..=key.to_vec()).next_back() {
            None => None,
            Some((stored_key, value)) => {
                if key.starts_with(stored_key) {
                    Some((stored_key, value))
                } else {
                    None
                }
            }
        }
    }

    /// If the FindKeyValues map contain a prefix that is a prefix of key in argument,
    /// then returns it and the corresponding FindKeyValues. Otherwise `None`.
    fn get_existing_find_key_values_entry(
        &self,
        key: &[u8],
    ) -> Option<(&Vec<u8>, &FindKeyValuesEntry)> {
        match self.find_key_values_map.range(..=key.to_vec()).next_back() {
            None => None,
            Some((stored_key, value)) => {
                if key.starts_with(stored_key) {
                    Some((stored_key, value))
                } else {
                    None
                }
            }
        }
    }

    /// Same as above but returns a mutable reference.
    fn get_existing_find_key_values_entry_mut(
        &mut self,
        key: &[u8],
    ) -> Option<(&Vec<u8>, &mut FindKeyValuesEntry)> {
        match self
            .find_key_values_map
            .range_mut(..=key.to_vec())
            .next_back()
        {
            None => None,
            Some((stored_key, value)) => {
                if key.starts_with(stored_key) {
                    Some((stored_key, value))
                } else {
                    None
                }
            }
        }
    }

    /// Trim value cache so that it fits within bounds.
    fn trim_value_cache(&mut self) {
        let mut keys = Vec::new();
        let mut control_size = self.total_value_size;
        let mut iter = self.queue.iter();
        loop {
            let value = iter.next();
            let Some((cache_key, size)) = value else {
                break;
            };
            if control_size < self.config.max_cache_value_size {
                break;
            }
            if let CacheKey::Value(key) = cache_key {
                control_size -= size;
                keys.push(key.to_vec());
            }
        }
        for key in keys {
            assert!(self.value_map.remove(&key).is_some());
            let cache_key = CacheKey::Value(key);
            self.remove_cache_key(&cache_key);
        }
    }

    /// Trim `find_keys_by_prefix` cache so that it fits within bounds.
    fn trim_find_keys_cache(&mut self) {
        let mut prefixes = Vec::new();
        let mut control_size = self.total_find_keys_size;
        let mut iter = self.queue.iter();
        loop {
            let value = iter.next();
            let Some((cache_key, size)) = value else {
                break;
            };
            if control_size < self.config.max_cache_find_keys_size {
                break;
            }
            if let CacheKey::FindKeys(prefix) = cache_key {
                control_size -= size;
                prefixes.push(prefix.to_vec());
            }
        }
        for prefix in prefixes {
            assert!(self.find_keys_map.remove(&prefix).is_some());
            let cache_key = CacheKey::FindKeys(prefix);
            self.remove_cache_key(&cache_key);
        }
    }

    /// Trim `find_key_values_by_prefix` cache so that it fits within bounds.
    fn trim_find_key_values_cache(&mut self) {
        let mut prefixes = Vec::new();
        let mut control_size = self.total_find_key_values_size;
        let mut iter = self.queue.iter();
        loop {
            let value = iter.next();
            let Some((cache_key, size)) = value else {
                break;
            };
            if control_size < self.config.max_cache_find_key_values_size {
                break;
            }
            if let CacheKey::FindKeyValues(prefix) = cache_key {
                control_size -= size;
                prefixes.push(prefix.to_vec());
            }
        }
        for prefix in prefixes {
            assert!(self.find_key_values_map.remove(&prefix).is_some());
            let cache_key = CacheKey::FindKeyValues(prefix);
            self.remove_cache_key(&cache_key);
        }
    }

    /// Trim the cache so that it fits within the constraints.
    fn trim_cache(&mut self) {
        while self.total_size >= self.config.max_cache_size
            || self.queue.len() >= self.config.max_cache_entries
        {
            let Some((cache_key, size)) = self.queue.pop_front() else {
                break;
            };
            self.decrease_sizes(&cache_key, size);
            self.remove_cache_key_from_map(&cache_key);
        }
    }

    /// Inserts an entry into the cache.
    fn insert_value(&mut self, key: &[u8], cache_entry: ValueEntry) {
        if self.config.max_value_entry_size == 0 {
            // If the maximum size of an entry is zero, then we do not insert
            return;
        }
        let size = key.len() + cache_entry.size();
        if (matches!(cache_entry, ValueEntry::DoesNotExist) && !self.has_exclusive_access)
            || size > self.config.max_value_entry_size
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
    pub(crate) fn put_key_value(&mut self, key: &[u8], value: &[u8]) {
        if self.has_exclusive_access {
            let lower_bound = self.get_existing_keys_entry_mut(key);
            if let Some((lower_bound, cache_entry)) = lower_bound {
                let reduced_key = &key[lower_bound.len()..];
                cache_entry.update_entry(reduced_key, true);
                let cache_key = CacheKey::FindKeys(lower_bound.to_vec());
                let new_size = lower_bound.len() + cache_entry.size();
                self.update_cache_key_sizes(&cache_key, new_size);
            }
            match self.get_existing_find_key_values_entry_mut(key) {
                Some((lower_bound, cache_entry)) => {
                    let reduced_key = &key[lower_bound.len()..];
                    cache_entry.update_entry(reduced_key, Some(value.to_vec()));
                    let cache_key = CacheKey::FindKeyValues(lower_bound.to_vec());
                    let new_size = lower_bound.len() + cache_entry.size();
                    self.update_cache_key_sizes(&cache_key, new_size);
                    let cache_key = CacheKey::Value(key.to_vec());
                    self.remove_cache_key_if_exists(&cache_key);
                }
                None => {
                    let cache_entry = ValueEntry::Value(value.to_vec());
                    self.insert_value(key, cache_entry);
                }
            }
        } else {
            let cache_entry = ValueEntry::Value(value.to_vec());
            self.insert_value(key, cache_entry);
        }
    }

    /// Deletes a key from the cache.
    pub(crate) fn delete_key(&mut self, key: &[u8]) {
        if self.has_exclusive_access {
            let lower_bound = self.get_existing_keys_entry_mut(key);
            let mut matching = false; // If matching, no need to insert in the value cache
            if let Some((lower_bound, cache_entry)) = lower_bound {
                let reduced_key = &key[lower_bound.len()..];
                cache_entry.update_entry(reduced_key, false);
                let cache_key = CacheKey::FindKeys(lower_bound.to_vec());
                let new_size = lower_bound.len() + cache_entry.size();
                self.update_cache_key_sizes(&cache_key, new_size);
                matching = true;
            }
            let lower_bound = self.get_existing_find_key_values_entry_mut(key);
            if let Some((lower_bound, cache_entry)) = lower_bound {
                let reduced_key = &key[lower_bound.len()..];
                cache_entry.update_entry(reduced_key, None);
                let cache_key = CacheKey::FindKeyValues(lower_bound.to_vec());
                let new_size = lower_bound.len() + cache_entry.size();
                self.update_cache_key_sizes(&cache_key, new_size);
                matching = true;
            }
            if !matching {
                let cache_entry = ValueEntry::DoesNotExist;
                self.insert_value(key, cache_entry);
            } else {
                let cache_key = CacheKey::Value(key.to_vec());
                self.remove_cache_key_if_exists(&cache_key);
            }
        } else {
            let cache_key = CacheKey::Value(key.to_vec());
            self.remove_cache_key_if_exists(&cache_key);
        }
    }

    /// Inserts a read_value result into the cache.
    pub(crate) fn insert_read_value(&mut self, key: &[u8], value: &Option<Vec<u8>>) {
        // We do not check for the find-key-values to update. Because we would have
        // matched if existing.
        let cache_entry = match value {
            None => ValueEntry::DoesNotExist,
            Some(vec) => ValueEntry::Value(vec.to_vec()),
        };
        self.insert_value(key, cache_entry)
    }

    /// Inserts a contains_key result into the cache.
    pub(crate) fn insert_contains_key(&mut self, key: &[u8], result: bool) {
        // We do not check for the find-keys / find-key-values to update.
        // Because we would have matched if existing.
        let cache_entry = if result {
            ValueEntry::Exists
        } else {
            ValueEntry::DoesNotExist
        };
        self.insert_value(key, cache_entry)
    }

    /// Inserts the result of `find_keys_by_prefix` in the cache.
    pub(crate) fn insert_find_keys(&mut self, key_prefix: Vec<u8>, keys: &[Vec<u8>]) {
        if self.config.max_find_keys_entry_size == 0 {
            // zero max size, exit from the start
            return;
        }
        let size = key_prefix.len() + keys.iter().map(Vec::len).sum::<usize>();
        if size > self.config.max_find_keys_entry_size {
            // The entry is too large, we do not insert it,
            return;
        }
        let find_entry = FindKeysEntry(keys.iter().cloned().collect());
        // Clearing up the FindKeys entries that are covered by the new FindKeys.
        let keys = self
            .find_keys_map
            .range(get_key_range_for_prefix(key_prefix.clone()))
            .map(|(x, _)| x.clone())
            .collect::<Vec<_>>();
        for key in keys {
            assert!(self.find_keys_map.remove(&key).is_some());
            let cache_key = CacheKey::FindKeys(key);
            self.remove_cache_key(&cache_key);
        }
        // Clearing up the value entries as they are covered by the new FindKeys.
        // That is the `Exists` and `DoesNotExist`. The Value entries are not covered by FindKeys.
        let keys = self
            .value_map
            .range(get_key_range_for_prefix(key_prefix.clone()))
            .filter_map(|(key, value)| match value {
                ValueEntry::DoesNotExist => Some(key.to_vec()),
                ValueEntry::Exists => Some(key.to_vec()),
                ValueEntry::Value(_) => None,
            })
            .collect::<Vec<_>>();
        for key in keys {
            assert!(self.value_map.remove(&key).is_some());
            let cache_key = CacheKey::Value(key);
            self.remove_cache_key(&cache_key);
        }
        let cache_key = CacheKey::FindKeys(key_prefix.clone());
        // The entry has to be missing otherwise, it would have been found
        assert!(self.find_keys_map.insert(key_prefix, find_entry).is_none());
        self.insert_cache_key(cache_key, size);
        self.trim_find_keys_cache();
        self.trim_cache();
    }

    /// Inserts the result of `find_key_values_by_prefix` in the cache.
    pub(crate) fn insert_find_key_values(
        &mut self,
        key_prefix: Vec<u8>,
        key_values: &[(Vec<u8>, Vec<u8>)],
    ) {
        if self.config.max_find_key_values_entry_size == 0 {
            // Zero, maximum size, exit from the start
            return;
        }
        let size = key_prefix.len()
            + key_values
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>();
        if size > self.config.max_find_key_values_entry_size {
            // The entry is too large, we do not insert it,
            return;
        }
        let find_entry = FindKeyValuesEntry(
            key_values
                .iter()
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect(),
        );
        // Clearing up the FindKeys entries that are covered by the new FindKeyValues.
        let prefixes = self
            .find_keys_map
            .range(get_key_range_for_prefix(key_prefix.clone()))
            .map(|(x, _)| x.clone())
            .collect::<Vec<_>>();
        for prefix in prefixes {
            assert!(self.find_keys_map.remove(&prefix).is_some());
            let cache_key = CacheKey::FindKeys(prefix);
            self.remove_cache_key(&cache_key);
        }
        // Clearing up the FindKeyValues entries that are covered by the new FindKeyValues.
        let prefixes = self
            .find_key_values_map
            .range(get_key_range_for_prefix(key_prefix.clone()))
            .map(|(x, _)| x.clone())
            .collect::<Vec<_>>();
        for prefix in prefixes {
            assert!(self.find_key_values_map.remove(&prefix).is_some());
            let cache_key = CacheKey::FindKeyValues(prefix);
            self.remove_cache_key(&cache_key);
        }
        // Clearing up the value entries as they are covered by the new FindKeyValues.
        let keys = self
            .value_map
            .range(get_key_range_for_prefix(key_prefix.clone()))
            .map(|(x, _)| x.clone())
            .collect::<Vec<_>>();
        for key in keys {
            assert!(self.value_map.remove(&key).is_some());
            let cache_key = CacheKey::Value(key);
            self.remove_cache_key(&cache_key);
        }
        let cache_key = CacheKey::FindKeyValues(key_prefix.clone());
        // The entry has to be missing otherwise, it would have been found
        assert!(self
            .find_key_values_map
            .insert(key_prefix, find_entry)
            .is_none());
        self.insert_cache_key(cache_key, size);
        self.trim_find_key_values_cache();
        self.trim_cache();
    }

    /// Marks cached keys that match the prefix as deleted. Importantly, this does not
    /// create new entries in the cache.
    pub(crate) fn delete_prefix(&mut self, key_prefix: &[u8]) {
        // When using delete_prefix, we do not insert `ValueEntry::DoesNotExist`
        // and instead drop the entries from the value-cache
        // This is because:
        // * In non-exclusive access, this could be added by another user.
        // * In exclusive access, we do this via the `FindKeyValues`.
        let mut keys = Vec::new();
        for (key, _) in self
            .value_map
            .range(get_key_range_for_prefix(key_prefix.to_vec()))
        {
            keys.push(key.to_vec());
        }
        for key in keys {
            assert!(self.value_map.remove(&key).is_some());
            let cache_key = CacheKey::Value(key);
            self.remove_cache_key(&cache_key);
        }
        if self.has_exclusive_access {
            // Remove the FindKeys that are covered by key_prefix.
            let mut prefixes = Vec::new();
            for (prefix, _) in self
                .find_keys_map
                .range(get_key_range_for_prefix(key_prefix.to_vec()))
            {
                prefixes.push(prefix.to_vec());
            }
            for prefix in prefixes {
                assert!(self.find_keys_map.remove(&prefix).is_some());
                let cache_key = CacheKey::FindKeys(prefix);
                self.remove_cache_key(&cache_key);
            }
            // Remove the FindKeyValues that are covered by key_prefix.
            let mut prefixes = Vec::new();
            for (prefix, _) in self
                .find_key_values_map
                .range(get_key_range_for_prefix(key_prefix.to_vec()))
            {
                prefixes.push(prefix.to_vec());
            }
            for prefix in prefixes {
                assert!(self.find_key_values_map.remove(&prefix).is_some());
                let cache_key = CacheKey::FindKeyValues(prefix);
                self.remove_cache_key(&cache_key);
            }
            // Finding a containing FindKeys. If existing update.
            let lower_bound = self.get_existing_keys_entry_mut(key_prefix);
            let result = if let Some((lower_bound, find_entry)) = lower_bound {
                // Delete the keys in the entry
                let key_prefix_red = &key_prefix[lower_bound.len()..];
                find_entry.delete_prefix(key_prefix_red);
                let new_cache_size = find_entry.size() + lower_bound.len();
                Some((new_cache_size, lower_bound.clone()))
            } else {
                None
            };
            if let Some((new_cache_size, lower_bound)) = result {
                // Update the size without changing the position.
                let cache_key = CacheKey::FindKeys(lower_bound.clone());
                self.update_cache_key_sizes(&cache_key, new_cache_size);
            }
            // Finding a containing FindKeyValues. If existing update, if not insert.
            let lower_bound = self.get_existing_find_key_values_entry_mut(key_prefix);
            let result = if let Some((lower_bound, find_entry)) = lower_bound {
                // Delete the keys (or key/values) in the entry
                let key_prefix_red = &key_prefix[lower_bound.len()..];
                find_entry.delete_prefix(key_prefix_red);
                let new_cache_size = find_entry.size() + lower_bound.len();
                Some((new_cache_size, lower_bound.clone()))
            } else {
                None
            };
            if let Some((new_cache_size, lower_bound)) = result {
                // Update the size without changing the position.
                let cache_key = CacheKey::FindKeyValues(lower_bound.clone());
                self.update_cache_key_sizes(&cache_key, new_cache_size);
            } else {
                // There is no lower bound. Therefore we can insert
                // the deleted prefix as a FindKeyValues.
                let size = key_prefix.len();
                let cache_key = CacheKey::FindKeyValues(key_prefix.to_vec());
                let find_key_values_entry = FindKeyValuesEntry(BTreeMap::new());
                self.find_key_values_map
                    .insert(key_prefix.to_vec(), find_key_values_entry);
                self.insert_cache_key(cache_key, size);
            }
        }
    }

    /// Returns the cached value, or `Some(None)` if the entry does not exist in the
    /// database. If `None` is returned, the entry might exist in the database but is
    /// not in the cache.
    pub(crate) fn query_read_value(&mut self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        // First, query the value map
        let result = match self.value_map.get(key) {
            None => None,
            Some(entry) => match entry {
                ValueEntry::DoesNotExist => Some(None),
                ValueEntry::Exists => None,
                ValueEntry::Value(vec) => Some(Some(vec.clone())),
            },
        };
        if result.is_some() {
            let cache_key = CacheKey::Value(key.to_vec());
            self.move_cache_key_on_top(cache_key);
            return result;
        }
        if self.has_exclusive_access {
            // Now trying the FindKeyValues map.
            let lower_bound = self.get_existing_find_key_values_entry(key);
            let (lower_bound, result) = if let Some((lower_bound, find_entry)) = lower_bound {
                let reduced_key = &key[lower_bound.len()..];
                (lower_bound, find_entry.get_read_value(reduced_key))
            } else {
                return None;
            };
            let cache_key = CacheKey::FindKeyValues(lower_bound.clone());
            self.move_cache_key_on_top(cache_key);
            Some(result)
        } else {
            None
        }
    }

    /// Returns `Some(true)` or `Some(false)` if we know that the entry does or does not
    /// exist in the database. Returns `None` if that information is not in the cache.
    pub(crate) fn query_contains_key(&mut self, key: &[u8]) -> Option<bool> {
        // First try on the value_map
        let result = self
            .value_map
            .get(key)
            .map(|entry| !matches!(entry, ValueEntry::DoesNotExist));
        if result.is_some() {
            let cache_key = CacheKey::Value(key.to_vec());
            self.move_cache_key_on_top(cache_key);
            return result;
        }
        if self.has_exclusive_access {
            // Now trying the FindKeys map.
            let lower_bound = self.get_existing_find_keys_entry(key);
            let result = if let Some((lower_bound, find_entry)) = lower_bound {
                let reduced_key = &key[lower_bound.len()..];
                Some((lower_bound, find_entry.contains_key(reduced_key)))
            } else {
                None
            };
            if let Some((lower_bound, result)) = result {
                let cache_key = CacheKey::FindKeys(lower_bound.clone());
                self.move_cache_key_on_top(cache_key);
                return Some(result);
            }
            // Now trying the FindKeyValues map.
            let lower_bound = self.get_existing_find_key_values_entry(key);
            let (lower_bound, result) = if let Some((lower_bound, find_entry)) = lower_bound {
                let reduced_key = &key[lower_bound.len()..];
                (lower_bound, find_entry.contains_key(reduced_key))
            } else {
                return None;
            };
            let cache_key = CacheKey::FindKeyValues(lower_bound.clone());
            self.move_cache_key_on_top(cache_key);
            return Some(result);
        }
        result
    }

    /// Gets the find_keys entry from the key prefix. Returns `None` if absent from the cache.
    pub(crate) fn query_find_keys(&mut self, key_prefix: &[u8]) -> Option<Vec<Vec<u8>>> {
        // Trying first the FindKeys cache.
        let result = match self.get_existing_find_keys_entry(key_prefix) {
            None => None,
            Some((lower_bound, cache_entry)) => {
                let key_prefix_red = key_prefix[lower_bound.len()..].to_vec();
                Some((lower_bound, cache_entry.get_keys_by_prefix(key_prefix_red)))
            }
        };
        if let Some((lower_bound, keys)) = result {
            let cache_key = CacheKey::FindKeys(lower_bound.clone());
            self.move_cache_key_on_top(cache_key);
            return Some(keys);
        }
        // Then with the FindKeyValues cache.
        let (lower_bound, result) = match self.get_existing_find_key_values_entry(key_prefix) {
            None => {
                return None;
            }
            Some((lower_bound, cache_entry)) => {
                let key_prefix_red = key_prefix[lower_bound.len()..].to_vec();
                (lower_bound, cache_entry.get_keys_by_prefix(key_prefix_red))
            }
        };
        let cache_key = CacheKey::FindKeyValues(lower_bound.clone());
        self.move_cache_key_on_top(cache_key);
        Some(result)
    }

    /// Gets the find key values entry from the key prefix. Returns `None` if absent from the cache.
    pub(crate) fn query_find_key_values(
        &mut self,
        key_prefix: &[u8],
    ) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let (lower_bound, result) = match self.get_existing_find_key_values_entry(key_prefix) {
            None => {
                return None;
            }
            Some((lower_bound, cache_entry)) => {
                let key_prefix_red = &key_prefix[lower_bound.len()..];
                (lower_bound, cache_entry.get_find_key_values(key_prefix_red))
            }
        };
        let cache_key = CacheKey::FindKeyValues(lower_bound.to_vec());
        self.move_cache_key_on_top(cache_key);
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rand::Rng;

    use super::*;

    fn check_prefix_free(set: BTreeSet<Vec<u8>>) {
        let keys = set.into_iter().collect::<Vec<_>>();
        let num_keys = keys.len();
        println!("keys={keys:?}");
        for i in 0..num_keys {
            for j in 0..num_keys {
                if i != j {
                    let key1 = keys[i].clone();
                    let key2 = keys[j].clone();
                    println!("i={i} j={j} key1={key1:?} key2={key2:?}");
                    assert!(!key1.starts_with(&key2));
                }
            }
        }
    }

    fn check_intersection(keys: Vec<Vec<u8>>, prefixes: &BTreeSet<Vec<u8>>) {
        for key in keys {
            for prefix in prefixes {
                assert!(!key.starts_with(prefix), "key matches a prefix");
            }
        }
    }

    impl LruPrefixCache {
        fn check_coherence(&self) {
            let value_map_set = self
                .value_map
                .keys()
                .cloned()
                .collect::<BTreeSet<Vec<u8>>>();
            let find_keys_map_set = self
                .find_keys_map
                .keys()
                .cloned()
                .collect::<BTreeSet<Vec<u8>>>();
            let find_key_values_map_set = self
                .find_key_values_map
                .keys()
                .cloned()
                .collect::<BTreeSet<Vec<u8>>>();
            // Checking prefix-free ness of the find-keys-map / find-key-values-map
            check_prefix_free(find_keys_map_set.clone());
            check_prefix_free(find_key_values_map_set.clone());
            // Building the data set and sizes.
            // Also checking that the sizes are coherent.
            let mut value_queue_set = BTreeSet::new();
            let mut find_keys_queue_set = BTreeSet::new();
            let mut find_key_values_queue_set = BTreeSet::new();
            let mut total_size = 0;
            let mut total_value_size = 0;
            let mut total_find_keys_size = 0;
            let mut total_find_key_values_size = 0;
            for (cache_key, size) in &self.queue {
                let queue_size = *size;
                match cache_key {
                    CacheKey::Value(key) => {
                        let value = self.value_map.get(key).unwrap();
                        let map_size = key.len() + value.size();
                        assert_eq!(map_size, queue_size, "Incoherence in value size");
                        value_queue_set.insert(key.clone());
                        total_value_size += queue_size;
                    }
                    CacheKey::FindKeys(key) => {
                        let value = self.find_keys_map.get(key).unwrap();
                        let map_size = key.len() + value.size();
                        assert_eq!(map_size, queue_size, "Incoherence in find-keys size");
                        find_keys_queue_set.insert(key.clone());
                        total_find_keys_size += queue_size;
                    }
                    CacheKey::FindKeyValues(key) => {
                        let value = self.find_key_values_map.get(key).unwrap();
                        let map_size = key.len() + value.size();
                        assert_eq!(map_size, queue_size, "Incoherence in find-keys size");
                        find_key_values_queue_set.insert(key.clone());
                        total_find_key_values_size += queue_size;
                    }
                }
                total_size += queue_size;
            }
            // Checking that there is no overlap between the maps.
            let value_map_vect = self.value_map.keys().cloned().collect::<Vec<_>>();
            check_intersection(value_map_vect, &find_key_values_map_set);
            let value_existence_map_vect = self
                .value_map
                .iter()
                .filter_map(|(key, value)| match value {
                    ValueEntry::DoesNotExist => Some(key.to_vec()),
                    ValueEntry::Exists => Some(key.to_vec()),
                    ValueEntry::Value(_) => None,
                })
                .collect::<Vec<_>>();
            check_intersection(value_existence_map_vect, &find_keys_map_set);
            // Checking that the set of keys are coherent between the queues and the maps.
            assert_eq!(
                value_queue_set, value_map_set,
                "Incoherence in value_map keys"
            );
            assert_eq!(
                find_keys_queue_set, find_keys_map_set,
                "Incoherence in find_keys_map keys"
            );
            assert_eq!(
                find_key_values_queue_set, find_key_values_map_set,
                "Incoherence in find_key_values_map keys"
            );
            // Checking that the total sizes are coherent.
            assert_eq!(total_size, self.total_size, "The total_size are incoherent");
            assert_eq!(
                total_value_size, self.total_value_size,
                "The total_value_size are incoherent"
            );
            assert_eq!(
                total_find_keys_size, self.total_find_keys_size,
                "The total_find_keys_size are incoherent"
            );
            assert_eq!(
                total_find_key_values_size, self.total_find_key_values_size,
                "The total_find_key_values_size are incoherent"
            );
            // Checking that the size are within the allowed bounds.
            if self.config.max_cache_size > 0 {
                assert!(
                    total_size < self.config.max_cache_size,
                    "The total_size is too large"
                );
            } else {
                assert!(total_size == 0, "The total_size should be 0");
            }
            if self.config.max_cache_value_size > 0 {
                assert!(
                    total_value_size < self.config.max_cache_value_size,
                    "The total_value_size is too large"
                );
            } else {
                assert!(total_value_size == 0, "The total_value_size should be 0");
            }
            if self.config.max_cache_find_keys_size > 0 {
                assert!(
                    total_find_keys_size < self.config.max_cache_find_keys_size,
                    "The total_value_size is too large"
                );
            } else {
                assert!(
                    total_find_keys_size == 0,
                    "The total_find_keys_size should be 0"
                );
            }
            if self.config.max_cache_find_key_values_size > 0 {
                assert!(
                    total_find_key_values_size < self.config.max_cache_find_key_values_size,
                    "The total_find_key_values_size is too large"
                );
            } else {
                assert!(
                    total_find_key_values_size == 0,
                    "The total_find_key_values_size should be 0"
                );
            }
        }
    }

    fn create_test_cache(has_exclusive_access: bool) -> LruPrefixCache {
        let config = StorageCacheConfig {
            max_cache_size: 1000,
            max_value_entry_size: 50,
            max_find_keys_entry_size: 100,
            max_find_key_values_entry_size: 200,
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
        cache.check_coherence();

        // Query the value
        let result = cache.query_read_value(&key);
        assert_eq!(result, Some(Some(value)));

        // Query non-existent key in the cache
        let result = cache.query_read_value(&[9, 9, 9]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_insert_and_query_contains_key() {
        let mut cache = create_test_cache(true);
        let key = vec![1, 2, 3];

        // Insert a key that exists
        cache.insert_contains_key(&key, true);
        cache.check_coherence();

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
        cache.check_coherence();

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
        cache.check_coherence();

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
            cache.check_coherence();
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
            cache.check_coherence();
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
        cache.check_coherence();
        cache.insert_read_value(&key2, &Some(value.clone()));
        cache.check_coherence();

        // Access key1 to promote it
        assert_eq!(Some(Some(value)), cache.query_read_value(&key1));

        // Access key3 to promote it
        assert_eq!(None, cache.query_read_value(&key3));

        // The queue should have key1 at the end (most recently used)
        let queue_keys = cache.queue.keys().collect::<Vec<_>>();
        assert_eq!(queue_keys[queue_keys.len() - 1], &CacheKey::Value(key1));
    }

    #[test]
    fn test_update_find_entry_mut() {
        let mut cache = create_test_cache(true);
        let prefix = vec![1];
        let key = vec![1, 2];
        let original_keys = vec![vec![2], vec![3]];

        // Insert find_keys entry
        cache.insert_find_keys(prefix.clone(), &original_keys);
        cache.check_coherence();

        // Update an entry
        cache.put_key_value(&key, &[42]);
        cache.check_coherence();

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
        cache.check_coherence();
        cache.insert_read_value(&key2, &Some(value.clone()));
        cache.check_coherence();
        cache.insert_read_value(&key3, &Some(value.clone()));
        cache.check_coherence();

        // Delete prefix [1]
        cache.delete_prefix(&prefix);
        cache.check_coherence();

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
    fn test_value_entry_size_limits() {
        let mut cache = create_test_cache(true);
        let key = vec![1];
        let large_value = vec![0; cache.config.max_value_entry_size + 1];

        // Insert value larger than max_value_entry_size
        // This is because the entry size is the key size + the value size
        cache.insert_read_value(&key, &Some(large_value));
        cache.check_coherence();

        // Should not be cached
        let result = cache.query_read_value(&key);
        assert_eq!(result, None);
    }

    #[test]
    fn test_findkeys_entry_size_limits() {
        let mut cache = create_test_cache(true);
        let key_prefix = vec![1];
        let mut keys = Vec::new();
        for i in 0..cache.config.max_find_keys_entry_size {
            keys.push(vec![i as u8]);
        }
        let size = keys.iter().map(Vec::len).sum::<usize>();
        assert_eq!(cache.config.max_find_keys_entry_size, size);
        // Insert value larger than max_entry_size
        // This is because the entry size is the key size + the value size
        cache.insert_find_keys(key_prefix.clone(), &keys);
        cache.check_coherence();

        // Should not be cached
        let result = cache.query_find_keys(&key_prefix);
        assert_eq!(result, None);
    }

    #[test]
    fn test_findkeyvalues_entry_size_limits() {
        let mut cache = create_test_cache(true);
        let key_prefix = vec![1];
        let mut key_values = Vec::new();
        for i in 0..cache.config.max_find_key_values_entry_size / 2 {
            key_values.push((vec![i as u8], vec![i as u8]));
        }
        let size = key_values
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>();
        assert_eq!(cache.config.max_find_key_values_entry_size, size);

        // Insert value larger than max_entry_size
        cache.insert_find_key_values(key_prefix.clone(), &key_values);
        cache.check_coherence();

        // Should not be cached
        let result = cache.query_find_key_values(&key_prefix);
        assert_eq!(result, None);
    }

    #[test]
    fn test_does_not_exist_entry_without_exclusive_access() {
        let mut cache = create_test_cache(false);
        let key = vec![1];

        // Insert DoesNotExist entry without exclusive access
        cache.insert_read_value(&key, &None);
        cache.check_coherence();

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
        find_entry.update_entry(&key1, true);
        find_entry.update_entry(&key2, true);

        // Test contains_key
        assert!(find_entry.contains_key(&key1));
        assert!(find_entry.contains_key(&key2));
        assert!(!find_entry.contains_key(&key3));
        assert!(!find_entry.contains_key(&[9, 9]));

        // Test get_keys_by_prefix
        let keys = find_entry.get_keys_by_prefix(vec![1]);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&vec![2]));
        assert!(keys.contains(&vec![3]));

        // Remove a key
        find_entry.update_entry(&key1, false);
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
        cache.check_coherence();

        // Size should increase
        assert!(cache.total_size > initial_size);
        assert!(cache.total_value_size > initial_value_size);

        let value_size_with_entry = cache.total_value_size;

        // Insert DoesNotExist entry (None value)
        cache.insert_read_value(&key, &None);
        cache.check_coherence();

        // Value size should be less than when we had a real value,
        // since DoesNotExist entries have size 0 for the value part
        assert!(cache.total_value_size < value_size_with_entry);
    }

    #[test]
    fn test_trim_value_cache() {
        let mut cache = LruPrefixCache::new(
            StorageCacheConfig {
                max_cache_size: 10000,
                max_value_entry_size: 500,
                max_find_keys_entry_size: 1000,
                max_find_key_values_entry_size: 2000,
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
            cache.check_coherence();
        }

        // Should have trimmed to stay within value cache limit
        assert!(cache.total_value_size <= cache.config.max_cache_value_size);
    }

    fn has_a_prefix_slow_method(prefixes: &BTreeSet<Vec<u8>>, key: &[u8]) -> bool {
        for prefix in prefixes {
            if key.starts_with(prefix) {
                return true;
            }
        }
        false
    }

    fn has_a_prefix_fast_method(prefixes: &BTreeSet<Vec<u8>>, key: &[u8]) -> bool {
        match prefixes.range(..=key.to_vec()).next_back() {
            None => false,
            Some(prefix) => key.starts_with(prefix),
        }
    }

    fn has_a_prefix(prefixes: &BTreeSet<Vec<u8>>, key: &[u8]) -> bool {
        let test1 = has_a_prefix_slow_method(prefixes, key);
        let test2 = has_a_prefix_fast_method(prefixes, key);
        assert_eq!(
            test1, test2,
            "The methods for testing prefix return different results"
        );
        test1
    }

    fn get_prefix<R: Rng>(rng: &mut R, max_len: usize, entry_size: usize) -> Vec<u8> {
        let len = rng.gen_range(1..=max_len);
        let mut prefix = Vec::new();
        for _ in 0..len {
            let entry = rng.gen_range(0..entry_size);
            prefix.push(entry as u8);
        }
        prefix
    }

    fn insert_into_prefix_set(prefixes: &mut BTreeSet<Vec<u8>>, new_prefix: Vec<u8>) {
        if has_a_prefix(prefixes, &new_prefix) {
            return;
        }
        let removed_keys1 = prefixes
            .range(get_key_range_for_prefix(new_prefix.clone()))
            .cloned()
            .collect::<BTreeSet<Vec<u8>>>();
        let mut removed_keys2 = BTreeSet::new();
        for prefix in prefixes.clone() {
            if prefix.starts_with(&new_prefix) {
                removed_keys2.insert(prefix);
            }
        }
        assert_eq!(
            removed_keys1, removed_keys2,
            "Inconsistent result of the computation of the intervals"
        );
        for prefix in removed_keys2 {
            prefixes.remove(&prefix);
        }
        prefixes.insert(new_prefix);
    }

    fn test_prefix_free_set(max_len: usize, num_gen: usize, key_size: usize) {
        let mut rng = crate::random::make_deterministic_rng();
        let mut prefixes = BTreeSet::new();
        for _ in 0..num_gen {
            let new_prefix = get_prefix(&mut rng, max_len, key_size);
            insert_into_prefix_set(&mut prefixes, new_prefix);
        }
    }

    // The functions `get_existing_find_{keys,key_values}_entry(_mut)`
    // need to be tested. The following test does some random tests
    // on the generated prefix free sets. Two methods are used and
    // their consistency are checked.
    #[test]
    fn test_lower_bounds() {
        test_prefix_free_set(10, 500, 2);
        test_prefix_free_set(6, 500, 3);
        test_prefix_free_set(5, 500, 4);
    }

    #[test]
    fn test_delete_key_operations() {
        let mut cache = create_test_cache(true);
        let key1 = vec![1, 2, 3];
        let key2 = vec![1, 2, 4];
        let key3 = vec![2, 3, 4];
        let value = vec![42, 43, 44];

        // Insert some values
        cache.put_key_value(&key1, &value);
        cache.check_coherence();
        cache.put_key_value(&key2, &value);
        cache.check_coherence();
        cache.put_key_value(&key3, &value);
        cache.check_coherence();

        // Verify values are present
        assert_eq!(cache.query_read_value(&key1), Some(Some(value.clone())));
        assert_eq!(cache.query_read_value(&key2), Some(Some(value.clone())));
        assert_eq!(cache.query_read_value(&key3), Some(Some(value.clone())));

        // Delete key1
        cache.delete_key(&key1);
        cache.check_coherence();

        // Check that key1 is now marked as DoesNotExist
        assert_eq!(cache.query_read_value(&key1), Some(None));
        // Other keys should remain unchanged
        assert_eq!(cache.query_read_value(&key2), Some(Some(value.clone())));
        assert_eq!(cache.query_read_value(&key3), Some(Some(value.clone())));

        // Delete key2
        cache.delete_key(&key2);
        cache.check_coherence();

        // Check that key2 is now marked as DoesNotExist
        assert_eq!(cache.query_read_value(&key2), Some(None));
        // key3 should remain unchanged
        assert_eq!(cache.query_read_value(&key3), Some(Some(value)));
    }

    #[test]
    fn test_find_key_values_entry_delete_prefix() {
        let mut find_entry = FindKeyValuesEntry(BTreeMap::new());

        // Add some key-value pairs
        let key1 = vec![1, 2, 3];
        let key2 = vec![1, 2, 4];
        let key3 = vec![1, 3, 5];
        let key4 = vec![2, 4, 6];
        let value = vec![42];

        find_entry.update_entry(&key1, Some(value.clone()));
        find_entry.update_entry(&key2, Some(value.clone()));
        find_entry.update_entry(&key3, Some(value.clone()));
        find_entry.update_entry(&key4, Some(value.clone()));

        // Verify all keys are present
        assert!(find_entry.contains_key(&key1));
        assert!(find_entry.contains_key(&key2));
        assert!(find_entry.contains_key(&key3));
        assert!(find_entry.contains_key(&key4));

        // Delete prefix [1, 2] - should remove key1 and key2
        find_entry.delete_prefix(&[1, 2]);

        // Check that keys with prefix [1, 2] are removed
        assert!(!find_entry.contains_key(&key1));
        assert!(!find_entry.contains_key(&key2));
        // Keys with different prefixes should remain
        assert!(find_entry.contains_key(&key3));
        assert!(find_entry.contains_key(&key4));

        // Delete prefix [1] - should remove key3
        find_entry.delete_prefix(&[1]);

        // Check that key3 is now removed
        assert!(!find_entry.contains_key(&key3));
        // key4 with different prefix should remain
        assert!(find_entry.contains_key(&key4));
    }

    #[test]
    fn test_trim_value_cache_removes_entries() {
        let mut cache = LruPrefixCache::new(
            StorageCacheConfig {
                max_cache_size: 10000,
                max_value_entry_size: 500,
                max_find_keys_entry_size: 1000,
                max_find_key_values_entry_size: 2000,
                max_cache_entries: 100,
                max_cache_value_size: 30, // Very small limit to force removal
                max_cache_find_keys_size: 1000,
                max_cache_find_key_values_size: 1000,
            },
            true,
        );

        // Insert multiple values that exceed the max_cache_value_size
        let mut inserted_keys = Vec::new();
        for i in 0..6 {
            let key = vec![i];
            let value = vec![0; 10]; // Each entry ~10 bytes + key = ~11 bytes
            cache.insert_read_value(&key, &Some(value));
            cache.check_coherence();
            inserted_keys.push(key);
        }

        // After trimming, total value size should be within limit
        assert!(cache.total_value_size <= cache.config.max_cache_value_size);

        // Some entries should have been removed (LRU eviction)
        let mut remaining_count = 0;
        for key in &inserted_keys {
            if cache.query_read_value(key).is_some() {
                remaining_count += 1;
            }
        }

        // We should have fewer entries than we inserted due to trimming
        assert!(remaining_count < inserted_keys.len());

        // The most recently inserted entries should still be present
        let last_key = &inserted_keys[inserted_keys.len() - 1];
        assert!(cache.query_read_value(last_key).is_some());
    }

    #[test]
    fn test_max_value_entry_size_zero_early_termination() {
        let mut cache = LruPrefixCache::new(
            StorageCacheConfig {
                max_cache_size: 1000,
                max_value_entry_size: 0, // Zero size - should cause early termination
                max_find_keys_entry_size: 100,
                max_find_key_values_entry_size: 200,
                max_cache_entries: 10,
                max_cache_value_size: 500,
                max_cache_find_keys_size: 500,
                max_cache_find_key_values_size: 500,
            },
            true,
        );

        let key1 = vec![1, 2, 3];
        let key2 = vec![4, 5, 6];
        let value = vec![42, 43, 44];

        // Insert values - should be terminated early due to max_value_entry_size == 0
        cache.insert_read_value(&key1, &Some(value.clone()));
        cache.check_coherence();
        cache.insert_read_value(&key2, &None);
        cache.check_coherence();

        // Values should not be cached due to early termination
        assert_eq!(cache.query_read_value(&key1), None);
        assert_eq!(cache.query_read_value(&key2), None);

        // Cache should remain empty
        assert_eq!(cache.total_size, 0);
        assert_eq!(cache.total_value_size, 0);
        assert!(cache.value_map.is_empty());
        assert!(cache.queue.is_empty());

        // Test put_key_value also respects the early termination
        cache.put_key_value(&key1, &value);
        cache.check_coherence();

        assert_eq!(cache.query_read_value(&key1), None);
        assert_eq!(cache.total_size, 0);

        // Test delete_key also respects the early termination
        cache.delete_key(&key1);
        cache.check_coherence();

        assert_eq!(cache.query_read_value(&key1), None);
        assert_eq!(cache.total_size, 0);
    }

    #[test]
    fn test_key_value_replacement_same_length() {
        let mut cache = create_test_cache(true);
        let key = vec![1, 2, 3];
        let value1 = vec![42, 43, 44]; // 3 bytes
        let value2 = vec![99, 98, 97]; // 3 bytes - same length as value1

        // Insert initial value
        cache.put_key_value(&key, &value1);
        cache.check_coherence();

        // Verify initial value is present
        assert_eq!(cache.query_read_value(&key), Some(Some(value1.clone())));

        let initial_size = cache.total_size;
        let initial_value_size = cache.total_value_size;

        // Replace with value of same length
        cache.put_key_value(&key, &value2);
        cache.check_coherence();

        // Verify new value is present
        assert_eq!(cache.query_read_value(&key), Some(Some(value2.clone())));

        // Size should remain the same since values have same length
        assert_eq!(cache.total_size, initial_size);
        assert_eq!(cache.total_value_size, initial_value_size);

        // Cache should still contain exactly one entry
        assert_eq!(cache.value_map.len(), 1);
        assert_eq!(cache.queue.len(), 1);

        // Test replacement with insert_read_value as well
        let value3 = vec![11, 22, 33]; // 3 bytes - same length
        cache.insert_read_value(&key, &Some(value3.clone()));
        cache.check_coherence();

        // Verify replacement worked
        assert_eq!(cache.query_read_value(&key), Some(Some(value3)));

        // Size should still be the same
        assert_eq!(cache.total_size, initial_size);
        assert_eq!(cache.total_value_size, initial_value_size);
    }

    #[test]
    fn test_max_find_keys_entry_size_zero_early_termination() {
        let mut cache = LruPrefixCache::new(
            StorageCacheConfig {
                max_cache_size: 1000,
                max_value_entry_size: 100,
                max_find_keys_entry_size: 0, // Zero size - should cause early termination
                max_find_key_values_entry_size: 200,
                max_cache_entries: 10,
                max_cache_value_size: 500,
                max_cache_find_keys_size: 500,
                max_cache_find_key_values_size: 500,
            },
            true,
        );

        let prefix = vec![1, 2];
        let keys = vec![vec![3], vec![4], vec![5]];

        // Insert find_keys entry - should be terminated early due to max_find_keys_entry_size == 0
        cache.insert_find_keys(prefix.clone(), &keys);
        cache.check_coherence();

        // Find keys should not be cached due to early termination
        assert_eq!(cache.query_find_keys(&prefix), None);

        // Cache should remain empty for find_keys
        assert_eq!(cache.total_find_keys_size, 0);
        assert!(cache.find_keys_map.is_empty());

        // Verify no find_keys entries in the queue
        for (cache_key, _) in &cache.queue {
            assert!(!matches!(cache_key, CacheKey::FindKeys(_)));
        }

        // Test with different prefix and keys to ensure consistent behavior
        let prefix2 = vec![9];
        let keys2 = vec![vec![1]];
        cache.insert_find_keys(prefix2.clone(), &keys2);
        cache.check_coherence();

        assert_eq!(cache.query_find_keys(&prefix2), None);
        assert_eq!(cache.total_find_keys_size, 0);
        assert!(cache.find_keys_map.is_empty());
    }

    #[test]
    fn test_max_find_key_values_entry_size_zero_early_termination() {
        let mut cache = LruPrefixCache::new(
            StorageCacheConfig {
                max_cache_size: 1000,
                max_value_entry_size: 100,
                max_find_keys_entry_size: 100,
                max_find_key_values_entry_size: 0, // Zero size - should cause early termination
                max_cache_entries: 10,
                max_cache_value_size: 500,
                max_cache_find_keys_size: 500,
                max_cache_find_key_values_size: 500,
            },
            true,
        );

        let prefix = vec![1, 2];
        let key_values = vec![(vec![3], vec![10]), (vec![4], vec![20])];

        // Insert find_key_values entry - should be terminated early due to max_find_key_values_entry_size == 0
        cache.insert_find_key_values(prefix.clone(), &key_values);
        cache.check_coherence();

        // Find key values should not be cached due to early termination
        assert_eq!(cache.query_find_key_values(&prefix), None);

        // Cache should remain empty for find_key_values
        assert_eq!(cache.total_find_key_values_size, 0);
        assert!(cache.find_key_values_map.is_empty());

        // Verify no find_key_values entries in the queue
        for (cache_key, _) in &cache.queue {
            assert!(!matches!(cache_key, CacheKey::FindKeyValues(_)));
        }

        // Test with different data to ensure consistent behavior
        let prefix2 = vec![9];
        let key_values2 = vec![(vec![1], vec![100])];
        cache.insert_find_key_values(prefix2.clone(), &key_values2);
        cache.check_coherence();

        assert_eq!(cache.query_find_key_values(&prefix2), None);
        assert_eq!(cache.total_find_key_values_size, 0);
        assert!(cache.find_key_values_map.is_empty());
    }

    #[test]
    fn test_value_entry_exists_returns_none() {
        let mut cache = create_test_cache(true);
        let key = vec![1, 2, 3];

        // Insert a ValueEntry::Exists (this happens when we cache that a key exists but not its value)
        cache.insert_contains_key(&key, true);
        cache.check_coherence();

        // Verify that contains_key query returns Some(true)
        assert_eq!(cache.query_contains_key(&key), Some(true));

        // When querying for the actual value with ValueEntry::Exists, it should return None
        // because we only know the key exists but don't have the actual value cached
        assert_eq!(cache.query_read_value(&key), None);

        // Test with a key that doesn't exist
        let key2 = vec![4, 5, 6];
        cache.insert_contains_key(&key2, false);
        cache.check_coherence();

        // This should return Some(false) for contains_key
        assert_eq!(cache.query_contains_key(&key2), Some(false));

        // And Some(None) for read_value since we cached that it doesn't exist
        assert_eq!(cache.query_read_value(&key2), Some(None));

        // Verify the cache state - we should have both entries
        assert_eq!(cache.value_map.len(), 2);
        assert_eq!(cache.queue.len(), 2);

        // Both entries should be ValueEntry::Exists and ValueEntry::DoesNotExist respectively
        assert!(matches!(
            cache.value_map.get(&key),
            Some(ValueEntry::Exists)
        ));
        assert!(matches!(
            cache.value_map.get(&key2),
            Some(ValueEntry::DoesNotExist)
        ));
    }

    #[test]
    fn test_find_keys_entry_delete_prefix_function() {
        let mut find_entry = FindKeysEntry(BTreeSet::new());

        // Add some keys
        let key1 = vec![1, 2, 3];
        let key2 = vec![1, 2, 4];
        let key3 = vec![1, 3, 5];
        let key4 = vec![2, 4, 6];

        find_entry.update_entry(&key1, true);
        find_entry.update_entry(&key2, true);
        find_entry.update_entry(&key3, true);
        find_entry.update_entry(&key4, true);

        // Verify all keys are present
        assert!(find_entry.contains_key(&key1));
        assert!(find_entry.contains_key(&key2));
        assert!(find_entry.contains_key(&key3));
        assert!(find_entry.contains_key(&key4));
        assert_eq!(find_entry.0.len(), 4);

        // Test delete_prefix function directly - delete prefix [1, 2]
        find_entry.delete_prefix(&[1, 2]);

        // Check that keys with prefix [1, 2] are removed
        assert!(!find_entry.contains_key(&key1));
        assert!(!find_entry.contains_key(&key2));
        // Keys with different prefixes should remain
        assert!(find_entry.contains_key(&key3));
        assert!(find_entry.contains_key(&key4));
        assert_eq!(find_entry.0.len(), 2);

        // Test delete_prefix with prefix [1] - should remove key3
        find_entry.delete_prefix(&[1]);

        // Check that key3 is now removed
        assert!(!find_entry.contains_key(&key3));
        // key4 with different prefix should remain
        assert!(find_entry.contains_key(&key4));
        assert_eq!(find_entry.0.len(), 1);

        // Test delete_prefix with empty prefix - should remove all remaining keys
        find_entry.delete_prefix(&[]);

        // All keys should be removed
        assert!(!find_entry.contains_key(&key4));
        assert_eq!(find_entry.0.len(), 0);
    }

    #[test]
    fn test_find_keys_size_decrease() {
        let mut cache = create_test_cache(true);
        let prefix = vec![1];
        let initial_keys = vec![vec![2, 3, 4], vec![3, 4, 5]]; // Larger keys

        // Insert initial find_keys entry
        cache.insert_find_keys(prefix.clone(), &initial_keys);
        cache.check_coherence();

        let initial_total_size = cache.total_size;
        let initial_find_keys_size = cache.total_find_keys_size;

        // Now delete one of the keys, which should decrease the size
        let key_to_delete = vec![1, 2, 3, 4]; // This matches prefix + first key
        cache.delete_key(&key_to_delete);
        cache.check_coherence();

        // The find_keys entry should have been updated and size decreased
        assert!(cache.total_size < initial_total_size);
        assert!(cache.total_find_keys_size < initial_find_keys_size);

        // Verify the key was actually removed from the find_keys entry
        let result = cache.query_find_keys(&prefix);
        assert!(result.is_some());
        let keys = result.unwrap();
        assert!(keys.contains(&vec![3, 4, 5])); // Second key should still be there
        assert!(!keys.contains(&vec![2, 3, 4])); // First key should be gone

        cache.check_coherence();
    }

    #[test]
    fn test_trim_find_keys_cache_lru_eviction() {
        let mut cache = LruPrefixCache::new(
            StorageCacheConfig {
                max_cache_size: 10000,
                max_value_entry_size: 500,
                max_find_keys_entry_size: 1000,
                max_find_key_values_entry_size: 2000,
                max_cache_entries: 100,
                max_cache_value_size: 5000,
                max_cache_find_keys_size: 50, // Small limit to trigger trimming
                max_cache_find_key_values_size: 5000,
            },
            true,
        );

        // Insert first find_keys entry
        let prefix1 = vec![1];
        let keys1 = vec![vec![2; 10], vec![3; 10]]; // ~20 bytes
        cache.insert_find_keys(prefix1.clone(), &keys1);
        cache.check_coherence();

        // Insert second find_keys entry
        let prefix2 = vec![2];
        let keys2 = vec![vec![4; 10], vec![5; 10]]; // ~20 bytes
        cache.insert_find_keys(prefix2.clone(), &keys2);
        cache.check_coherence();

        // Both entries should be present at this point
        assert!(cache.query_find_keys(&prefix1).is_some());
        assert!(cache.query_find_keys(&prefix2).is_some());

        // Insert third find_keys entry that should trigger trimming
        let prefix3 = vec![3];
        let keys3 = vec![vec![6; 10], vec![7; 10]]; // ~20 bytes
        cache.insert_find_keys(prefix3.clone(), &keys3);
        cache.check_coherence();

        // The cache should have trimmed to stay within max_cache_find_keys_size
        assert!(cache.total_find_keys_size <= cache.config.max_cache_find_keys_size);

        // The least recently used entry (prefix1) should have been evicted
        assert_eq!(cache.query_find_keys(&prefix1), None);

        // The more recent entries should still be present
        assert!(cache.query_find_keys(&prefix2).is_some());
        assert!(cache.query_find_keys(&prefix3).is_some());

        // Verify we have fewer find_keys entries than we inserted
        assert!(cache.find_keys_map.len() < 3);
    }

    #[test]
    fn test_find_keys_removal_from_map() {
        let mut cache = LruPrefixCache::new(
            StorageCacheConfig {
                max_cache_size: 100, // Small cache to trigger eviction
                max_value_entry_size: 50,
                max_find_keys_entry_size: 1000,
                max_find_key_values_entry_size: 2000,
                max_cache_entries: 3, // Very small to force eviction
                max_cache_value_size: 500,
                max_cache_find_keys_size: 500,
                max_cache_find_key_values_size: 500,
            },
            true,
        );

        let prefix1 = vec![1];
        let prefix2 = vec![2];
        let prefix3 = vec![3];
        let keys = vec![vec![1], vec![2]];

        // Insert find_keys entries to fill the cache
        cache.insert_find_keys(prefix1.clone(), &keys);
        cache.check_coherence();
        cache.insert_find_keys(prefix2.clone(), &keys);
        cache.check_coherence();

        // Verify both entries are present
        assert!(cache.find_keys_map.contains_key(&prefix1));
        assert!(cache.find_keys_map.contains_key(&prefix2));

        // Insert third entry that should trigger eviction and call remove_cache_key_from_map for FindKeys
        cache.insert_find_keys(prefix3.clone(), &keys);
        cache.check_coherence();

        // The first entry should have been evicted
        assert!(!cache.find_keys_map.contains_key(&prefix1));
        // The newer entries should still be present
        assert!(cache.find_keys_map.contains_key(&prefix2));
        assert!(cache.find_keys_map.contains_key(&prefix3));

        // Verify cache constraints are maintained
        assert!(cache.queue.len() <= cache.config.max_cache_entries);
    }

    #[test]
    fn test_find_key_values_removal_from_map() {
        let mut cache = LruPrefixCache::new(
            StorageCacheConfig {
                max_cache_size: 100, // Small cache to trigger eviction
                max_value_entry_size: 50,
                max_find_keys_entry_size: 1000,
                max_find_key_values_entry_size: 2000,
                max_cache_entries: 3, // Very small to force eviction
                max_cache_value_size: 500,
                max_cache_find_keys_size: 500,
                max_cache_find_key_values_size: 500,
            },
            true,
        );

        let prefix1 = vec![1];
        let prefix2 = vec![2];
        let prefix3 = vec![3];
        let key_values = vec![(vec![1], vec![10]), (vec![2], vec![20])];

        // Insert find_key_values entries to fill the cache
        cache.insert_find_key_values(prefix1.clone(), &key_values);
        cache.check_coherence();
        cache.insert_find_key_values(prefix2.clone(), &key_values);
        cache.check_coherence();

        // Verify both entries are present
        assert!(cache.find_key_values_map.contains_key(&prefix1));
        assert!(cache.find_key_values_map.contains_key(&prefix2));

        // Insert third entry that should trigger eviction and call remove_cache_key_from_map for FindKeyValues
        cache.insert_find_key_values(prefix3.clone(), &key_values);
        cache.check_coherence();

        // The first entry should have been evicted
        assert!(!cache.find_key_values_map.contains_key(&prefix1));
        // The newer entries should still be present
        assert!(cache.find_key_values_map.contains_key(&prefix2));
        assert!(cache.find_key_values_map.contains_key(&prefix3));

        // Verify cache constraints are maintained
        assert!(cache.queue.len() <= cache.config.max_cache_entries);
    }

    #[test]
    fn test_contains_key_failing_without_exclusive_access() {
        let mut cache = create_test_cache(false); // has_exclusive_access = false
        let key = vec![1, 2, 3];

        // With has_exclusive_access = false, query_contains_key should only check value_map
        // Since the key is not in value_map, it should return None
        let result = cache.query_contains_key(&key);
        assert_eq!(result, None);

        // Verify that only value_map is checked by inserting directly into value_map
        cache.insert_contains_key(&key, true);
        cache.check_coherence();

        // Now it should return Some(true) because it's found in value_map
        let result = cache.query_contains_key(&key);
        assert_eq!(result, Some(true));
    }

    #[test]
    fn test_contains_key_found_in_find_key_values() {
        let mut cache = create_test_cache(true); // has_exclusive_access = true
        let prefix = vec![1, 2];
        let key = vec![1, 2, 3, 4];
        let key_values = vec![(vec![3, 4], vec![100]), (vec![5, 6], vec![200])];

        // Insert a FindKeyValues entry that contains the key we'll query
        cache.insert_find_key_values(prefix.clone(), &key_values);
        cache.check_coherence();

        // Query for the key - this should find it in FindKeyValues (lines 852-854)
        let result = cache.query_contains_key(&key);
        assert_eq!(result, Some(true));

        // Test with a key that doesn't exist in the FindKeyValues
        let non_existent_key = vec![1, 2, 7, 8];
        let result = cache.query_contains_key(&non_existent_key);
        assert_eq!(result, Some(false)); // Found the prefix but key doesn't exist in the entry

        // Test with a key that doesn't match any prefix
        let unmatched_key = vec![9, 9, 9];
        let result = cache.query_contains_key(&unmatched_key);
        assert_eq!(result, None); // No matching prefix found
    }

    #[test]
    fn test_find_keys_removal_when_inserting_broader_find_keys() {
        let mut cache = create_test_cache(true);

        // Insert another FindKeys entry with a more specific prefix
        let specific_prefix = vec![1, 2, 3, 4];
        let keys2 = vec![vec![6], vec![7]];
        cache.insert_find_keys(specific_prefix.clone(), &keys2);
        cache.check_coherence();

        // Insert a FindKeys entry with a specific prefix
        let narrow_prefix = vec![1, 2, 3];
        let keys1 = vec![vec![4, 6], vec![4, 7]];
        cache.insert_find_keys(narrow_prefix.clone(), &keys1);
        cache.check_coherence();
    }

    #[test]
    fn test_overlapping_find_key_values_removes_find_keys_and_find_key_values() {
        let mut cache = create_test_cache(true);

        // Insert a FindKeys entry
        let find_keys_prefix = vec![1, 2, 3];
        let keys = vec![vec![4, 6], vec![4, 7], vec![5]];
        cache.insert_find_keys(find_keys_prefix.clone(), &keys);
        cache.check_coherence();

        // Insert a FindKeyValues entry with a different but overlapping prefix
        let find_key_values_prefix = vec![1, 2, 3, 4];
        let key_values1 = vec![(vec![6], vec![10]), (vec![7], vec![20])];
        cache.insert_find_key_values(find_key_values_prefix.clone(), &key_values1);
        cache.check_coherence();

        // Inserts a Value
        cache.put_key_value(&[1, 2, 8, 9], &[254]);
        cache.check_coherence();

        // Now insert a broader FindKeyValues entry that overlaps with both previous entries
        let broad_prefix = vec![1, 2];
        let key_values2 = vec![
            (vec![3, 4, 6], vec![10]),
            (vec![3, 4, 7], vec![20]),
            (vec![3, 5], vec![34]),
            (vec![8, 9], vec![254]),
        ];
        cache.insert_find_key_values(broad_prefix.clone(), &key_values2);
        cache.check_coherence();

        // The broader FindKeyValues should have removed the overlapping FindKeys entry.
        assert!(!cache.find_keys_map.contains_key(&find_keys_prefix));

        // The broader FindKeyValues should have removed the overlapping FindKeyValues entry.
        assert!(!cache
            .find_key_values_map
            .contains_key(&find_key_values_prefix));

        // The new broad FindKeyValues entry should exist
        assert!(cache.find_key_values_map.contains_key(&broad_prefix));

        // Verify the new entry has the expected key-values
        let key_values = cache.query_find_key_values(&broad_prefix).unwrap();
        assert!(key_values.contains(&(vec![3, 4, 6], vec![10])));
        assert!(key_values.contains(&(vec![3, 4, 7], vec![20])));
        assert!(key_values.contains(&(vec![8, 9], vec![254])));

        // Test that we can still query for keys under the broad prefix
        let query_result = cache.query_contains_key(&[1, 2, 3, 1]);
        assert_eq!(query_result, Some(false)); // Should find it in the new FindKeyValues entry
    }

    #[test]
    fn test_delete_prefix_removes_entire_find_keys_entry() {
        let mut cache = create_test_cache(true); // has_exclusive_access = true

        // Insert a FindKeys entry
        let prefix = vec![1, 2, 3];
        let keys = vec![vec![4], vec![5], vec![6]];
        cache.insert_find_keys(prefix.clone(), &keys);
        cache.check_coherence();

        // Verify the FindKeys entry exists
        assert!(cache.find_keys_map.contains_key(&prefix));
        let result = cache.query_find_keys(&prefix);
        assert!(result.is_some());

        // Delete a prefix that covers the entire FindKeys entry
        let delete_prefix = vec![1, 2]; // This covers [1, 2, 3]
        cache.delete_prefix(&delete_prefix);
        cache.check_coherence();

        // Verify the entry is no longer queryable
        let result = cache.query_find_keys(&prefix);
        assert_eq!(result, Some(vec![]));

        // Verify the queue no longer contains the FindKeys entry
        for (cache_key, _) in &cache.queue {
            assert!(!matches!(cache_key, CacheKey::FindKeys(key) if key == &prefix));
        }
    }

    #[test]
    fn test_delete_prefix_removes_keys_within_find_keys_entry() {
        let mut cache = create_test_cache(true); // has_exclusive_access = true

        // Insert a FindKeys entry with a broader prefix
        let find_keys_prefix = vec![1];
        let keys = vec![vec![2, 3, 4], vec![2, 3, 5], vec![2, 4, 6], vec![3, 7, 8]];
        cache.insert_find_keys(find_keys_prefix.clone(), &keys);
        cache.check_coherence();

        // Verify the FindKeys entry exists and contains all keys
        assert!(cache.find_keys_map.contains_key(&find_keys_prefix));
        let initial_keys = cache.query_find_keys(&find_keys_prefix).unwrap();
        assert!(initial_keys.contains(&vec![2, 3, 4]));
        assert!(initial_keys.contains(&vec![2, 3, 5]));
        assert!(initial_keys.contains(&vec![2, 4, 6]));
        assert!(initial_keys.contains(&vec![3, 7, 8]));

        let initial_size = cache.total_find_keys_size;

        // Delete a prefix that only affects some keys within the FindKeys entry
        let delete_prefix = vec![1, 2, 3]; // This should only affect keys [2,3,4] and [2,3,5]
        cache.delete_prefix(&delete_prefix);
        cache.check_coherence();

        // The FindKeys entry should still exist but with fewer keys
        assert!(cache.find_keys_map.contains_key(&find_keys_prefix));

        // Verify that only the keys with prefix [2,3] were removed
        let remaining_keys = cache.query_find_keys(&find_keys_prefix).unwrap();
        assert!(!remaining_keys.contains(&vec![2, 3, 4])); // Should be removed
        assert!(!remaining_keys.contains(&vec![2, 3, 5])); // Should be removed
        assert!(remaining_keys.contains(&vec![2, 4, 6])); // Should remain
        assert!(remaining_keys.contains(&vec![3, 7, 8])); // Should remain

        // The cache size should have decreased due to the removed keys
        assert!(cache.total_find_keys_size < initial_size);

        // Test another delete that removes more keys
        let delete_prefix2 = vec![1, 2]; // This should affect key [2,4,6]
        cache.delete_prefix(&delete_prefix2);
        cache.check_coherence();

        // Check the remaining keys
        let final_keys = cache.query_find_keys(&find_keys_prefix).unwrap();
        assert!(!final_keys.contains(&vec![2, 4, 6])); // Should now be removed
        assert!(final_keys.contains(&vec![3, 7, 8])); // Should still remain
    }

    #[test]
    fn test_put_key_value_without_find_key_values_match() {
        let mut cache = create_test_cache(false); // has_exclusive_access = true
        let key = vec![1, 2, 3, 4];
        let value = vec![100, 200];

        // The value should be inserted without accessing the FindKeys / FindKeyValues.
        cache.put_key_value(&key, &value);
        cache.check_coherence();

        // Testing the reading of value.
        assert_eq!(cache.query_read_value(&key), Some(Some(value.clone())));
        assert!(cache.value_map.contains_key(&key));
    }

    #[test]
    fn test_delete_key_without_exclusive_access() {
        let mut cache = create_test_cache(false); // has_exclusive_access = false
        let key = vec![1, 2, 3];
        let value = vec![42, 43, 44];

        // Insert a value first
        cache.insert_read_value(&key, &Some(value.clone()));
        cache.check_coherence();

        // Verify the value is cached
        assert_eq!(cache.query_read_value(&key), Some(Some(value)));
        assert!(cache.value_map.contains_key(&key));

        // Delete the key without exclusive access. So, do not use the
        // FindKeys/FindKeyValues.
        cache.delete_key(&key);
        cache.check_coherence();

        // The entry should be removed from the cache.
        assert!(!cache.value_map.contains_key(&key));
        assert_eq!(cache.query_read_value(&key), None);

        // Verify the queue no longer contains the entry
        for (cache_key, _) in &cache.queue {
            assert!(!matches!(cache_key, CacheKey::Value(k) if k == &key));
        }

        // Test deleting a key that doesn't exist
        let non_existent_key = vec![9, 9, 9];
        cache.delete_key(&non_existent_key); // Should not crash
        cache.check_coherence();
    }

    #[test]
    fn test_line_502_insert_then_delete_without_exclusive_access() {
        let mut cache = create_test_cache(false); // has_exclusive_access = false
        let key = vec![1, 2, 3, 4];
        let value = vec![100, 200, 201];

        // First insert a value entry
        cache.insert_read_value(&key, &Some(value.clone()));
        cache.check_coherence();

        // Verify the value is cached
        assert_eq!(cache.query_read_value(&key), Some(Some(value)));
        assert!(cache.value_map.contains_key(&key));

        // Now insert a DoesNotExist entry (simulating a delete) without exclusive access
        // This should trigger line 502: removal due to !has_exclusive_access
        cache.insert_read_value(&key, &None);
        cache.check_coherence();

        // The entry should be completely removed from the cache (line 502)
        assert!(!cache.value_map.contains_key(&key));
        assert_eq!(cache.query_read_value(&key), None);

        // Verify the cache key is removed from the queue
        let queue_contains_key = cache
            .queue
            .iter()
            .any(|(cache_key, _)| matches!(cache_key, CacheKey::Value(k) if k == &key));
        assert!(!queue_contains_key);
    }

    #[test]
    fn test_does_not_exist_removal_during_insert_find_keys() {
        let mut cache = create_test_cache(true); // has_exclusive_access = true
        let prefix = vec![1, 2];
        let key1 = vec![1, 2, 3];
        let key2 = vec![1, 2, 4];
        let key3 = vec![1, 2, 5];

        // Insert DoesNotExist entries for keys that will be covered by the FindKeys
        cache.insert_read_value(&key1, &None); // Creates DoesNotExist entry
        cache.insert_read_value(&key2, &None); // Creates DoesNotExist entry
        cache.insert_read_value(&key3, &Some(vec![100])); // Creates Value entry (should not be removed)
        cache.check_coherence();

        // Verify the DoesNotExist entries are cached
        assert_eq!(cache.query_read_value(&key1), Some(None));
        assert_eq!(cache.query_read_value(&key2), Some(None));
        assert_eq!(cache.query_read_value(&key3), Some(Some(vec![100])));
        assert!(cache.value_map.contains_key(&key1));
        assert!(cache.value_map.contains_key(&key2));
        assert!(cache.value_map.contains_key(&key3));

        // Insert a FindKeys entry that covers the prefix - this should trigger line 640
        // Line 640 filters DoesNotExist entries for removal, but not Value entries
        let find_keys_result = vec![key1.clone(), key2.clone(), key3.clone()];
        cache.insert_find_keys(prefix.clone(), &find_keys_result);
        cache.check_coherence();

        // After insert_find_keys, the DoesNotExist entries should be removed (line 640)
        // but the Value entry should remain (line 642 returns None for Value entries)
        assert_eq!(cache.query_read_value(&key1), None); // DoesNotExist removed
        assert_eq!(cache.query_read_value(&key2), None); // DoesNotExist removed
        assert_eq!(cache.query_read_value(&key3), Some(Some(vec![100]))); // Value entry kept
        assert!(!cache.value_map.contains_key(&key1));
        assert!(!cache.value_map.contains_key(&key2));
        assert!(cache.value_map.contains_key(&key3)); // Value entry remains

        // Verify the FindKeys entry was created
        assert_eq!(cache.query_find_keys(&prefix), Some(find_keys_result));
    }

    #[test]
    fn test_trim_value_cache_complete_iteration() {
        let mut cache = create_test_cache(true);

        // Insert some value entries to populate the cache
        let key1 = vec![1, 2, 3];
        let key2 = vec![4, 5, 6];
        let value1 = vec![100, 200];
        let value2 = vec![203, 177];

        cache.insert_read_value(&key1, &Some(value1));
        cache.insert_read_value(&key2, &Some(value2));
        cache.check_coherence();

        // Set cache size to 0 to force trimming but ensure queue is not empty
        cache.config.max_cache_value_size = 0;

        // Call trim_value_cache - this should iterate through entire queue
        // and hit line 411 (break when iter.next() returns None)
        cache.trim_value_cache();
        cache.check_coherence();

        // All value entries should be removed since max_cache_value_size = 0
        assert!(!cache.value_map.contains_key(&key1));
        assert!(!cache.value_map.contains_key(&key2));
    }

    #[test]
    fn test_trim_find_keys_cache_complete_iteration() {
        let mut cache = create_test_cache(true);

        // Insert some FindKeys entries to populate the cache
        let prefix1 = vec![1, 2];
        let prefix2 = vec![3, 4];
        let keys1 = vec![vec![1, 2, 3], vec![1, 2, 4]];
        let keys2 = vec![vec![3, 4, 5], vec![3, 4, 6]];

        cache.insert_find_keys(prefix1, &keys1);
        cache.insert_find_keys(prefix2, &keys2);
        cache.check_coherence();

        // Set cache size to 0 to force trimming but ensure queue is not empty
        cache.config.max_cache_find_keys_size = 0;

        // Call trim_find_keys_cache - this should iterate through entire queue
        // and hit line 436 (break when iter.next() returns None)
        cache.trim_find_keys_cache();
        cache.check_coherence();

        // All FindKeys entries should be removed since max_cache_find_keys_size = 0
        assert!(cache.find_keys_map.is_empty());
    }

    #[test]
    fn test_trim_find_key_values_cache_complete_iteration() {
        let mut cache = create_test_cache(true);

        // Insert some FindKeyValues entries to populate the cache
        let prefix1 = vec![1, 2];
        let prefix2 = vec![3, 4];
        let key_values1 = vec![(vec![1, 2, 3], vec![100]), (vec![1, 2, 4], vec![200])];
        let key_values2 = vec![(vec![3, 4, 5], vec![203]), (vec![3, 4, 6], vec![177])];

        cache.insert_find_key_values(prefix1, &key_values1);
        cache.insert_find_key_values(prefix2, &key_values2);
        cache.check_coherence();

        // Set cache size to 0 to force trimming but ensure queue is not empty
        cache.config.max_cache_find_key_values_size = 0;

        // Call trim_find_key_values_cache - this should iterate through entire queue
        // and hit line 461 (break when iter.next() returns None)
        cache.trim_find_key_values_cache();
        cache.check_coherence();

        // All FindKeyValues entries should be removed since max_cache_find_key_values_size = 0
        assert!(cache.find_key_values_map.is_empty());
    }

    #[test]
    fn test_trim_cache_breaks_on_empty_queue() {
        let mut cache = LruPrefixCache::new(
            StorageCacheConfig {
                max_cache_size: 10000,       // to make irrelevant
                max_value_entry_size: 10000, // to make irrelevant
                max_find_keys_entry_size: 10000,
                max_find_key_values_entry_size: 10000,
                max_cache_entries: 10,
                max_cache_value_size: 500,
                max_cache_find_keys_size: 500,
                max_cache_find_key_values_size: 500,
            },
            true,
        );

        // Insert some entries to populate the cache
        for i in 0..10 {
            let key = vec![1, 2, i];
            let value = vec![100 + i];
            cache.insert_read_value(&key, &Some(value));
            cache.check_coherence();
        }

        // Manually manipulate total_size so that the cache gets emptied.
        cache.config.max_cache_size = 0;

        // Inserting something that triggers the whole cache being emptied.
        let key = vec![1, 2];
        let value = vec![100];
        cache.insert_read_value(&key, &Some(value));
        cache.check_coherence();
    }
}
