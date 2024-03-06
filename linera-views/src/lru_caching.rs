// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// The standard cache size used for tests.
pub const TEST_CACHE_SIZE: usize = 1000;

use crate::{
    batch::{Batch, WriteOperation},
    common::{get_interval, KeyValueStore, ReadableKeyValueStore, WritableKeyValueStore},
};
use async_lock::Mutex;
use async_trait::async_trait;
use linked_hash_map::LinkedHashMap;
use std::{
    collections::{btree_map, hash_map::RandomState, BTreeMap},
    sync::Arc,
};

#[cfg(with_metrics)]
use {
    linera_base::sync::Lazy,
    prometheus::{register_int_counter_vec, IntCounterVec},
};

#[cfg(any(test, feature = "test"))]
use {
    crate::common::{AdminKeyValueStore, CommonStoreConfig, ContextFromStore},
    crate::memory::{MemoryStore, MemoryStoreConfig, TEST_MEMORY_MAX_STREAM_QUERIES},
    crate::views::ViewError,
};

#[cfg(with_metrics)]
/// The total number of cache faults
static NUM_CACHE_FAULT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!("num_cache_fault", "Number of cache faults", &[])
        .expect("Counter creation should not fail")
});

#[cfg(with_metrics)]
/// The total number of cache successes
static NUM_CACHE_SUCCESS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!("num_cache_success", "Number of cache success", &[])
        .expect("Counter creation should not fail")
});

/// The `LruPrefixCache` stores the data for simple `read_values` queries
/// It is inspired by the crate `lru-cache`.
///
/// We cannot apply this crate directly because the batch operation
/// need to update the cache. In the case of `DeletePrefix` we have to
/// handle the keys by prefixes. And so we need to have a BTreeMap to
/// keep track of this.

/// The data structures
struct LruPrefixCache {
    map: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    queue: LinkedHashMap<Vec<u8>, (), RandomState>,
    max_cache_size: usize,
}

impl<'a> LruPrefixCache {
    /// Creates a LruPrefixCache.
    pub fn new(max_cache_size: usize) -> Self {
        Self {
            map: BTreeMap::new(),
            queue: LinkedHashMap::new(),
            max_cache_size,
        }
    }

    /// Inserts an entry into the cache.
    pub fn insert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        match self.map.entry(key.clone()) {
            btree_map::Entry::Occupied(mut entry) => {
                entry.insert(value);
                // Put it on first position for LRU
                self.queue.remove(&key);
                self.queue.insert(key, ());
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
                self.queue.insert(key, ());
                if self.queue.len() > self.max_cache_size {
                    let Some(value) = self.queue.pop_front() else {
                        unreachable!()
                    };
                    self.map.remove(&value.0);
                }
            }
        }
    }

    /// Marks cached keys that match the prefix as deleted. Importantly, this does not create new entries in the cache.
    pub fn delete_prefix(&mut self, key_prefix: &[u8]) {
        for (_, value) in self.map.range_mut(get_interval(key_prefix.to_vec())) {
            *value = None;
        }
    }

    /// Gets the entry from the key.
    pub fn query(&'a self, key: &'a [u8]) -> Option<&'a Option<Vec<u8>>> {
        self.map.get(key)
    }
}

/// We take a store, a maximum size and build a LRU-based system.
#[derive(Clone)]
pub struct LruCachingStore<K> {
    /// The inner store that is called by the LRU cache one
    pub store: K,
    lru_read_values: Option<Arc<Mutex<LruPrefixCache>>>,
}

#[async_trait]
impl<K> ReadableKeyValueStore<K::Error> for LruCachingStore<K>
where
    K: KeyValueStore + Send + Sync,
{
    // The LRU cache does not change the underlying store's size limits.
    const MAX_KEY_SIZE: usize = K::MAX_KEY_SIZE;
    type Keys = K::Keys;
    type KeyValues = K::KeyValues;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, K::Error> {
        match &self.lru_read_values {
            None => {
                return self.store.read_value_bytes(key).await;
            }
            Some(lru_read_values) => {
                // First inquiring in the read_value_bytes LRU
                let lru_read_values_container = lru_read_values.lock().await;
                if let Some(value) = lru_read_values_container.query(key) {
                    #[cfg(with_metrics)]
                    NUM_CACHE_SUCCESS.with_label_values(&[]).inc();
                    return Ok(value.clone());
                }
                drop(lru_read_values_container);
                #[cfg(with_metrics)]
                NUM_CACHE_FAULT.with_label_values(&[]).inc();
                let value = self.store.read_value_bytes(key).await?;
                let mut lru_read_values = lru_read_values.lock().await;
                lru_read_values.insert(key.to_vec(), value.clone());
                Ok(value)
            }
        }
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, K::Error> {
        if let Some(values) = &self.lru_read_values {
            let values = values.lock().await;
            if let Some(value) = values.query(key) {
                return Ok(value.is_some());
            }
        }
        self.store.contains_key(key).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, K::Error> {
        match &self.lru_read_values {
            None => {
                return self.store.read_multi_values_bytes(keys).await;
            }
            Some(lru_read_values) => {
                let mut result = Vec::with_capacity(keys.len());
                let mut cache_miss_indices = Vec::new();
                let mut miss_keys = Vec::new();
                let lru_read_values_container = lru_read_values.lock().await;
                for (i, key) in keys.into_iter().enumerate() {
                    if let Some(value) = lru_read_values_container.query(&key) {
                        #[cfg(with_metrics)]
                        NUM_CACHE_SUCCESS.with_label_values(&[]).inc();
                        result.push(value.clone());
                    } else {
                        #[cfg(with_metrics)]
                        NUM_CACHE_FAULT.with_label_values(&[]).inc();
                        result.push(None);
                        cache_miss_indices.push(i);
                        miss_keys.push(key);
                    }
                }
                drop(lru_read_values_container);
                let values = self
                    .store
                    .read_multi_values_bytes(miss_keys.clone())
                    .await?;
                let mut lru_read_values = lru_read_values.lock().await;
                for (i, (key, value)) in cache_miss_indices
                    .into_iter()
                    .zip(miss_keys.into_iter().zip(values))
                {
                    lru_read_values.insert(key, value.clone());
                    result[i] = value;
                }
                Ok(result)
            }
        }
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, K::Error> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, K::Error> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

#[async_trait]
impl<K> WritableKeyValueStore<K::Error> for LruCachingStore<K>
where
    K: KeyValueStore + Send + Sync,
{
    // The LRU cache does not change the underlying store's size limits.
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), K::Error> {
        match &self.lru_read_values {
            None => {
                return self.store.write_batch(batch, base_key).await;
            }
            Some(lru_read_values) => {
                let mut lru_read_values = lru_read_values.lock().await;
                for operation in &batch.operations {
                    match operation {
                        WriteOperation::Put { key, value } => {
                            lru_read_values.insert(key.to_vec(), Some(value.to_vec()));
                        }
                        WriteOperation::Delete { key } => {
                            lru_read_values.insert(key.to_vec(), None);
                        }
                        WriteOperation::DeletePrefix { key_prefix } => {
                            lru_read_values.delete_prefix(key_prefix);
                        }
                    }
                }
                drop(lru_read_values);
                self.store.write_batch(batch, base_key).await
            }
        }
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), K::Error> {
        self.store.clear_journal(base_key).await
    }
}

impl<K> KeyValueStore for LruCachingStore<K>
where
    K: KeyValueStore + Send + Sync,
{
    type Error = K::Error;
}

impl<K> LruCachingStore<K>
where
    K: KeyValueStore,
{
    /// Creates a new key-value store that provides LRU caching at top of the given store.
    pub fn new(store: K, max_size: usize) -> Self {
        if max_size == 0 {
            Self {
                store,
                lru_read_values: None,
            }
        } else {
            let lru_read_values = Some(Arc::new(Mutex::new(LruPrefixCache::new(max_size))));
            Self {
                store,
                lru_read_values,
            }
        }
    }
}

/// A context that stores all values in memory.
#[cfg(any(test, feature = "test"))]
pub type LruCachingMemoryContext<E> = ContextFromStore<E, LruCachingStore<MemoryStore>>;

#[cfg(any(test, feature = "test"))]
impl<E> LruCachingMemoryContext<E> {
    /// Creates a [`crate::key_value_store_view::KeyValueStoreMemoryContext`].
    pub async fn new(base_key: Vec<u8>, extra: E, n: usize) -> Result<Self, ViewError> {
        let common_config = CommonStoreConfig {
            max_concurrent_queries: None,
            max_stream_queries: TEST_MEMORY_MAX_STREAM_QUERIES,
            cache_size: 1000,
        };
        let config = MemoryStoreConfig { common_config };
        let namespace = "linera";
        let store = MemoryStore::connect(&config, namespace)
            .await
            .expect("store");
        let store = LruCachingStore::new(store, n);
        Ok(Self {
            store,
            base_key,
            extra,
        })
    }
}
