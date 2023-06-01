// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// The standard cache size used for tests.
pub const TEST_CACHE_SIZE: usize = 1000;

use crate::{
    batch::{Batch, WriteOperation},
    common::{get_interval, KeyValueStoreClient},
};
use async_lock::Mutex;
use async_trait::async_trait;
use linked_hash_map::LinkedHashMap;
use std::{
    collections::{btree_map, hash_map::RandomState, BTreeMap},
    sync::Arc,
};

#[cfg(any(test, feature = "test"))]
use {
    crate::common::ContextFromDb,
    crate::memory::{MemoryClient, MemoryStoreMap},
    crate::views::ViewError,
    async_lock::{MutexGuardArc, RwLock},
};

/// The LruPrefixCache store the data for a simple read_keys queries
/// It is inspired from lru-cache crate.
///
/// We cannot apply this crate directly because the batch operation
/// need to update the cache. In the case of DeletePrefix we have to
/// handle the keys by prefix. And so we need to have a BTreeMap to
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
                    let Some(value) = self.queue.pop_front() else { unreachable!() };
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

/// We take a client, a maximum size and build a LRU based system.
#[derive(Clone)]
pub struct LruCachingKeyValueClient<K> {
    client: K,
    lru_read_keys: Option<Arc<Mutex<LruPrefixCache>>>,
}

#[async_trait]
impl<K> KeyValueStoreClient for LruCachingKeyValueClient<K>
where
    K: KeyValueStoreClient + Send + Sync,
{
    const MAX_CONNECTIONS: usize = K::MAX_CONNECTIONS;
    type Error = K::Error;
    type Keys = K::Keys;
    type KeyValues = K::KeyValues;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        match &self.lru_read_keys {
            None => {
                return self.client.read_key_bytes(key).await;
            }
            Some(lru_read_keys) => {
                // First inquiring in the read_key_bytes LRU
                let lru_read_keys_container = lru_read_keys.lock().await;
                if let Some(value) = lru_read_keys_container.query(key) {
                    return Ok(value.clone());
                }
                drop(lru_read_keys_container);
                let value = self.client.read_key_bytes(key).await?;
                let mut lru_read_keys = lru_read_keys.lock().await;
                lru_read_keys.insert(key.to_vec(), value.clone());
                Ok(value)
            }
        }
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        match &self.lru_read_keys {
            None => {
                return self.client.read_multi_key_bytes(keys).await;
            }
            Some(lru_read_keys) => {
                let mut result = Vec::with_capacity(keys.len());
                let mut cache_miss_indices = Vec::new();
                let mut miss_keys = Vec::new();
                let lru_read_keys_container = lru_read_keys.lock().await;
                for (i, key) in keys.into_iter().enumerate() {
                    if let Some(value) = lru_read_keys_container.query(&key) {
                        result.push(value.clone());
                    } else {
                        result.push(None);
                        cache_miss_indices.push(i);
                        miss_keys.push(key);
                    }
                }
                drop(lru_read_keys_container);
                let values = self.client.read_multi_key_bytes(miss_keys.clone()).await?;
                let mut lru_read_keys = lru_read_keys.lock().await;
                for (i, (key, value)) in cache_miss_indices
                    .into_iter()
                    .zip(miss_keys.into_iter().zip(values))
                {
                    lru_read_keys.insert(key, value.clone());
                    result[i] = value;
                }
                Ok(result)
            }
        }
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        self.client.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        self.client.find_key_values_by_prefix(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), Self::Error> {
        match &self.lru_read_keys {
            None => {
                return self.client.write_batch(batch, base_key).await;
            }
            Some(lru_read_keys) => {
                let mut lru_read_keys = lru_read_keys.lock().await;
                for operation in &batch.operations {
                    match operation {
                        WriteOperation::Put { key, value } => {
                            lru_read_keys.insert(key.to_vec(), Some(value.to_vec()));
                        }
                        WriteOperation::Delete { key } => {
                            lru_read_keys.insert(key.to_vec(), None);
                        }
                        WriteOperation::DeletePrefix { key_prefix } => {
                            lru_read_keys.delete_prefix(key_prefix);
                        }
                    }
                }
                drop(lru_read_keys);
                self.client.write_batch(batch, base_key).await
            }
        }
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), Self::Error> {
        self.client.clear_journal(base_key).await
    }
}

impl<K> LruCachingKeyValueClient<K>
where
    K: KeyValueStoreClient,
{
    /// Creates a new Key Value Store Client that implements LRU caching.
    pub fn new(client: K, max_size: usize) -> Self {
        if max_size == 0 {
            Self {
                client,
                lru_read_keys: None,
            }
        } else {
            let lru_read_keys = Some(Arc::new(Mutex::new(LruPrefixCache::new(max_size))));
            Self {
                client,
                lru_read_keys,
            }
        }
    }
}

/// A context that stores all values in memory.
#[cfg(any(test, feature = "test"))]
pub type LruCachingMemoryContext<E> = ContextFromDb<E, LruCachingKeyValueClient<MemoryClient>>;

#[cfg(any(test, feature = "test"))]
impl<E> LruCachingMemoryContext<E> {
    /// Creates a [`crate::key_value_store_view::KeyValueStoreMemoryContext`].
    pub async fn new(
        guard: MutexGuardArc<MemoryStoreMap>,
        base_key: Vec<u8>,
        extra: E,
        n: usize,
    ) -> Result<Self, ViewError> {
        let client = Arc::new(RwLock::new(guard));
        let lru_client = LruCachingKeyValueClient::new(client, n);
        Ok(Self {
            db: lru_client,
            base_key,
            extra,
        })
    }
}
