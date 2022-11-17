// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{
    common::{Batch, ContextFromDb, KeyValueOperations, SimpleKeyIterator, WriteOperation},
    views::ViewError,
};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::{
    collections::BTreeMap,
    fmt::Debug,
    ops::{
        Bound,
        Bound::{Excluded, Included, Unbounded},
    },
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::{OwnedMutexGuard, RwLock};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::Bound;

/// The data is serialized in memory just like for rocksdb / dynamodb
/// The analogue of the database is the BTreeMap
pub type MemoryStoreMap = BTreeMap<Vec<u8>, Vec<u8>>;

pub type MemoryContainer = Arc<RwLock<OwnedMutexGuard<MemoryStoreMap>>>;

/// A context that stores all values in memory.
pub type MemoryContext<E> = ContextFromDb<E, MemoryContainer>;

/// When wanting to find the entries in a BTreeMap with a specific prefix,
/// one option is to iterate over all keys. Another is to select an interval
/// that represents exactly the keys having that prefix. Which fortunately
/// is possible with the way the comparison operators for vectors is built.
fn get_interval(key_prefix: Vec<u8>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let len = key_prefix.len();
    for i in (0..len).rev() {
        let val = key_prefix[i];
        if val < 255 {
            let mut upper_bound = key_prefix.clone();
            upper_bound[i] += 1;
            for x in &mut upper_bound[i + 1..] {
                *x = 0;
            }
            return (Included(key_prefix), Excluded(upper_bound));
        }
    }
    (Included(key_prefix), Unbounded)
}

impl<E> MemoryContext<E> {
    pub fn new(guard: OwnedMutexGuard<MemoryStoreMap>, extra: E) -> Self {
        Self {
            db: Arc::new(RwLock::new(guard)),
            base_key: Vec::new(),
            extra,
        }
    }
}

#[async_trait]
impl KeyValueOperations for MemoryContainer {
    type Error = MemoryContextError;
    type KeyIterator = SimpleKeyIterator<MemoryContextError>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MemoryContextError> {
        let map = self.read().await;
        Ok(map.get(key).cloned())
    }

    async fn find_keys_with_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyIterator, MemoryContextError> {
        let map = self.read().await;
        let mut vals = Vec::new();
        for (key, _value) in map.range(get_interval(key_prefix.to_vec())) {
            vals.push(key.clone())
        }
        Ok(SimpleKeyIterator::new(vals))
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), MemoryContextError> {
        let mut map = self.write().await;
        for ent in batch.operations {
            match ent {
                WriteOperation::Put { key, value } => map.insert(key, value),
                WriteOperation::Delete { key } => map.remove(&key),
            };
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum MemoryContextError {
    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),
}

impl From<MemoryContextError> for ViewError {
    fn from(error: MemoryContextError) -> Self {
        Self::ContextError {
            backend: "memory".to_string(),
            error: error.to_string(),
        }
    }
}
