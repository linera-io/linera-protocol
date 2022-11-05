// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    hash::HashingContext,
    views::{Context, ViewError},
    common::{WriteOperation, Batch},
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};
use thiserror::Error;
use tokio::sync::{OwnedMutexGuard, RwLock};

/// The data is serialized in memory just like for rocksdb / dynamodb
/// The analogue of the database is the BTreeMap
pub type MemoryStoreMap = BTreeMap<Vec<u8>, Vec<u8>>;

/// A context that stores all values in memory.
#[derive(Clone, Debug)]
pub struct MemoryContext<E> {
    map: Arc<RwLock<OwnedMutexGuard<MemoryStoreMap>>>,
    base_key: Vec<u8>,
    extra: E,
}

impl<E> MemoryContext<E> {
    pub fn new(guard: OwnedMutexGuard<MemoryStoreMap>, extra: E) -> Self {
        Self {
            map: Arc::new(RwLock::new(guard)),
            base_key: Vec::new(),
            extra,
        }
    }
}

#[async_trait]
impl<E> Context for MemoryContext<E>
where
    E: Clone + Send + Sync,
{
    type Extra = E;
    type Error = MemoryContextError;

    fn extra(&self) -> &E {
        &self.extra
    }

    fn base_key(&self) -> Vec<u8> {
        self.base_key.clone()
    }

    fn derive_key<I: Serialize>(&self, index: &I) -> Result<Vec<u8>,MemoryContextError> {
        let mut key = self.base_key.clone();
        bcs::serialize_into(&mut key, index)?;
        assert!(
            key.len() > self.base_key.len(),
            "Empty indices are not allowed"
        );
        Ok(key)
    }

    async fn read_key<V: DeserializeOwned>(
        &mut self,
        key: &[u8],
    ) -> Result<Option<V>, MemoryContextError> {
        let map = self.map.read().await;
        match map.get(key) {
            None => Ok(None),
            Some(bytes) => Ok(Some(bcs::from_bytes(bytes)?)),
        }
    }

    async fn find_keys_with_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, MemoryContextError> {
        let map = self.map.read().await;
        let mut vals = Vec::new();
        for key in map.keys() {
            if key.starts_with(key_prefix) {
                vals.push(key.clone())
            }
        }
        Ok(vals)
    }

    async fn get_sub_keys<Key>(&mut self, key_prefix: &[u8]) -> Result<Vec<Key>, MemoryContextError>
    where
        Key: DeserializeOwned + Send,
    {
        let map = self.map.read().await;
        let mut keys = Vec::new();
        let len = key_prefix.len();
        for key in map.keys() {
            if key.starts_with(key_prefix) {
                keys.push(bcs::from_bytes(&key[len..])?);
            }
        }
        Ok(keys)
    }

    async fn run_with_batch<F>(&self, builder: F) -> Result<(), ViewError>
    where
        F: FnOnce(&mut Batch) -> futures::future::BoxFuture<Result<(), ViewError>>
            + Send
            + Sync,
    {
        let mut batch = Batch::default();
        builder(&mut batch).await?;
        self.write_batch(batch).await
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), ViewError> {
        let mut map = self.map.write().await;
        for ent in batch.operations {
            match ent {
                WriteOperation::Put { key, value } => map.insert(key, value),
                WriteOperation::Delete { key } => map.remove(&key),
            };
        }
        Ok(())
    }

    fn clone_self(&self, base_key: Vec<u8>) -> Self {
        Self {
            map: self.map.clone(),
            base_key,
            extra: self.extra.clone(),
        }
    }
}

impl<E> HashingContext for MemoryContext<E>
where
    E: Clone + Send + Sync,
{
    type Hasher = sha2::Sha512;
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
