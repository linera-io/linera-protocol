// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    hash::HashingContext,
    views::{
        CollectionOperations, Context, LogOperations, MapOperations, QueueOperations,
        RegisterOperations, ScopedOperations, ViewError,
    },
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{cmp::Eq, collections::BTreeMap, fmt::Debug, ops::Range, sync::Arc};
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

pub enum WriteOperation {
    Delete { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
}

#[derive(Default)]
pub struct Batch(Vec<WriteOperation>);

trait WriteOperations {
    fn write_key<V: Serialize>(
        &mut self,
        key: Vec<u8>,
        value: &V,
    ) -> Result<(), MemoryContextError>;

    fn delete_key(&mut self, key: Vec<u8>);
}

impl WriteOperations for Batch {
    fn write_key<V: Serialize>(
        &mut self,
        key: Vec<u8>,
        value: &V,
    ) -> Result<(), MemoryContextError> {
        let bytes = bcs::to_bytes(value)?;
        self.0.push(WriteOperation::Put { key, value: bytes });
        Ok(())
    }

    fn delete_key(&mut self, key: Vec<u8>) {
        self.0.push(WriteOperation::Delete { key });
    }
}

impl<E> MemoryContext<E> {
    pub fn new(guard: OwnedMutexGuard<MemoryStoreMap>, extra: E) -> Self {
        Self {
            map: Arc::new(RwLock::new(guard)),
            base_key: Vec::new(),
            extra,
        }
    }

    fn derive_key<I: serde::Serialize>(&self, index: &I) -> Vec<u8> {
        let mut key = self.base_key.clone();
        bcs::serialize_into(&mut key, index).expect("serialization should not fail");
        key
    }

    async fn read_key<V: DeserializeOwned>(
        &mut self,
        key: &Vec<u8>,
    ) -> Result<Option<V>, MemoryContextError> {
        let map = self.map.read().await;
        match map.get(key) {
            None => Ok(None),
            Some(bytes) => Ok(Some(bcs::from_bytes(bytes)?)),
        }
    }

    async fn find_keys_with_prefix(
        &mut self,
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
}

#[async_trait]
impl<E> Context for MemoryContext<E>
where
    E: Clone + Send + Sync,
{
    type Batch = Batch;
    type Extra = E;
    type Error = MemoryContextError;

    fn extra(&self) -> &E {
        &self.extra
    }

    async fn run_with_batch<F>(&self, builder: F) -> Result<(), ViewError>
    where
        F: FnOnce(&mut Self::Batch) -> futures::future::BoxFuture<Result<(), ViewError>>
            + Send
            + Sync,
    {
        let mut batch = Batch(Vec::new());
        builder(&mut batch).await?;
        self.write_batch(batch).await
    }

    fn create_batch(&self) -> Self::Batch {
        Batch(Vec::new())
    }

    async fn write_batch(&self, batch: Self::Batch) -> Result<(), ViewError> {
        let mut map = self.map.write().await;
        for ent in batch.0 {
            match ent {
                WriteOperation::Put { key, value } => map.insert(key, value),
                WriteOperation::Delete { key } => map.remove(&key),
            };
        }
        Ok(())
    }
}

#[async_trait]
impl<E> ScopedOperations for MemoryContext<E>
where
    E: Clone + Send + Sync,
{
    fn clone_with_scope(&self, index: u64) -> Self {
        Self {
            map: self.map.clone(),
            base_key: self.derive_key(&index),
            extra: self.extra.clone(),
        }
    }
}

#[async_trait]
impl<E, T> RegisterOperations<T> for MemoryContext<E>
where
    T: Serialize + DeserializeOwned + Default + Clone + Send + Sync + 'static,
    E: Clone + Send + Sync,
{
    async fn get(&mut self) -> Result<T, MemoryContextError> {
        let value = self
            .read_key(&self.base_key.clone())
            .await?
            .unwrap_or_default();
        Ok(value)
    }

    fn set(&mut self, batch: &mut Self::Batch, value: &T) -> Result<(), MemoryContextError> {
        batch.write_key(self.base_key.clone(), value)
    }

    fn delete(&mut self, batch: &mut Self::Batch) -> Result<(), MemoryContextError> {
        batch.delete_key(self.base_key.clone());
        Ok(())
    }
}

#[async_trait]
impl<E, T> LogOperations<T> for MemoryContext<E>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    E: Clone + Send + Sync,
{
    async fn count(&mut self) -> Result<usize, MemoryContextError> {
        let count = self
            .read_key(&self.base_key.clone())
            .await?
            .unwrap_or_default();
        Ok(count)
    }

    async fn get(&mut self, index: usize) -> Result<Option<T>, MemoryContextError> {
        self.read_key(&self.derive_key(&index)).await
    }

    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, MemoryContextError> {
        let mut items = Vec::with_capacity(range.len());
        for index in range {
            let item = match self.read_key(&self.derive_key(&index)).await? {
                Some(item) => item,
                None => return Ok(items),
            };
            items.push(item);
        }
        Ok(items)
    }

    fn append(
        &mut self,
        stored_count: usize,
        batch: &mut Self::Batch,
        values: Vec<T>,
    ) -> Result<(), MemoryContextError> {
        if values.is_empty() {
            return Ok(());
        }
        let mut count = stored_count;
        for value in values {
            batch.write_key(self.derive_key(&count), &value)?;
            count += 1;
        }
        batch.write_key(self.base_key.clone(), &count)?;
        Ok(())
    }

    fn delete(
        &mut self,
        stored_count: usize,
        batch: &mut Self::Batch,
    ) -> Result<(), MemoryContextError> {
        batch.delete_key(self.base_key.clone());
        for index in 0..stored_count {
            batch.delete_key(self.derive_key(&index));
        }
        Ok(())
    }
}

#[async_trait]
impl<E, T> QueueOperations<T> for MemoryContext<E>
where
    T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: Clone + Send + Sync,
{
    async fn indices(&mut self) -> Result<Range<usize>, Self::Error> {
        let range = self
            .read_key(&self.base_key.clone())
            .await?
            .unwrap_or_default();
        Ok(range)
    }

    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error> {
        Ok(self.read_key(&self.derive_key(&index)).await?)
    }

    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, Self::Error> {
        let mut values = Vec::new();
        for index in range {
            match self.read_key(&self.derive_key(&index)).await? {
                None => return Ok(values),
                Some(value) => values.push(value),
            }
        }
        Ok(values)
    }

    fn delete_front(
        &mut self,
        stored_indices: &mut Range<usize>,
        batch: &mut Self::Batch,
        count: usize,
    ) -> Result<(), Self::Error> {
        let deletion_range = stored_indices.clone().take(count);
        stored_indices.start += count;
        batch.write_key(self.base_key.clone(), &stored_indices)?;
        for index in deletion_range {
            batch.delete_key(self.derive_key(&index));
        }
        Ok(())
    }

    fn append_back(
        &mut self,
        stored_indices: &mut Range<usize>,
        batch: &mut Self::Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error> {
        for value in values {
            batch.write_key(self.derive_key(&stored_indices.end), &value)?;
            stored_indices.end += 1;
        }
        batch.write_key(self.base_key.clone(), &stored_indices)?;
        Ok(())
    }

    fn delete(
        &mut self,
        stored_indices: Range<usize>,
        batch: &mut Self::Batch,
    ) -> Result<(), MemoryContextError> {
        batch.delete_key(self.base_key.clone());
        for index in stored_indices {
            batch.delete_key(self.derive_key(&index));
        }
        Ok(())
    }
}

#[async_trait]
impl<E, I, V> MapOperations<I, V> for MemoryContext<E>
where
    I: Eq + Ord + Send + Sync + Clone + Serialize + DeserializeOwned + 'static,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: Clone + Send + Sync,
{
    async fn get(&mut self, index: &I) -> Result<Option<V>, MemoryContextError> {
        Ok(self.read_key(&self.derive_key(index)).await?)
    }

    fn insert(
        &mut self,
        batch: &mut Self::Batch,
        index: I,
        value: V,
    ) -> Result<(), MemoryContextError> {
        batch.write_key(self.derive_key(&index), &value)?;
        Ok(())
    }

    fn remove(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), MemoryContextError> {
        batch.delete_key(self.derive_key(&index));
        Ok(())
    }

    async fn indices(&mut self) -> Result<Vec<I>, MemoryContextError> {
        let len = self.base_key.len();
        let mut keys = Vec::new();
        for key in self.find_keys_with_prefix(&self.base_key.clone()).await? {
            keys.push(bcs::from_bytes(&key[len..])?);
        }
        Ok(keys)
    }

    #[allow(clippy::unit_arg)]
    async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), MemoryContextError>
    where
        F: FnMut(I) + Send,
    {
        let len = self.base_key.len();
        for key in self.find_keys_with_prefix(&self.base_key.clone()).await? {
            let key = bcs::from_bytes(&key[len..])?;
            f(key);
        }
        Ok(())
    }

    async fn delete(&mut self, batch: &mut Self::Batch) -> Result<(), MemoryContextError> {
        for key in self.find_keys_with_prefix(&self.base_key.clone()).await? {
            batch.delete_key(key);
        }
        Ok(())
    }
}

#[derive(Serialize)]
enum CollectionKey<I> {
    Index(I),
    Subview(I),
}

#[async_trait]
impl<E: Clone, I> CollectionOperations<I> for MemoryContext<E>
where
    I: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + Ord + Clone + 'static,
    E: Clone + Send + Sync,
{
    fn clone_with_scope(&self, index: &I) -> Self {
        Self {
            map: self.map.clone(),
            base_key: self.derive_key(&CollectionKey::Subview(index)),
            extra: self.extra.clone(),
        }
    }

    fn add_index(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error> {
        batch.write_key(self.derive_key(&CollectionKey::Index(index)), &())?;
        Ok(())
    }

    fn remove_index(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error> {
        batch.delete_key(self.derive_key(&CollectionKey::Index(index)));
        Ok(())
    }

    async fn indices(&mut self) -> Result<Vec<I>, Self::Error> {
        let base = self.derive_key(&CollectionKey::Index(()));
        let len = base.len();
        let mut keys = Vec::new();
        for bytes in self.find_keys_with_prefix(&base).await? {
            match bcs::from_bytes(&bytes[len..]) {
                Ok(key) => {
                    keys.push(key);
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        Ok(keys)
    }

    #[allow(clippy::unit_arg)]
    async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), Self::Error>
    where
        F: FnMut(I) + Send,
    {
        let base = self.derive_key(&CollectionKey::Index(()));
        let len = base.len();
        for bytes in self.find_keys_with_prefix(&base).await? {
            match bcs::from_bytes(&bytes[len..]) {
                Ok(key) => {
                    f(key);
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        Ok(())
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
