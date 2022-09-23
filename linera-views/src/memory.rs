// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    hash::HashingContext,
    views::{
        AppendOnlyLogOperations, CollectionOperations, Context, MapOperations, QueueOperations,
        RegisterOperations, ScopedOperations, ViewError,
    },
};
use async_trait::async_trait;
use serde::Serialize;
use std::{
    any::Any,
    cmp::Eq,
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt::Debug,
    ops::Range,
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::{OwnedMutexGuard, RwLock};

/// A context that stores all values in memory.
#[derive(Clone, Debug)]
pub struct MemoryContext {
    map: Arc<RwLock<OwnedMutexGuard<MemoryStoreMap>>>,
    base_key: Vec<u8>,
}

/// A Rust value stored in memory.
pub type MemoryStoreValue = Box<dyn Any + Send + Sync + 'static>;

/// A map of Rust values indexed by their keys.
pub type MemoryStoreMap = BTreeMap<Vec<u8>, MemoryStoreValue>;

impl MemoryContext {
    pub fn new(guard: OwnedMutexGuard<MemoryStoreMap>) -> Self {
        Self {
            map: Arc::new(RwLock::new(guard)),
            base_key: Vec::new(),
        }
    }

    fn derive_key<I: serde::Serialize>(&self, index: &I) -> Vec<u8> {
        let mut key = self.base_key.clone();
        bcs::serialize_into(&mut key, index).expect("serialization should not fail");
        key
    }

    async fn with_ref<F, T, V>(&self, f: F) -> V
    where
        F: Send + FnOnce(Option<&T>) -> V,
        T: 'static,
    {
        let map = self.map.read().await;
        match map.get(&self.base_key) {
            None => f(None),
            Some(blob) => {
                let value = blob
                    .downcast_ref::<T>()
                    .expect("downcast to &T should not fail");
                f(Some(value))
            }
        }
    }

    async fn with_mut<F, T, V>(&self, f: F) -> V
    where
        F: Send + FnOnce(&mut T) -> V,
        T: Default + Send + Sync + 'static,
    {
        let mut map = self.map.write().await;
        let blob = map
            .entry(self.base_key.clone())
            .or_insert_with(|| Box::new(T::default()));
        let value = blob
            .downcast_mut::<T>()
            .expect("downcast to &mut T should not fail");
        f(value)
    }

    async fn erase(&mut self) -> Result<(), MemoryViewError> {
        let mut map = self.map.write().await;
        map.remove(&self.base_key);
        Ok(())
    }
}

#[async_trait]
impl Context for MemoryContext {
    type Batch = ();
    type Error = MemoryViewError;

    async fn run_with_batch<F>(&self, builder: F) -> Result<(), Self::Error>
    where
        F: FnOnce(&mut Self::Batch) -> futures::future::BoxFuture<Result<(), Self::Error>>
            + Send
            + Sync,
    {
        builder(&mut ()).await
    }
}

#[async_trait]
impl ScopedOperations for MemoryContext {
    fn clone_with_scope(&self, index: u64) -> Self {
        Self {
            map: self.map.clone(),
            base_key: self.derive_key(&index),
        }
    }
}

#[async_trait]
impl<T> RegisterOperations<T> for MemoryContext
where
    T: Default + Clone + Send + Sync + 'static,
{
    async fn get(&mut self) -> Result<T, MemoryViewError> {
        Ok(self
            .with_ref(|value: Option<&T>| value.cloned().unwrap_or_default())
            .await)
    }

    async fn set(&mut self, _batch: &mut Self::Batch, value: T) -> Result<(), MemoryViewError> {
        let mut map = self.map.write().await;
        map.insert(self.base_key.clone(), Box::new(value));
        Ok(())
    }

    async fn delete(&mut self, _batch: &mut Self::Batch) -> Result<(), MemoryViewError> {
        self.erase().await
    }
}

#[async_trait]
impl<T> AppendOnlyLogOperations<T> for MemoryContext
where
    T: Clone + Send + Sync + 'static,
{
    async fn count(&mut self) -> Result<usize, MemoryViewError> {
        Ok(self
            .with_ref(|v: Option<&Vec<T>>| match v {
                None => 0,
                Some(x) => x.len(),
            })
            .await)
    }

    async fn get(&mut self, index: usize) -> Result<Option<T>, MemoryViewError> {
        Ok(self
            .with_ref(|v: Option<&Vec<T>>| v?.get(index).cloned())
            .await)
    }

    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, MemoryViewError> {
        Ok(self
            .with_ref(|v: Option<&Vec<T>>| match v {
                None => Vec::new(),
                Some(x) => x[range].to_vec(),
            })
            .await)
    }

    async fn append(
        &mut self,
        _stored_count: usize,
        _batch: &mut Self::Batch,
        mut values: Vec<T>,
    ) -> Result<(), MemoryViewError> {
        if !values.is_empty() {
            self.with_mut(|v: &mut Vec<T>| v.append(&mut values)).await;
        }
        Ok(())
    }

    async fn delete(
        &mut self,
        _stored_count: usize,
        _batch: &mut Self::Batch,
    ) -> Result<(), MemoryViewError> {
        self.erase().await
    }
}

#[async_trait]
impl<T> QueueOperations<T> for MemoryContext
where
    T: Clone + Send + Sync + 'static,
{
    async fn indices(&mut self) -> Result<Range<usize>, Self::Error> {
        Ok(self
            .with_ref(|v: Option<&VecDeque<T>>| match v {
                None => 0..0,
                Some(x) => 0..x.len(),
            })
            .await)
    }

    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error> {
        Ok(self
            .with_ref(|v: Option<&VecDeque<T>>| v?.get(index).cloned())
            .await)
    }

    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, Self::Error> {
        Ok(self
            .with_ref(|v: Option<&VecDeque<T>>| match v {
                None => Vec::new(),
                Some(x) => x.range(range).cloned().collect(),
            })
            .await)
    }

    async fn delete_front(
        &mut self,
        _stored_indices: &mut Range<usize>,
        _batch: &mut Self::Batch,
        count: usize,
    ) -> Result<(), Self::Error> {
        self.with_mut(|v: &mut VecDeque<T>| {
            for _ in 0..count {
                v.pop_front();
            }
        })
        .await;
        Ok(())
    }

    async fn append_back(
        &mut self,
        _stored_indices: &mut Range<usize>,
        _batch: &mut Self::Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error> {
        self.with_mut(|v: &mut VecDeque<T>| {
            for value in values {
                v.push_back(value);
            }
        })
        .await;
        Ok(())
    }

    async fn delete(
        &mut self,
        _stored_indices: Range<usize>,
        _batch: &mut Self::Batch,
    ) -> Result<(), MemoryViewError> {
        self.erase().await
    }
}

#[async_trait]
impl<I, V> MapOperations<I, V> for MemoryContext
where
    I: Eq + Ord + Send + Sync + Clone + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn get(&mut self, index: &I) -> Result<Option<V>, MemoryViewError> {
        Ok(self
            .with_ref(|m: Option<&BTreeMap<I, V>>| m?.get(index).cloned())
            .await)
    }

    async fn insert(
        &mut self,
        _batch: &mut Self::Batch,
        index: I,
        value: V,
    ) -> Result<(), MemoryViewError> {
        self.with_mut(|m: &mut BTreeMap<I, V>| {
            m.insert(index, value);
        })
        .await;
        Ok(())
    }

    async fn remove(&mut self, _batch: &mut Self::Batch, index: I) -> Result<(), MemoryViewError> {
        self.with_mut(|m: &mut BTreeMap<I, V>| {
            m.remove(&index);
        })
        .await;
        Ok(())
    }

    async fn indices(&mut self) -> Result<Vec<I>, MemoryViewError> {
        Ok(self
            .with_ref(|m: Option<&BTreeMap<I, V>>| match m {
                None => Vec::new(),
                Some(m) => m.keys().cloned().collect(),
            })
            .await)
    }

    async fn delete(&mut self, _batch: &mut Self::Batch) -> Result<(), MemoryViewError> {
        self.erase().await
    }
}

#[derive(Serialize)]
enum CollectionKey<I> {
    Indices,
    Subview(I),
}

#[async_trait]
impl<I> CollectionOperations<I> for MemoryContext
where
    I: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + Ord + Clone + 'static,
{
    fn clone_with_scope(&self, index: &I) -> Self {
        Self {
            map: self.map.clone(),
            base_key: self.derive_key(&CollectionKey::Subview(index)),
        }
    }

    async fn add_index(&mut self, _batch: &mut Self::Batch, index: I) -> Result<(), Self::Error> {
        let context = Self {
            map: self.map.clone(),
            base_key: self.derive_key(&CollectionKey::<I>::Indices),
        };
        context
            .with_mut(|m: &mut BTreeSet<I>| {
                m.insert(index);
            })
            .await;
        Ok(())
    }

    async fn remove_index(
        &mut self,
        _batch: &mut Self::Batch,
        index: I,
    ) -> Result<(), Self::Error> {
        let mut context = Self {
            map: self.map.clone(),
            base_key: self.derive_key(&CollectionKey::<I>::Indices),
        };
        let is_empty = context
            .with_mut(|m: &mut BTreeSet<I>| {
                m.remove(&index);
                m.is_empty()
            })
            .await;
        if is_empty {
            context.erase().await?;
        }
        Ok(())
    }

    async fn indices(&mut self) -> Result<Vec<I>, Self::Error> {
        let context = Self {
            map: self.map.clone(),
            base_key: self.derive_key(&CollectionKey::<I>::Indices),
        };
        Ok(context
            .with_ref(|m: Option<&BTreeSet<I>>| match m {
                None => Vec::new(),
                Some(m) => m.iter().cloned().collect(),
            })
            .await)
    }
}

impl HashingContext for MemoryContext {
    type Hasher = sha2::Sha512;
}

#[derive(Error, Debug)]
pub enum MemoryViewError {
    #[error("View error: {0}")]
    ViewError(#[from] ViewError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("BCS error: {0}")]
    Bcs(#[from] bcs::Error),

    #[error("Entry does not exist in memory: {0}")]
    NotFound(String),
}

impl From<MemoryViewError> for linera_base::error::Error {
    fn from(error: MemoryViewError) -> Self {
        Self::StorageError {
            backend: "memory".to_string(),
            error: error.to_string(),
        }
    }
}
