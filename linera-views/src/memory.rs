// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    hash::HashingContext,
    views::{
        AppendOnlyLogOperations, CollectionOperations, Context, MapOperations, RegisterOperations,
        ScopedOperations, ViewError,
    },
};
use async_trait::async_trait;
use std::{any::Any, cmp::Eq, collections::BTreeMap, fmt::Debug, ops::Bound, sync::Arc};
use thiserror::Error;
use tokio::sync::{OwnedMutexGuard, RwLock};

/// A context that stores all values in memory.
#[derive(Clone, Debug)]
pub struct InMemoryContext {
    map: Arc<RwLock<OwnedMutexGuard<EntryMap>>>,
    base_key: Vec<u8>,
}

/// A Rust value stored in memory.
pub type Entry = Box<dyn Any + Send + Sync + 'static>;

/// A map of Rust values indexed by their keys.
pub type EntryMap = BTreeMap<Vec<u8>, Entry>;

impl InMemoryContext {
    pub fn new(guard: OwnedMutexGuard<EntryMap>) -> Self {
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
}

#[async_trait]
impl Context for InMemoryContext {
    type Error = MemoryViewError;

    async fn erase(&mut self) -> Result<(), Self::Error> {
        let mut map = self.map.write().await;
        map.remove(&self.base_key);
        Ok(())
    }
}

#[async_trait]
impl ScopedOperations for InMemoryContext {
    fn clone_with_scope(&self, index: u64) -> Self {
        Self {
            map: self.map.clone(),
            base_key: self.derive_key(&index),
        }
    }
}

#[async_trait]
impl<T> RegisterOperations<T> for InMemoryContext
where
    T: Default + Clone + Send + Sync + 'static,
{
    async fn get(&mut self) -> Result<T, MemoryViewError> {
        Ok(self
            .with_ref(|value: Option<&T>| value.cloned().unwrap_or_default())
            .await)
    }

    async fn set(&mut self, value: T) -> Result<(), MemoryViewError> {
        let mut map = self.map.write().await;
        map.insert(self.base_key.clone(), Box::new(value));
        Ok(())
    }
}

#[async_trait]
impl<T> AppendOnlyLogOperations<T> for InMemoryContext
where
    T: Clone + Send + Sync + 'static,
{
    async fn count(&mut self) -> Result<usize, MemoryViewError> {
        Ok(self
            .with_ref(|v: Option<&Vec<T>>| match v {
                None => 0,
                Some(s) => s.len(),
            })
            .await)
    }

    async fn read(&mut self, range: std::ops::Range<usize>) -> Result<Vec<T>, MemoryViewError> {
        Ok(self
            .with_ref(|v: Option<&Vec<T>>| match v {
                None => Vec::new(),
                Some(s) => s[range].to_vec(),
            })
            .await)
    }

    async fn append(&mut self, mut values: Vec<T>) -> Result<(), MemoryViewError> {
        if !values.is_empty() {
            self.with_mut(|v: &mut Vec<T>| v.append(&mut values)).await;
        }
        Ok(())
    }
}

#[async_trait]
impl<I, V> MapOperations<I, V> for InMemoryContext
where
    I: Eq + Ord + Send + Sync + Clone + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn get(&mut self, index: &I) -> Result<Option<V>, MemoryViewError> {
        Ok(self
            .with_ref(|m: Option<&BTreeMap<I, V>>| match m {
                None => None,
                Some(m) => m.get(index).cloned(),
            })
            .await)
    }

    async fn insert(&mut self, index: I, value: V) -> Result<(), MemoryViewError> {
        self.with_mut(|m: &mut BTreeMap<I, V>| {
            m.insert(index, value);
        })
        .await;
        Ok(())
    }

    async fn remove(&mut self, index: I) -> Result<(), MemoryViewError> {
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
}

#[async_trait]
impl<I> CollectionOperations<I> for InMemoryContext
where
    I: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    fn clone_with_scope(&self, index: &I) -> Self {
        Self {
            map: self.map.clone(),
            base_key: self.derive_key(index),
        }
    }

    async fn indices(&mut self) -> Result<Vec<I>, Self::Error> {
        let map = self.map.read().await;
        let base_len = self.base_key.len();
        Ok(map
            .range::<Vec<u8>, _>((Bound::Excluded(&self.base_key), Bound::Unbounded))
            .take_while(|(k, _)| k.starts_with(&self.base_key))
            .map(|(k, _)| bcs::from_bytes(&k[base_len..]))
            .collect::<Result<Vec<I>, bcs::Error>>()?)
    }
}

impl HashingContext for InMemoryContext {
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
}
