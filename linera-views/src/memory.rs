// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::views::{
    AppendOnlyLogOperations, CollectionOperations, Context, MapOperations, RegisterOperations,
};
use async_trait::async_trait;
use std::{
    any::Any, borrow::Borrow, cmp::Eq, collections::HashMap, convert::Infallible, fmt::Debug,
    hash::Hash, sync::Arc,
};
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
pub type EntryMap = HashMap<Vec<u8>, Entry>;

impl InMemoryContext {
    pub fn new(guard: OwnedMutexGuard<EntryMap>) -> Self {
        Self {
            map: Arc::new(RwLock::new(guard)),
            base_key: Vec::new(),
        }
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

impl Context for InMemoryContext {
    type Error = Infallible;

    fn clone_with_scope<I: serde::Serialize>(&self, index: &I) -> Self {
        let mut base_key = self.base_key.clone();
        bcs::serialize_into(&mut base_key, index).unwrap();
        Self {
            map: self.map.clone(),
            base_key,
        }
    }
}

#[async_trait]
impl<T> RegisterOperations<T> for InMemoryContext
where
    T: Default + Clone + Send + Sync + 'static,
{
    async fn get(&mut self) -> Result<T, Infallible> {
        Ok(self
            .with_ref(|value: Option<&T>| value.cloned().unwrap_or_default())
            .await)
    }

    async fn set(&mut self, value: T) -> Result<(), Infallible> {
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
    async fn count(&mut self) -> Result<usize, Infallible> {
        Ok(self
            .with_ref(|v: Option<&Vec<T>>| match v {
                None => 0,
                Some(s) => s.len(),
            })
            .await)
    }

    async fn read(&mut self, range: std::ops::Range<usize>) -> Result<Vec<T>, Infallible> {
        Ok(self
            .with_ref(|v: Option<&Vec<T>>| match v {
                None => Vec::new(),
                Some(s) => s[range].to_vec(),
            })
            .await)
    }

    async fn append(&mut self, mut values: Vec<T>) -> Result<(), Infallible> {
        if !values.is_empty() {
            self.with_mut(|v: &mut Vec<T>| v.append(&mut values)).await;
        }
        Ok(())
    }
}

#[async_trait]
impl<K, V> MapOperations<K, V> for InMemoryContext
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn get<K2>(&mut self, index: &K2) -> Result<Option<V>, Infallible>
    where
        K: Borrow<K2>,
        K2: Eq + Hash + Sync + ?Sized,
    {
        Ok(self
            .with_ref(|m: Option<&HashMap<K, V>>| match m {
                None => None,
                Some(m) => m.get(index).cloned(),
            })
            .await)
    }

    async fn insert(&mut self, index: K, value: V) -> Result<(), Infallible> {
        self.with_mut(|m: &mut HashMap<K, V>| {
            m.insert(index, value);
        })
        .await;
        Ok(())
    }

    async fn remove(&mut self, index: K) -> Result<(), Infallible> {
        self.with_mut(|m: &mut HashMap<K, V>| {
            m.remove(&index);
        })
        .await;
        Ok(())
    }
}

impl<K, V> CollectionOperations<K, V> for InMemoryContext {}
