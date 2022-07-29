// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::views::{
    AppendOnlyLogKey, AppendOnlyLogOperations, CollectionKey, Context, Key, MapKey, MapOperations,
    RegisterKey, RegisterOperations,
};
use async_trait::async_trait;
use std::{
    any::Any, borrow::Borrow, cmp::Eq, collections::HashMap, convert::Infallible, fmt::Debug,
    hash::Hash, sync::Arc,
};
use tokio::sync::{OwnedMutexGuard, RwLock};

/// A context that stores all values in memory.
#[derive(Clone, Debug)]
pub struct InMemoryContext(Arc<RwLock<OwnedMutexGuard<EntryMap>>>);

/// A Rust value stored in memory.
pub type Entry = Box<dyn Any + Send + Sync + 'static>;

/// A map of Rust values indexed by their keys.
pub type EntryMap = HashMap<Vec<u8>, Entry>;

impl InMemoryContext {
    pub fn new(guard: OwnedMutexGuard<EntryMap>) -> Self {
        Self(Arc::new(RwLock::new(guard)))
    }
}

impl Context for InMemoryContext {
    type Error = Infallible;
}

/// Helper trait to create views on memory data.
#[async_trait]
pub trait InMemoryKey: Key {
    /// The encoding of the data in memory
    type Value: Default + Send + Sync + 'static;

    async fn with_ref<F: Send, V>(&self, context: &InMemoryContext, f: F) -> V
    where
        F: FnOnce(Option<&Self::Value>) -> V,
    {
        let base = context.0.read().await;
        match base.get(self.as_ref()) {
            None => f(None),
            Some(blob) => {
                let value = blob
                    .downcast_ref::<Self::Value>()
                    .expect("downcast to &Value should not fail");
                f(Some(value))
            }
        }
    }

    async fn with_mut<F: Send, V>(&self, context: &InMemoryContext, f: F) -> V
    where
        F: FnOnce(&mut Self::Value) -> V,
    {
        let mut base = context.0.write().await;
        let blob = base
            .entry(self.as_ref().to_vec())
            .or_insert_with(|| Box::new(Self::Value::default()));
        let value = blob
            .downcast_mut::<Self::Value>()
            .expect("downcast to &mut Value should not fail");
        f(value)
    }
}

#[async_trait]
impl<T> InMemoryKey for RegisterKey<T>
where
    T: Default + Send + Sync + 'static,
{
    type Value = T;
}

#[async_trait]
impl<T> RegisterOperations<T> for InMemoryContext
where
    T: Default + Clone + Send + Sync + 'static,
{
    async fn get(&mut self, key: &RegisterKey<T>) -> Result<T, Infallible> {
        Ok(key
            .with_ref(self, |value| value.cloned().unwrap_or_default())
            .await)
    }

    async fn set(&mut self, key: &RegisterKey<T>, value: T) -> Result<(), Infallible> {
        let mut base = self.0.write().await;
        base.insert(key.as_ref().to_vec(), Box::new(value));
        Ok(())
    }
}

#[async_trait]
impl<T> InMemoryKey for AppendOnlyLogKey<T>
where
    T: Send + Sync + 'static,
{
    type Value = Vec<T>;
}

#[async_trait]
impl<T> AppendOnlyLogOperations<T> for InMemoryContext
where
    T: Clone + Send + Sync + 'static,
{
    async fn count(&mut self, key: &AppendOnlyLogKey<T>) -> Result<usize, Infallible> {
        Ok(key
            .with_ref(self, |slice| match slice {
                None => 0,
                Some(s) => s.len(),
            })
            .await)
    }

    async fn read(
        &mut self,
        key: &AppendOnlyLogKey<T>,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<T>, Infallible> {
        Ok(key
            .with_ref(self, |slice| match slice {
                None => Vec::new(),
                Some(s) => s[range].to_vec(),
            })
            .await)
    }

    async fn append(
        &mut self,
        key: &AppendOnlyLogKey<T>,
        mut values: Vec<T>,
    ) -> Result<(), Infallible> {
        if !values.is_empty() {
            key.with_mut(self, |v| v.append(&mut values)).await;
        }
        Ok(())
    }
}

#[async_trait]
impl<K, V> InMemoryKey for MapKey<K, V>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    type Value = HashMap<K, V>;
}

#[async_trait]
impl<K, V> MapOperations<K, V> for InMemoryContext
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn get<K2>(&mut self, key: &MapKey<K, V>, index: &K2) -> Result<Option<V>, Infallible>
    where
        K: Borrow<K2>,
        K2: Eq + Hash + Sync + ?Sized,
    {
        Ok(key
            .with_ref(self, |m| match m {
                None => None,
                Some(m) => m.get(index).cloned(),
            })
            .await)
    }

    async fn insert(&mut self, key: &MapKey<K, V>, index: K, value: V) -> Result<(), Infallible> {
        key.with_mut(self, |m| {
            m.insert(index, value);
        })
        .await;
        Ok(())
    }

    async fn remove(&mut self, key: &MapKey<K, V>, index: K) -> Result<(), Infallible> {
        key.with_mut(self, |m| {
            m.remove(&index);
        })
        .await;
        Ok(())
    }
}

#[async_trait]
impl<I, K> InMemoryKey for CollectionKey<I, K> {
    type Value = (); // Not storing anything at the root of the collection
}
