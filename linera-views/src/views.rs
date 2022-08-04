// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use std::{
    borrow::Borrow,
    cmp::Eq,
    collections::{hash_map, HashMap},
    error::Error,
    fmt::Debug,
    hash::Hash,
};

/// The context in which a view is operated. Typically, this includes the client to
/// connect to the database.
pub trait Context {
    /// The error type in use.
    type Error: Error;

    /// Clone the context and advance the (otherwise implicit) base key for all read/write
    /// operations.
    fn clone_with_scope<I: serde::Serialize>(&self, index: &I) -> Self;
}

/// A view gives an exclusive access to read and write the data stored at an underlying
/// address in storage.
#[async_trait]
pub trait View<C: Context>: Sized {
    /// Create a view or a subview.
    async fn load(context: C) -> Result<Self, C::Error>;

    /// Discard all pending changes. After that `commit` should have no effect to storage.
    fn reset(&mut self);

    /// Persist changes to storage. This consumes the view. If the view is dropped without
    /// calling `commit`, staged changes are simply lost.
    async fn commit(self) -> Result<(), C::Error>;
}

/// A view that adds a prefix to all the keys of the contained view.
#[derive(Debug, Clone)]
pub struct ScopedView<const INDEX: u64, W> {
    view: W,
}

impl<W, const INDEX: u64> std::ops::Deref for ScopedView<INDEX, W> {
    type Target = W;

    fn deref(&self) -> &W {
        &self.view
    }
}

impl<W, const INDEX: u64> std::ops::DerefMut for ScopedView<INDEX, W> {
    fn deref_mut(&mut self) -> &mut W {
        &mut self.view
    }
}

#[async_trait]
impl<C, W, const INDEX: u64> View<C> for ScopedView<INDEX, W>
where
    C: Context + Send + Sync + 'static,
    W: View<C> + Send,
{
    async fn load(context: C) -> Result<Self, C::Error> {
        let view = W::load(context.clone_with_scope(&INDEX)).await?;
        Ok(Self { view })
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        self.view.commit().await
    }

    fn reset(&mut self) {
        self.view.reset();
    }
}

/// A view that supports modifying a single value of type `T`.
#[derive(Debug, Clone)]
pub struct RegisterView<C, T> {
    context: C,
    old_value: T,
    new_value: Option<T>,
}

/// The context operations supporting [`RegisterView`].
#[async_trait]
pub trait RegisterOperations<T>: Context {
    async fn get(&mut self) -> Result<T, Self::Error>;

    async fn set(&mut self, value: T) -> Result<(), Self::Error>;
}

#[async_trait]
impl<C, T> View<C> for RegisterView<C, T>
where
    C: RegisterOperations<T> + Send + Sync,
    T: Send + Sync,
{
    async fn load(mut context: C) -> Result<Self, C::Error> {
        let old_value = context.get().await?;
        Ok(Self {
            context,
            old_value,
            new_value: None,
        })
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        if let Some(value) = self.new_value {
            self.context.set(value).await?;
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.new_value = None;
    }
}

impl<C, T> RegisterView<C, T> {
    /// Read the value in the register.
    pub fn get(&self) -> &T {
        match &self.new_value {
            Some(value) => value,
            None => &self.old_value,
        }
    }

    /// Set the value in the register.
    pub fn set(&mut self, value: T) {
        self.new_value = Some(value);
    }
}

/// A view that supports logging values of type `T`.
#[derive(Debug, Clone)]
pub struct AppendOnlyLogView<C, T> {
    context: C,
    old_count: usize,
    new_values: Vec<T>,
}

/// The context operations supporting [`AppendOnlyLogView`].
#[async_trait]
pub trait AppendOnlyLogOperations<T>: Context {
    async fn count(&mut self) -> Result<usize, Self::Error>;

    async fn read(&mut self, range: std::ops::Range<usize>) -> Result<Vec<T>, Self::Error>;

    async fn append(&mut self, values: Vec<T>) -> Result<(), Self::Error>;
}

#[async_trait]
impl<C, T> View<C> for AppendOnlyLogView<C, T>
where
    C: AppendOnlyLogOperations<T> + Send + Sync,
    T: Send + Sync + Clone,
{
    async fn load(mut context: C) -> Result<Self, C::Error> {
        let old_count = context.count().await?;
        Ok(Self {
            context,
            old_count,
            new_values: Vec::new(),
        })
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        if !self.new_values.is_empty() {
            self.context.append(self.new_values).await?;
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.new_values = Vec::new();
    }
}

impl<C, T> AppendOnlyLogView<C, T> {
    /// Push a value to the end of the log.
    pub fn push(&mut self, value: T) {
        self.new_values.push(value);
    }

    /// Read the size of the log.
    pub fn count(&self) -> usize {
        self.old_count + self.new_values.len()
    }
}

impl<C, T> AppendOnlyLogView<C, T>
where
    C: AppendOnlyLogOperations<T> + Send + Sync,
    T: Send + Sync + Clone,
{
    /// Read the logged values in the given range (including staged ones).
    pub async fn read(&mut self, mut range: std::ops::Range<usize>) -> Result<Vec<T>, C::Error> {
        if range.end > self.count() {
            range.end = self.count();
        }
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let mut values = Vec::new();
        values.reserve(range.end - range.start);
        if range.start < self.old_count {
            if range.end <= self.old_count {
                values.extend(self.context.read(range.start..range.end).await?);
            } else {
                values.extend(self.context.read(range.start..self.old_count).await?);
                values.extend(self.new_values[0..(range.end - self.old_count)].to_vec());
            }
        } else {
            values.extend(
                self.new_values[(range.start - self.old_count)..(range.end - self.old_count)]
                    .to_vec(),
            );
        }
        Ok(values)
    }
}

/// A view that supports inserting and removing individual values indexed by their keys.
#[derive(Debug, Clone)]
pub struct MapView<C, K, V> {
    context: C,
    updates: HashMap<K, Option<V>>,
}

/// The context operations supporting [`MapView`].
#[async_trait]
pub trait MapOperations<K, V>: Context {
    async fn get<T>(&mut self, index: &T) -> Result<Option<V>, Self::Error>
    where
        K: Borrow<T>,
        T: Eq + Hash + Sync + ?Sized;

    async fn insert(&mut self, index: K, value: V) -> Result<(), Self::Error>;

    async fn remove(&mut self, index: K) -> Result<(), Self::Error>;
}

#[async_trait]
impl<C, K, V> View<C> for MapView<C, K, V>
where
    C: MapOperations<K, V> + Send,
    K: Eq + Hash + Send + Sync,
    V: Clone + Send + Sync,
{
    async fn load(context: C) -> Result<Self, C::Error> {
        Ok(Self {
            context,
            updates: HashMap::new(),
        })
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        for (index, update) in self.updates {
            match update {
                None => {
                    self.context.remove(index).await?;
                }
                Some(value) => {
                    self.context.insert(index, value).await?;
                }
            }
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.updates = HashMap::new();
    }
}

impl<C, K, V> MapView<C, K, V>
where
    K: Eq + Hash,
{
    /// Set or insert a value.
    pub fn insert(&mut self, index: K, value: V) {
        self.updates.insert(index, Some(value));
    }

    /// Remove a value.
    pub fn remove(&mut self, index: K) {
        self.updates.insert(index, None);
    }
}

impl<C, K, V> MapView<C, K, V>
where
    C: MapOperations<K, V>,
    K: Eq + Hash + Sync,
    V: Clone,
{
    /// Read the value at the given postition, if any.
    pub async fn get<T>(&mut self, index: &T) -> Result<Option<V>, C::Error>
    where
        K: Borrow<T>,
        T: Eq + Hash + Sync + ?Sized,
    {
        if let Some(update) = self.updates.get(index) {
            return Ok(update.as_ref().cloned());
        }
        self.context.get(index).await
    }
}

/// A view that supports accessing a collection of views of the same kind, indexed by a
/// key.
#[derive(Debug, Clone)]
pub struct CollectionView<C, I, W> {
    context: C,
    active_views: HashMap<I, W>,
}

/// The context operations supporting [`CollectionView`].
pub trait CollectionOperations<I, W>: Context {}

#[async_trait]
impl<C, I, W> View<C> for CollectionView<C, I, W>
where
    C: CollectionOperations<I, W> + Send,
    I: Send,
    W: View<C> + Send,
{
    async fn load(context: C) -> Result<Self, C::Error> {
        Ok(Self {
            context,
            active_views: HashMap::new(),
        })
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        for view in self.active_views.into_values() {
            view.commit().await?;
        }
        // TODO: clean up empty entries
        Ok(())
    }

    fn reset(&mut self) {
        self.active_views = HashMap::new();
    }
}

impl<C, I, W> CollectionView<C, I, W>
where
    C: CollectionOperations<I, W> + Send,
    I: Eq + Hash + Sync + Clone + serde::Serialize,
    W: View<C>,
{
    /// Obtain a subview to access the data at the given index in the collection.
    pub async fn view(&mut self, index: I) -> Result<&mut W, C::Error> {
        match self.active_views.entry(index.clone()) {
            hash_map::Entry::Occupied(e) => Ok(e.into_mut()),
            hash_map::Entry::Vacant(e) => {
                let context = self.context.clone_with_scope(&index);
                let view = W::load(context).await?;
                Ok(e.insert(view))
            }
        }
    }
}
