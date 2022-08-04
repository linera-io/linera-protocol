// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use std::{
    borrow::Borrow,
    cmp::Eq,
    collections::{hash_map, HashMap},
    fmt::Debug,
    hash::Hash,
};
use thiserror::Error;

/// The context in which a view is operated. Typically, this includes the client to
/// connect to the database and the address of the current entry.
#[async_trait]
pub trait Context {
    /// The error type in use.
    type Error: Debug + Send + From<ViewError>;

    /// Erase the current entry from storage.
    async fn erase(&mut self) -> Result<(), Self::Error>;
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

    /// Instead of persisting changes, clear all the data that belong to this view and its
    /// subviews.
    async fn delete(self) -> Result<(), C::Error>;
}

#[derive(Error, Debug)]
pub enum ViewError {
    #[error("the entry with key {0} was removed thus cannot be loaded any more")]
    RemovedEntry(String),
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

pub trait ScopedOperations: Context {
    /// Clone the context and advance the (otherwise implicit) base key for all read/write
    /// operations.
    fn clone_with_scope(&self, index: u64) -> Self;
}

#[async_trait]
impl<C, W, const INDEX: u64> View<C> for ScopedView<INDEX, W>
where
    C: Context + Send + Sync + ScopedOperations + 'static,
    W: View<C> + Send,
{
    async fn load(context: C) -> Result<Self, C::Error> {
        let view = W::load(context.clone_with_scope(INDEX)).await?;
        Ok(Self { view })
    }

    fn reset(&mut self) {
        self.view.reset();
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        self.view.commit().await
    }

    async fn delete(self) -> Result<(), C::Error> {
        self.view.delete().await
    }
}

/// A view that supports modifying a single value of type `T`.
#[derive(Debug, Clone)]
pub struct RegisterView<C, T> {
    context: C,
    stored_value: T,
    update: Option<T>,
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
        let stored_value = context.get().await?;
        Ok(Self {
            context,
            stored_value,
            update: None,
        })
    }

    fn reset(&mut self) {
        self.update = None
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        if let Some(value) = self.update {
            self.context.set(value).await?;
        }
        Ok(())
    }

    async fn delete(mut self) -> Result<(), C::Error> {
        self.context.erase().await
    }
}

impl<C, T> RegisterView<C, T> {
    /// Read the value in the register.
    pub fn get(&self) -> &T {
        match &self.update {
            None => &self.stored_value,
            Some(value) => value,
        }
    }

    /// Set the value in the register.
    pub fn set(&mut self, value: T) {
        self.update = Some(value);
    }
}

/// A view that supports logging values of type `T`.
#[derive(Debug, Clone)]
pub struct AppendOnlyLogView<C, T> {
    context: C,
    stored_count: usize,
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
        let stored_count = context.count().await?;
        Ok(Self {
            context,
            stored_count,
            new_values: Vec::new(),
        })
    }

    fn reset(&mut self) {
        self.new_values.clear();
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        if !self.new_values.is_empty() {
            self.context.append(self.new_values).await?;
        }
        Ok(())
    }

    async fn delete(mut self) -> Result<(), C::Error> {
        self.context.erase().await
    }
}

impl<C, T> AppendOnlyLogView<C, T> {
    /// Push a value to the end of the log.
    pub fn push(&mut self, value: T) {
        self.new_values.push(value);
    }

    /// Read the size of the log.
    pub fn count(&self) -> usize {
        self.stored_count + self.new_values.len()
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
        if range.start < self.stored_count {
            if range.end <= self.stored_count {
                values.extend(self.context.read(range.start..range.end).await?);
            } else {
                values.extend(self.context.read(range.start..self.stored_count).await?);
                values.extend(self.new_values[0..(range.end - self.stored_count)].to_vec());
            }
        } else {
            values.extend(
                self.new_values[(range.start - self.stored_count)..(range.end - self.stored_count)]
                    .to_vec(),
            );
        }
        Ok(values)
    }
}

/// A view that supports inserting and removing values indexed by a key.
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

    fn reset(&mut self) {
        self.updates.clear();
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

    async fn delete(mut self) -> Result<(), C::Error> {
        self.context.erase().await
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
    /// Read the value at the given position, if any.
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
    updates: HashMap<I, Option<W>>,
}

/// The context operations supporting [`CollectionView`].
#[async_trait]
pub trait CollectionOperations<I>: Context {
    /// Clone the context and advance the (otherwise implicit) base key for all read/write
    /// operations.
    fn clone_with_scope(&self, index: &I) -> Self;

    /// Return the list of indices in the collection.
    async fn indices(&mut self) -> Result<Vec<I>, Self::Error>;
}

#[async_trait]
impl<C, I, W> View<C> for CollectionView<C, I, W>
where
    C: CollectionOperations<I> + Send,
    I: Send + Sync,
    W: View<C> + Send,
{
    async fn load(context: C) -> Result<Self, C::Error> {
        Ok(Self {
            context,
            updates: HashMap::new(),
        })
    }

    fn reset(&mut self) {
        self.updates.clear();
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        for (index, update) in self.updates {
            match update {
                Some(view) => view.commit().await?,
                None => {
                    let context = self.context.clone_with_scope(&index);
                    let view = W::load(context).await?;
                    view.delete().await?;
                }
            }
        }
        Ok(())
    }

    async fn delete(mut self) -> Result<(), C::Error> {
        for index in self.context.indices().await? {
            let context = self.context.clone_with_scope(&index);
            let view = W::load(context).await?;
            view.delete().await?;
        }
        Ok(())
    }
}

impl<C, I, W> CollectionView<C, I, W>
where
    C: CollectionOperations<I> + Send,
    I: Eq + Hash + Sync + Clone + Send + Debug,
    W: View<C>,
{
    /// Obtain a subview for the data at the given index in the collection. Return an
    /// error if `remove_entry` was used earlier on this index from the same [`CollectionView`].
    pub async fn load_entry(&mut self, index: I) -> Result<&mut W, C::Error> {
        match self.updates.entry(index.clone()) {
            hash_map::Entry::Occupied(e) => match e.into_mut() {
                Some(view) => Ok(view),
                None => Err(C::Error::from(ViewError::RemovedEntry(format!(
                    "{:?}",
                    index
                )))),
            },
            hash_map::Entry::Vacant(e) => {
                let context = self.context.clone_with_scope(&index);
                let view = W::load(context).await?;
                Ok(e.insert(Some(view)).as_mut().unwrap())
            }
        }
    }

    /// Mark the entry so that it is removed in the next commit.
    pub fn remove_entry(&mut self, index: I) {
        self.updates.insert(index, None);
    }
}
