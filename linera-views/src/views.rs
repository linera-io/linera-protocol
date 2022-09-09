// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use futures::FutureExt;
use linera_base::ensure;
use std::{
    cmp::Eq,
    collections::{btree_map, BTreeMap, VecDeque},
    fmt::Debug,
    mem,
    ops::{Deref, DerefMut, Range},
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::{Mutex, OwnedMutexGuard};

#[cfg(test)]
#[path = "unit_tests/views.rs"]
mod tests;

/// The context in which a view is operated. Typically, this includes the client to
/// connect to the database and the address of the current entry.
#[async_trait]
pub trait Context {
    /// A batch of writes inside a transaction;
    type Batch: Send + Sync;

    /// User provided data to be carried along.
    type Extra: Clone + Send + Sync;

    /// The error type in use.
    type Error: std::error::Error + Debug + Send + Sync + From<ViewError> + From<std::io::Error>;

    /// Getter for the user provided data.
    fn extra(&self) -> &Self::Extra;

    /// Provide a reference to a new batch to the builder then execute the batch.
    async fn run_with_batch<F>(&self, builder: F) -> Result<(), Self::Error>
    where
        F: FnOnce(&mut Self::Batch) -> futures::future::BoxFuture<Result<(), Self::Error>>
            + Send
            + Sync;
}

/// A view gives an exclusive access to read and write the data stored at an underlying
/// address in storage.
#[async_trait]
pub trait View<C: Context>: Sized {
    /// Obtain a mutable reference to the internal context.
    fn context(&self) -> &C;

    /// Create a view or a subview.
    async fn load(context: C) -> Result<Self, C::Error>;

    /// Discard all pending changes. After that `commit` should have no effect to storage.
    fn rollback(&mut self);

    /// Persist changes to storage and reset the view's staged changes. Crash-resistant storage
    /// implementations are expected to accumulate the desired changes in the `batch`
    /// variable first. If the view is dropped without calling `commit` or `commit_and_reset`,
    /// staged changes are simply lost.
    async fn commit_and_reset(&mut self, batch: &mut C::Batch) -> Result<(), C::Error>;

    /// Persist changes to storage. This consumes the view. Crash-resistant storage
    /// implementations are expected to accumulate the desired changes in the `batch`
    /// variable first. If the view is dropped without calling `commit` or `commit_and_reset`,
    /// staged changes are simply lost.
    async fn commit(mut self, batch: &mut C::Batch) -> Result<(), C::Error> {
        self.commit_and_reset(batch).await
    }

    /// Instead of persisting changes, clear all the data that belong to this view and its
    /// subviews. Crash-resistant storage implementations are expected to accumulate the
    /// desired changes into the `batch` variable first.
    async fn delete(self, batch: &mut C::Batch) -> Result<(), C::Error>;
}

#[derive(Error, Debug)]
pub enum ViewError {
    #[error("the entry with key {0} was removed thus cannot be loaded any more")]
    RemovedEntry(String),

    #[error("failed to serialize value to calculate its hash")]
    Serialization(#[from] bcs::Error),
}

/// A view that adds a prefix to all the keys of the contained view.
#[derive(Debug, Clone)]
pub struct ScopedView<const INDEX: u64, W> {
    pub(crate) view: W,
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
    fn context(&self) -> &C {
        self.view.context()
    }

    async fn load(context: C) -> Result<Self, C::Error> {
        let view = W::load(context.clone_with_scope(INDEX)).await?;
        Ok(Self { view })
    }

    fn rollback(&mut self) {
        self.view.rollback();
    }

    async fn commit_and_reset(&mut self, batch: &mut C::Batch) -> Result<(), C::Error> {
        self.view.commit_and_reset(batch).await
    }

    async fn delete(self, batch: &mut C::Batch) -> Result<(), C::Error> {
        self.view.delete(batch).await
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
    /// Obtain the value in the register.
    async fn get(&mut self) -> Result<T, Self::Error>;

    /// Set the value in the register. Crash-resistant implementations should only write to `batch`.
    async fn set(&mut self, batch: &mut Self::Batch, value: T) -> Result<(), Self::Error>;

    /// Delete the register. Crash-resistant implementations should only write to `batch`.
    async fn delete(&mut self, batch: &mut Self::Batch) -> Result<(), Self::Error>;
}

#[async_trait]
impl<C, T> View<C> for RegisterView<C, T>
where
    C: RegisterOperations<T> + Send + Sync,
    T: Clone + Send + Sync,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(mut context: C) -> Result<Self, C::Error> {
        let stored_value = context.get().await?;
        Ok(Self {
            context,
            stored_value,
            update: None,
        })
    }

    fn rollback(&mut self) {
        self.update = None
    }

    async fn commit_and_reset(&mut self, batch: &mut C::Batch) -> Result<(), C::Error> {
        if let Some(value) = self.update.take() {
            self.context.set(batch, value.clone()).await?;
            self.stored_value = value;
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut C::Batch) -> Result<(), C::Error> {
        self.context.delete(batch).await
    }
}

impl<C, T> RegisterView<C, T>
where
    C: Context,
{
    /// Access the current value in the register.
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

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, T> RegisterView<C, T>
where
    C: Context,
    T: Clone,
{
    /// Obtain a mutable reference to the value in the register.
    pub fn get_mut(&mut self) -> &mut T {
        match &mut self.update {
            Some(value) => value,
            update => {
                *update = Some(self.stored_value.clone());
                update.as_mut().unwrap()
            }
        }
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
    /// Return the size of the log in storage.
    async fn count(&mut self) -> Result<usize, Self::Error>;

    /// Obtain the value at the given index.
    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error>;

    /// Obtain the values in the given range of indices.
    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, Self::Error>;

    /// Append values to the logs. Crash-resistant implementations should only write to `batch`.
    async fn append(
        &mut self,
        stored_count: usize,
        batch: &mut Self::Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error>;

    /// Delete the log. Crash-resistant implementations should only write to `batch`.
    async fn delete(
        &mut self,
        stored_count: usize,
        batch: &mut Self::Batch,
    ) -> Result<(), Self::Error>;
}

#[async_trait]
impl<C, T> View<C> for AppendOnlyLogView<C, T>
where
    C: AppendOnlyLogOperations<T> + Send + Sync,
    T: Send + Sync + Clone,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(mut context: C) -> Result<Self, C::Error> {
        let stored_count = context.count().await?;
        Ok(Self {
            context,
            stored_count,
            new_values: Vec::new(),
        })
    }

    fn rollback(&mut self) {
        self.new_values.clear();
    }

    async fn commit_and_reset(&mut self, batch: &mut C::Batch) -> Result<(), C::Error> {
        if !self.new_values.is_empty() {
            let new_count = self.stored_count + self.new_values.len();
            self.context
                .append(self.stored_count, batch, mem::take(&mut self.new_values))
                .await?;
            self.stored_count = new_count;
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut C::Batch) -> Result<(), C::Error> {
        self.context.delete(self.stored_count, batch).await
    }
}

impl<C, T> AppendOnlyLogView<C, T>
where
    C: Context,
{
    /// Push a value to the end of the log.
    pub fn push(&mut self, value: T) {
        self.new_values.push(value);
    }

    /// Read the size of the log.
    pub fn count(&self) -> usize {
        self.stored_count + self.new_values.len()
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, T> AppendOnlyLogView<C, T>
where
    C: AppendOnlyLogOperations<T> + Send + Sync,
    T: Send + Sync + Clone,
{
    /// Read the logged values in the given range (including staged ones).
    pub async fn get(&mut self, index: usize) -> Result<Option<T>, C::Error> {
        if index < self.stored_count {
            self.context.get(index).await
        } else {
            Ok(self.new_values.get(index - self.stored_count).cloned())
        }
    }

    /// Read the logged values in the given range (including staged ones).
    pub async fn read(&mut self, mut range: Range<usize>) -> Result<Vec<T>, C::Error> {
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
pub struct MapView<C, I, V> {
    context: C,
    updates: BTreeMap<I, Option<V>>,
}

/// The context operations supporting [`MapView`].
#[async_trait]
pub trait MapOperations<I, V>: Context {
    /// Obtain the value at the given index, if any.
    async fn get(&mut self, index: &I) -> Result<Option<V>, Self::Error>;

    /// Return the list of indices in the map.
    async fn indices(&mut self) -> Result<Vec<I>, Self::Error>;

    /// Set a value. Crash-resistant implementations should only write to `batch`.
    async fn insert(
        &mut self,
        batch: &mut Self::Batch,
        index: I,
        value: V,
    ) -> Result<(), Self::Error>;

    /// Remove the entry at the given index. Crash-resistant implementations should only write
    /// to `batch`.
    async fn remove(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error>;

    /// Delete the map and its entries from storage. Crash-resistant implementations should only
    /// write to `batch`.
    async fn delete(&mut self, batch: &mut Self::Batch) -> Result<(), Self::Error>;
}

#[async_trait]
impl<C, I, V> View<C> for MapView<C, I, V>
where
    C: MapOperations<I, V> + Send,
    I: Eq + Ord + Send + Sync,
    V: Clone + Send + Sync,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, C::Error> {
        Ok(Self {
            context,
            updates: BTreeMap::new(),
        })
    }

    fn rollback(&mut self) {
        self.updates.clear();
    }

    async fn commit_and_reset(&mut self, batch: &mut C::Batch) -> Result<(), C::Error> {
        for (index, update) in mem::take(&mut self.updates) {
            match update {
                None => {
                    self.context.remove(batch, index).await?;
                }
                Some(value) => {
                    self.context.insert(batch, index, value).await?;
                }
            }
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut C::Batch) -> Result<(), C::Error> {
        self.context.delete(batch).await
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context,
    I: Eq + Ord,
{
    /// Set or insert a value.
    pub fn insert(&mut self, index: I, value: V) {
        self.updates.insert(index, Some(value));
    }

    /// Remove a value.
    pub fn remove(&mut self, index: I) {
        self.updates.insert(index, None);
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: MapOperations<I, V>,
    I: Eq + Ord + Sync + Clone,
    V: Clone,
{
    /// Read the value at the given position, if any.
    pub async fn get(&mut self, index: &I) -> Result<Option<V>, C::Error> {
        if let Some(update) = self.updates.get(index) {
            return Ok(update.as_ref().cloned());
        }
        self.context.get(index).await
    }

    /// Return the list of indices in the map.
    pub async fn indices(&mut self) -> Result<Vec<I>, C::Error> {
        let mut indices = Vec::new();
        for index in self.context.indices().await? {
            if !self.updates.contains_key(&index) {
                indices.push(index);
            }
        }
        for (index, entry) in &self.updates {
            if entry.is_some() {
                indices.push(index.clone());
            }
        }
        indices.sort();
        Ok(indices)
    }
}

/// A view that supports a FIFO queue for values of type `T`.
#[derive(Debug, Clone)]
pub struct QueueView<C, T> {
    context: C,
    stored_indices: Range<usize>,
    front_delete_count: usize,
    new_back_values: VecDeque<T>,
}

/// The context operations supporting [`QueueView`].
#[async_trait]
pub trait QueueOperations<T>: Context {
    /// Obtain the range of indices in the stored queue.
    async fn indices(&mut self) -> Result<Range<usize>, Self::Error>;

    /// Obtain the value at the given index.
    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error>;

    /// Obtain the values in the given range.
    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, Self::Error>;

    /// Delete `count` values from the front of the queue. Crash-resistant implementations
    /// should only write to `batch`.
    async fn delete_front(
        &mut self,
        stored_indices: &mut Range<usize>,
        batch: &mut Self::Batch,
        count: usize,
    ) -> Result<(), Self::Error>;

    /// Append the given values from the back of the queue. Crash-resistant
    /// implementations should only write to `batch`.
    async fn append_back(
        &mut self,
        stored_indices: &mut Range<usize>,
        batch: &mut Self::Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error>;

    /// Delete the queue from storage. Crash-resistant implementations should only write to `batch`.
    async fn delete(
        &mut self,
        stored_indices: Range<usize>,
        batch: &mut Self::Batch,
    ) -> Result<(), Self::Error>;
}

#[async_trait]
impl<C, T> View<C> for QueueView<C, T>
where
    C: QueueOperations<T> + Send + Sync,
    T: Send + Sync + Clone,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(mut context: C) -> Result<Self, C::Error> {
        let stored_indices = context.indices().await?;
        Ok(Self {
            context,
            stored_indices,
            front_delete_count: 0,
            new_back_values: VecDeque::new(),
        })
    }

    fn rollback(&mut self) {
        self.front_delete_count = 0;
        self.new_back_values.clear();
    }

    async fn commit_and_reset(&mut self, batch: &mut C::Batch) -> Result<(), C::Error> {
        if self.front_delete_count > 0 {
            self.context
                .delete_front(&mut self.stored_indices, batch, self.front_delete_count)
                .await?;
            self.front_delete_count = 0;
        }
        if !self.new_back_values.is_empty() {
            self.context
                .append_back(
                    &mut self.stored_indices,
                    batch,
                    mem::take(&mut self.new_back_values).into_iter().collect(),
                )
                .await?;
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut C::Batch) -> Result<(), C::Error> {
        self.context.delete(self.stored_indices, batch).await
    }
}

impl<C, T> QueueView<C, T>
where
    C: QueueOperations<T> + Send + Sync,
    T: Send + Sync + Clone,
{
    /// Read the front value, if any.
    pub async fn front(&mut self) -> Result<Option<T>, C::Error> {
        let stored_remainder = self.stored_indices.len() - self.front_delete_count;
        if stored_remainder > 0 {
            self.context
                .get(self.stored_indices.end - stored_remainder)
                .await
        } else {
            Ok(self.new_back_values.front().cloned())
        }
    }

    /// Read the back value, if any.
    pub async fn back(&mut self) -> Result<Option<T>, C::Error> {
        match self.new_back_values.back() {
            Some(value) => Ok(Some(value.clone())),
            None if self.stored_indices.len() > self.front_delete_count => {
                self.context.get(self.stored_indices.end - 1).await
            }
            _ => Ok(None),
        }
    }

    /// Delete the front value, if any.
    pub fn delete_front(&mut self) {
        if self.front_delete_count < self.stored_indices.len() {
            self.front_delete_count += 1;
        } else {
            self.new_back_values.pop_front();
        }
    }

    /// Push a value to the end of the queue.
    pub fn push_back(&mut self, value: T) {
        self.new_back_values.push_back(value);
    }

    /// Read the size of the queue.
    pub fn count(&self) -> usize {
        self.stored_indices.len() - self.front_delete_count + self.new_back_values.len()
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }

    /// Read the `count` next values in the queue (including staged ones).
    pub async fn read_front(&mut self, mut count: usize) -> Result<Vec<T>, C::Error> {
        if count > self.count() {
            count = self.count();
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut values = Vec::new();
        values.reserve(count);
        let stored_remainder = self.stored_indices.len() - self.front_delete_count;
        let start = self.stored_indices.end - stored_remainder;
        if count <= stored_remainder {
            values.extend(self.context.read(start..(start + count)).await?);
        } else {
            values.extend(self.context.read(start..self.stored_indices.end).await?);
            values.extend(
                self.new_back_values
                    .range(0..(count - stored_remainder))
                    .cloned(),
            );
        }
        Ok(values)
    }

    /// Read the `count` last values in the queue (including staged ones).
    pub async fn read_back(&mut self, mut count: usize) -> Result<Vec<T>, C::Error> {
        if count > self.count() {
            count = self.count();
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut values = Vec::new();
        values.reserve(count);
        let new_back_len = self.new_back_values.len();
        if count <= new_back_len {
            values.extend(
                self.new_back_values
                    .range((new_back_len - count)..new_back_len)
                    .cloned(),
            );
        } else {
            let start = self.stored_indices.end + new_back_len - count;
            values.extend(self.context.read(start..self.stored_indices.end).await?);
            values.extend(self.new_back_values.iter().cloned());
        }
        Ok(values)
    }
}

/// A view that supports accessing a collection of views of the same kind, indexed by a
/// key.
#[derive(Debug, Clone)]
pub struct CollectionView<C, I, W> {
    context: C,
    updates: BTreeMap<I, Option<W>>,
}

/// The context operations supporting [`CollectionView`].
#[async_trait]
pub trait CollectionOperations<I>: Context {
    /// Clone the context and advance the (otherwise implicit) base key for all read/write
    /// operations.
    fn clone_with_scope(&self, index: &I) -> Self;

    /// Return the list of indices in the collection.
    async fn indices(&mut self) -> Result<Vec<I>, Self::Error>;

    /// Add the index to the list of indices. Crash-resistant implementations should only write
    /// to `batch`.
    async fn add_index(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error>;

    /// Remove the index from the list of indices. Crash-resistant implementations should only
    /// write to `batch`.
    async fn remove_index(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error>;
}

#[async_trait]
impl<C, I, W> View<C> for CollectionView<C, I, W>
where
    C: CollectionOperations<I> + Send,
    I: Send + Sync + Debug + Clone,
    W: View<C> + Send,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, C::Error> {
        Ok(Self {
            context,
            updates: BTreeMap::new(),
        })
    }

    fn rollback(&mut self) {
        self.updates.clear();
    }

    async fn commit_and_reset(&mut self, batch: &mut C::Batch) -> Result<(), C::Error> {
        for (index, update) in mem::take(&mut self.updates) {
            match update {
                Some(view) => {
                    view.commit(batch).await?;
                    self.context.add_index(batch, index).await?;
                }
                None => {
                    let context = self.context.clone_with_scope(&index);
                    self.context.remove_index(batch, index).await?;
                    let view = W::load(context).await?;
                    view.delete(batch).await?;
                }
            }
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut C::Batch) -> Result<(), C::Error> {
        for index in self.context.indices().await? {
            let context = self.context.clone_with_scope(&index);
            self.context.remove_index(batch, index).await?;
            let view = W::load(context).await?;
            view.delete(batch).await?;
        }
        Ok(())
    }
}

impl<C, I, W> CollectionView<C, I, W>
where
    C: CollectionOperations<I> + Send,
    I: Eq + Ord + Sync + Clone + Send + Debug,
    W: View<C>,
{
    /// Obtain a subview for the data at the given index in the collection. Return an
    /// error if `remove_entry` was used earlier on this index from the same [`CollectionView`].
    pub async fn load_entry(&mut self, index: I) -> Result<CollectionEntry<'_, I, W>, C::Error> {
        let entry = match self.updates.entry(index.clone()) {
            btree_map::Entry::Occupied(e) => match e.into_mut() {
                Some(view) => Ok(view),
                None => Err(C::Error::from(ViewError::RemovedEntry(format!(
                    "{:?}",
                    index
                )))),
            },
            btree_map::Entry::Vacant(e) => {
                let context = self.context.clone_with_scope(&index);
                let view = W::load(context).await?;
                Ok(e.insert(Some(view)).as_mut().unwrap())
            }
        }?;

        Ok(CollectionEntry { index, entry })
    }

    /// Mark the entry so that it is removed in the next commit.
    pub fn remove_entry(&mut self, index: I) {
        self.updates.insert(index, None);
    }

    /// Return the list of indices in the collection.
    pub async fn indices(&mut self) -> Result<Vec<I>, C::Error> {
        let mut indices = Vec::new();
        for index in self.context.indices().await? {
            if !self.updates.contains_key(&index) {
                indices.push(index);
            }
        }
        for (index, entry) in &self.updates {
            if entry.is_some() {
                indices.push(index.clone());
            }
        }
        indices.sort();
        Ok(indices)
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

/// An active entry inside a [`CollectionView`].
pub struct CollectionEntry<'entry, Index, Entry> {
    index: Index,
    entry: &'entry mut Entry,
}

impl<Index, Entry> CollectionEntry<'_, Index, Entry> {
    /// The index of this entry.
    pub fn index(&self) -> &Index {
        &self.index
    }
}

impl<'entry, Index, Entry> Deref for CollectionEntry<'entry, Index, Entry> {
    type Target = Entry;

    fn deref(&self) -> &Self::Target {
        &*self.entry
    }
}

impl<'entry, Index, Entry> DerefMut for CollectionEntry<'entry, Index, Entry> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.entry
    }
}

/// A view that supports accessing a collection of views of the same kind, indexed by a key, in a
/// concurrent manner.
///
/// This is a variant of [`CollectionView`] that uses locks to guard each entry.
#[derive(Debug, Clone)]
pub struct SharedCollectionView<C, I, W> {
    context: C,
    updates: BTreeMap<I, Arc<Mutex<Option<W>>>>,
}

#[async_trait]
impl<C, I, W> View<C> for SharedCollectionView<C, I, W>
where
    C: CollectionOperations<I> + Send,
    I: Send + Sync + Debug + Clone + Ord,
    W: View<C> + Send,
{
    async fn load(context: C) -> Result<Self, C::Error> {
        Ok(Self {
            context,
            updates: BTreeMap::new(),
        })
    }

    fn rollback(&mut self) {
        // TODO: Maybe make `View::rollback` async?
        self.try_rollback()
            .now_or_never()
            .expect("Attempt to rollback a `SharedCollectionView` while an entry was still locked");
    }

    async fn commit(mut self) -> Result<(), C::Error> {
        for (index, update) in self.updates {
            match update.lock().await.take() {
                Some(view) => {
                    view.commit().await?;
                    self.context.add_index(index).await?;
                }
                None => {
                    let context = self.context.clone_with_scope(&index);
                    self.context.remove_index(index).await?;
                    let view = W::load(context).await?;
                    view.delete().await?;
                }
            }
        }
        Ok(())
    }

    async fn delete(mut self) -> Result<(), C::Error> {
        for index in self.context.indices().await? {
            let _guard = match self.updates.get(&index) {
                Some(entry) => Some(entry.lock().await),
                None => None,
            };
            let context = self.context.clone_with_scope(&index);
            self.context.remove_index(index).await?;
            let view = W::load(context).await?;
            view.delete().await?;
        }
        Ok(())
    }
}

impl<C, I, W> SharedCollectionView<C, I, W>
where
    C: CollectionOperations<I> + Send,
    I: Eq + Ord + Sync + Clone + Send + Debug,
    W: View<C>,
{
    /// Obtain a subview for the data at the given index in the collection. Return an
    /// error if `remove_entry` was used earlier on this index from the same [`CollectionView`].
    ///
    /// Note that the returned [`SharedCollectionEntry`] holds a lock on the entry, which means
    /// that subsequent calls to methods that access the same entry (or all entries, like `commit`,
    /// `rollback` and `delete`) won't complete until the lock is dropped.
    pub async fn load_entry(&mut self, index: I) -> Result<SharedCollectionEntry<I, W>, C::Error> {
        let entry = match self.updates.entry(index.clone()) {
            btree_map::Entry::Occupied(entry) => entry.into_mut(),
            btree_map::Entry::Vacant(entry) => {
                let context = self.context.clone_with_scope(&index);
                let view = W::load(context).await?;
                entry.insert(Arc::new(Mutex::new(Some(view))))
            }
        }
        .clone()
        .lock_owned()
        .await;

        ensure!(
            entry.is_some(),
            C::Error::from(ViewError::RemovedEntry(format!("{:?}", index)))
        );

        Ok(SharedCollectionEntry { index, entry })
    }

    /// Mark the entry so that it is removed in the next commit.
    ///
    /// If the entry is being editted, awaits until its lock is released.
    pub async fn remove_entry(&mut self, index: I) {
        match self.updates.entry(index) {
            btree_map::Entry::Occupied(entry) => {
                entry.get().lock().await.take();
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(Arc::new(Mutex::new(None)));
            }
        }
    }

    /// Return the list of indices in the collection.
    ///
    /// If any entry is being editted, awaits until all entry locks are released.
    pub async fn indices(&mut self) -> Result<Vec<I>, C::Error> {
        let mut indices = Vec::new();
        for index in self.context.indices().await? {
            if !self.updates.contains_key(&index) {
                indices.push(index);
            }
        }
        for (index, entry) in &self.updates {
            if entry.lock().await.is_some() {
                indices.push(index.clone());
            }
        }
        indices.sort();
        Ok(indices)
    }

    /// Restore the view to its initial state.
    ///
    /// If any entry is being editted, awaits until all entry locks are released.
    pub async fn try_rollback(&mut self) {
        for (_, update) in mem::take(&mut self.updates) {
            update.lock().await;
        }
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

/// An active entry inside a [`SharedCollectionView`].
pub struct SharedCollectionEntry<Index, Entry> {
    index: Index,
    entry: OwnedMutexGuard<Option<Entry>>,
}

impl<Index, Entry> SharedCollectionEntry<Index, Entry> {
    /// The index of this entry.
    pub fn index(&self) -> &Index {
        &self.index
    }
}

impl<Index, Entry> Deref for SharedCollectionEntry<Index, Entry> {
    type Target = Entry;

    fn deref(&self) -> &Self::Target {
        self.entry
            .as_ref()
            .expect("Created a SharedCollectionEntry for a removed entry")
    }
}

impl<Index, Entry> DerefMut for SharedCollectionEntry<Index, Entry> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.entry
            .as_mut()
            .expect("Created a SharedCollectionEntry for a removed entry")
    }
}
