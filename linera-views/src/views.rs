// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::common::Batch;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    cmp::Eq,
    collections::{btree_map, BTreeMap, VecDeque},
    fmt::Debug,
    mem,
    ops::Range,
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
    /// User provided data to be carried along.
    type Extra: Clone + Send + Sync;

    /// The error type in use by internal operations.
    /// In practice, we always want `ViewError: From<Self::Error>` here.
    type Error: std::error::Error + Debug + Send + Sync + 'static + std::convert::From<bcs::Error>;

    /// Getter for the user provided data.
    fn extra(&self) -> &Self::Extra;

    /// Getter for the address of the current entry (aka the base_key)
    fn base_key(&self) -> Vec<u8>;

    /// Obtain the Vec<u8> key from the key by serialization and using the base_key
    fn derive_key<I: Serialize>(&self, index: &I) -> Result<Vec<u8>, Self::Error>;

    /// Retrieve a generic `Item` from the table using the provided `key` prefixed by the current
    /// context.
    /// The `Item` is deserialized using [`bcs`].
    async fn read_key<Item: DeserializeOwned>(
        &mut self,
        key: &[u8],
    ) -> Result<Option<Item>, Self::Error>;

    /// Find keys matching the prefix. The full keys are returned, that is including the prefix.
    async fn find_keys_with_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error>;

    /// Find the keys matching the prefix. The remainder of the key are parsed back into elements.
    async fn get_sub_keys<Key: DeserializeOwned + Send>(
        &mut self,
        key_prefix: &[u8],
    ) -> Result<Vec<Key>, Self::Error>;

    /// Apply the operations from the `batch`, persisting the changes.
    async fn write_batch(&self, batch: Batch) -> Result<(), ViewError>;

    fn clone_self(&self, base_key: Vec<u8>) -> Self;
}

/// A view gives an exclusive access to read and write the data stored at an underlying
/// address in storage.
#[async_trait]
pub trait View<C: Context>: Sized {
    /// Obtain a mutable reference to the internal context.
    fn context(&self) -> &C;

    /// Create a view or a subview.
    async fn load(context: C) -> Result<Self, ViewError>;

    /// Discard all pending changes. After that `flush` should have no effect to storage.
    fn rollback(&mut self);

    /// Clear the view. That can be seen as resetting to default. In the case of a RegisterView
    /// this means setting the value to T::default(). For LogView, QueueView, this leaves
    /// the range data to be left in the database.
    fn clear(&mut self);

    /// Persist changes to storage. This leaves the view still usable and is essentially neutral to the
    /// program running. Crash-resistant storage implementations are expected to accumulate the desired
    /// changes in the `batch` variable first. If the view is dropped without calling `flush`, staged
    /// changes are simply lost.
    async fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError>;

    /// Instead of persisting changes, clear all the data that belong to this view and its
    /// subviews. Crash-resistant storage implementations are expected to accumulate the
    /// desired changes into the `batch` variable first.
    /// No data/metadata at all is left after delete. The view is consumed by delete
    /// and cannot be used in any way after delete.
    async fn delete(self, batch: &mut Batch) -> Result<(), ViewError>;
}

#[derive(Error, Debug)]
pub enum ViewError {
    #[error("the entry with key {0} was removed thus cannot be loaded any more")]
    RemovedEntry(String),

    #[error("failed to serialize value to calculate its hash")]
    Serialization(#[from] bcs::Error),

    #[error(
        "trying to flush or delete a collection view while some entries are still being accessed"
    )]
    CannotAcquireCollectionEntry,

    #[error("IO error")]
    Io(#[from] std::io::Error),

    #[error("Failed to lock collection entry: {0}")]
    TryLockError(#[from] tokio::sync::TryLockError),

    #[error("Panic in sub-task: {0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

    #[error("Storage operation error in {backend}: {error}")]
    ContextError { backend: String, error: String },

    /// FIXME(#148): This belongs to a future `linera_storage::StoreError`.
    #[error("Entry does not exist in memory: {0}")]
    NotFound(String),
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

impl<C: Context> ScopedOperations for C {
    fn clone_with_scope(&self, index: u64) -> Self {
        self.clone_self(self.derive_key(&index).expect("derive_key should not fail"))
    }
}

#[async_trait]
impl<C, W, const INDEX: u64> View<C> for ScopedView<INDEX, W>
where
    C: Context + Send + Sync + ScopedOperations + 'static,
    ViewError: From<C::Error>,
    W: View<C> + Send,
{
    fn context(&self) -> &C {
        self.view.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let view = W::load(context.clone_with_scope(INDEX)).await?;
        Ok(Self { view })
    }

    fn rollback(&mut self) {
        self.view.rollback();
    }

    async fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.view.flush(batch).await
    }

    async fn delete(self, batch: &mut Batch) -> Result<(), ViewError> {
        self.view.delete(batch).await
    }

    fn clear(&mut self) {
        self.view.clear();
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
    fn set(&mut self, batch: &mut Batch, value: &T) -> Result<(), Self::Error>;

    /// Delete the register. Crash-resistant implementations should only write to `batch`.
    fn delete(&mut self, batch: &mut Batch) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T, C: Context + Send> RegisterOperations<T> for C
where
    T: Default + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn get(&mut self) -> Result<T, Self::Error> {
        let base = self.base_key();
        let value = self.read_key(&base).await?.unwrap_or_default();
        Ok(value)
    }

    fn set(&mut self, batch: &mut Batch, value: &T) -> Result<(), Self::Error> {
        batch.put_key_value(self.base_key(), value)?;
        Ok(())
    }

    fn delete(&mut self, batch: &mut Batch) -> Result<(), Self::Error> {
        batch.delete_key(self.base_key());
        Ok(())
    }
}

#[async_trait]
impl<C, T> View<C> for RegisterView<C, T>
where
    C: RegisterOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Default,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(mut context: C) -> Result<Self, ViewError> {
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

    async fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if let Some(value) = self.update.take() {
            self.context.set(batch, &value)?;
            self.stored_value = value;
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.context.delete(batch)?;
        Ok(())
    }

    fn clear(&mut self) {
        self.update = Some(T::default())
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
pub struct LogView<C, T> {
    context: C,
    was_reset_to_default: bool,
    stored_count: usize,
    new_values: Vec<T>,
}

/// The context operations supporting [`LogView`].
#[async_trait]
pub trait LogOperations<T>: Context {
    /// Return the size of the log in storage.
    async fn count(&mut self) -> Result<usize, Self::Error>;

    /// Obtain the value at the given index.
    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error>;

    /// Obtain the values in the given range of indices.
    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, Self::Error>;

    /// Append values to the logs. Crash-resistant implementations should only write to `batch`.
    fn append(
        &mut self,
        stored_count: usize,
        batch: &mut Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error>;

    /// Delete the log. Crash-resistant implementations should only write to `batch`.
    /// The stored_count is an invariant of the structure. It is a leaky abstraction
    /// but allows a priori better performance.
    fn delete(&mut self, stored_count: usize, batch: &mut Batch) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T, C: Context + Send> LogOperations<T> for C
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn count(&mut self) -> Result<usize, Self::Error> {
        let base = self.base_key();
        let count = self.read_key(&base).await?.unwrap_or_default();
        Ok(count)
    }

    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error> {
        let key = self.derive_key(&index)?;
        self.read_key(&key).await
    }

    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, Self::Error> {
        let mut values = Vec::with_capacity(range.len());
        for index in range {
            let key = self.derive_key(&index)?;
            match self.read_key(&key).await? {
                None => return Ok(values),
                Some(value) => values.push(value),
            };
        }
        Ok(values)
    }

    fn append(
        &mut self,
        stored_count: usize,
        batch: &mut Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error> {
        if values.is_empty() {
            return Ok(());
        }
        let mut count = stored_count;
        for value in values {
            batch.put_key_value(self.derive_key(&count)?, &value)?;
            count += 1;
        }
        batch.put_key_value(self.base_key(), &count)?;
        Ok(())
    }

    fn delete(&mut self, stored_count: usize, batch: &mut Batch) -> Result<(), Self::Error> {
        batch.delete_key(self.base_key());
        for index in 0..stored_count {
            batch.delete_key(self.derive_key(&index)?);
        }
        Ok(())
    }
}

#[async_trait]
impl<C, T> View<C> for LogView<C, T>
where
    C: LogOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(mut context: C) -> Result<Self, ViewError> {
        let stored_count = context.count().await?;
        Ok(Self {
            context,
            was_reset_to_default: false,
            stored_count,
            new_values: Vec::new(),
        })
    }

    fn rollback(&mut self) {
        self.was_reset_to_default = false;
        self.new_values.clear();
    }

    async fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_reset_to_default {
            self.was_reset_to_default = false;
            if self.stored_count > 0 {
                self.context.delete(self.stored_count, batch)?;
                self.stored_count = 0;
            }
        }
        if !self.new_values.is_empty() {
            let count = self.new_values.len();
            self.context
                .append(self.stored_count, batch, mem::take(&mut self.new_values))?;
            self.stored_count += count;
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.context.delete(self.stored_count, batch)?;
        Ok(())
    }

    fn clear(&mut self) {
        self.was_reset_to_default = true;
        self.new_values.clear();
    }
}

impl<C, T> LogView<C, T>
where
    C: Context,
{
    /// Push a value to the end of the log.
    pub fn push(&mut self, value: T) {
        self.new_values.push(value);
    }

    /// Read the size of the log.
    pub fn count(&self) -> usize {
        if self.was_reset_to_default {
            self.new_values.len()
        } else {
            self.stored_count + self.new_values.len()
        }
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, T> LogView<C, T>
where
    C: LogOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone,
{
    /// Read the logged values in the given range (including staged ones).
    pub async fn get(&mut self, index: usize) -> Result<Option<T>, ViewError> {
        let value = if self.was_reset_to_default {
            self.new_values.get(index).cloned()
        } else if index < self.stored_count {
            self.context.get(index).await?
        } else {
            self.new_values.get(index - self.stored_count).cloned()
        };
        Ok(value)
    }

    /// Read the logged values in the given range (including staged ones).
    pub async fn read(&mut self, mut range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let effective_stored_count = if self.was_reset_to_default {
            0
        } else {
            self.stored_count
        };
        if range.end > self.count() {
            range.end = self.count();
        }
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let mut values = Vec::new();
        values.reserve(range.end - range.start);
        if range.start < effective_stored_count {
            if range.end <= effective_stored_count {
                values.extend(self.context.read(range.start..range.end).await?);
            } else {
                values.extend(
                    self.context
                        .read(range.start..effective_stored_count)
                        .await?,
                );
                values.extend(self.new_values[0..(range.end - effective_stored_count)].to_vec());
            }
        } else {
            values.extend(
                self.new_values
                    [(range.start - effective_stored_count)..(range.end - effective_stored_count)]
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
    was_reset_to_default: bool,
    updates: BTreeMap<I, Option<V>>,
}

/// The context operations supporting [`MapView`].
#[async_trait]
pub trait MapOperations<I, V>: Context {
    /// Obtain the value at the given index, if any.
    async fn get(&mut self, index: &I) -> Result<Option<V>, Self::Error>;

    /// Return the list of indices in the map.
    async fn indices(&mut self) -> Result<Vec<I>, Self::Error>;

    /// Execute a function on each index.
    async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), Self::Error>
    where
        F: FnMut(I) + Send;

    /// Set a value. Crash-resistant implementations should only write to `batch`.
    fn insert(&mut self, batch: &mut Batch, index: I, value: V) -> Result<(), Self::Error>;

    /// Remove the entry at the given index. Crash-resistant implementations should only write
    /// to `batch`.
    fn remove(&mut self, batch: &mut Batch, index: I) -> Result<(), Self::Error>;

    /// Delete the map and its entries from storage. Crash-resistant implementations should only
    /// write to `batch`.
    async fn delete(&mut self, batch: &mut Batch) -> Result<(), Self::Error>;
}

#[async_trait]
impl<I, V, C: Context + Send + Sync> MapOperations<I, V> for C
where
    I: Eq + Ord + Send + Sync + Serialize + DeserializeOwned + Clone + 'static,
    V: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    async fn get(&mut self, index: &I) -> Result<Option<V>, Self::Error> {
        let key = self.derive_key(index)?;
        Ok(self.read_key(&key).await?)
    }

    fn insert(&mut self, batch: &mut Batch, index: I, value: V) -> Result<(), Self::Error> {
        let key = self.derive_key(&index)?;
        batch.put_key_value(key, &value)?;
        Ok(())
    }

    fn remove(&mut self, batch: &mut Batch, index: I) -> Result<(), Self::Error> {
        let key = self.derive_key(&index)?;
        batch.delete_key(key);
        Ok(())
    }

    async fn indices(&mut self) -> Result<Vec<I>, Self::Error> {
        let base = self.base_key();
        self.get_sub_keys(&base).await
    }

    async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), Self::Error>
    where
        F: FnMut(I) + Send,
    {
        let base = self.base_key();
        for index in self.get_sub_keys(&base).await? {
            f(index);
        }
        Ok(())
    }

    async fn delete(&mut self, batch: &mut Batch) -> Result<(), Self::Error> {
        let base = self.base_key();
        for key in self.find_keys_with_prefix(&base).await? {
            batch.delete_key(key);
        }
        Ok(())
    }
}

#[async_trait]
impl<C, I, V> View<C> for MapView<C, I, V>
where
    C: MapOperations<I, V> + Send,
    ViewError: From<C::Error>,
    I: Eq + Ord + Send + Sync + Clone,
    V: Clone + Send + Sync,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Ok(Self {
            context,
            was_reset_to_default: false,
            updates: BTreeMap::new(),
        })
    }

    fn rollback(&mut self) {
        self.was_reset_to_default = false;
        self.updates.clear();
    }

    async fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_reset_to_default {
            self.was_reset_to_default = false;
            self.context.delete(batch).await?;
            for (index, update) in mem::take(&mut self.updates) {
                if let Some(value) = update {
                    self.context.insert(batch, index, value)?;
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                match update {
                    None => self.context.remove(batch, index)?,
                    Some(value) => self.context.insert(batch, index, value)?,
                }
            }
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.context.delete(batch).await?;
        Ok(())
    }

    fn clear(&mut self) {
        self.was_reset_to_default = true;
        self.updates.clear();
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
        if self.was_reset_to_default {
            self.updates.remove(&index);
        } else {
            self.updates.insert(index, None);
        }
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: MapOperations<I, V>,
    ViewError: From<C::Error>,
    I: Eq + Ord + Sync + Clone + Send,
    V: Clone + Sync,
{
    /// Read the value at the given position, if any.
    pub async fn get(&mut self, index: &I) -> Result<Option<V>, ViewError> {
        if let Some(update) = self.updates.get(index) {
            return Ok(update.as_ref().cloned());
        }
        if self.was_reset_to_default {
            return Ok(None);
        }
        Ok(self.context.get(index).await?)
    }

    /// Return the list of indices in the map.
    pub async fn indices(&mut self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        if !self.was_reset_to_default {
            self.context
                .for_each_index(|index: I| {
                    if !self.updates.contains_key(&index) {
                        indices.push(index);
                    }
                })
                .await?;
        }
        for (index, entry) in &self.updates {
            if entry.is_some() {
                indices.push(index.clone());
            }
        }
        indices.sort();
        Ok(indices)
    }

    /// Execute a function on each index.
    pub async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) + Send,
    {
        if !self.was_reset_to_default {
            self.context
                .for_each_index(|index: I| {
                    if !self.updates.contains_key(&index) {
                        f(index);
                    }
                })
                .await?;
        }
        for (index, entry) in &self.updates {
            if entry.is_some() {
                f(index.clone());
            }
        }
        Ok(())
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
    fn delete_front(
        &mut self,
        stored_indices: &mut Range<usize>,
        batch: &mut Batch,
        count: usize,
    ) -> Result<(), Self::Error>;

    /// Append the given values from the back of the queue. Crash-resistant
    /// implementations should only write to `batch`.
    fn append_back(
        &mut self,
        stored_indices: &mut Range<usize>,
        batch: &mut Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error>;

    /// Delete the queue from storage. Crash-resistant implementations should only write to `batch`.
    fn delete(
        &mut self,
        stored_indices: Range<usize>,
        batch: &mut Batch,
    ) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T, C: Context + Send> QueueOperations<T> for C
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn indices(&mut self) -> Result<Range<usize>, Self::Error> {
        let base = self.base_key();
        let range = self.read_key(&base).await?.unwrap_or_default();
        Ok(range)
    }

    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error> {
        let key = self.derive_key(&index)?;
        Ok(self.read_key(&key).await?)
    }

    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, Self::Error> {
        let mut values = Vec::with_capacity(range.len());
        for index in range {
            let key = self.derive_key(&index)?;
            match self.read_key(&key).await? {
                None => return Ok(values),
                Some(value) => values.push(value),
            }
        }
        Ok(values)
    }

    fn delete_front(
        &mut self,
        stored_indices: &mut Range<usize>,
        batch: &mut Batch,
        count: usize,
    ) -> Result<(), Self::Error> {
        if count == 0 {
            return Ok(());
        }
        let deletion_range = stored_indices.clone().take(count);
        stored_indices.start += count;
        batch.put_key_value(self.base_key(), &stored_indices)?;
        for index in deletion_range {
            let key = self.derive_key(&index)?;
            batch.delete_key(key);
        }
        Ok(())
    }

    fn append_back(
        &mut self,
        stored_indices: &mut Range<usize>,
        batch: &mut Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error> {
        if values.is_empty() {
            return Ok(());
        }
        for value in values {
            let key = self.derive_key(&stored_indices.end)?;
            batch.put_key_value(key, &value)?;
            stored_indices.end += 1;
        }
        let base = self.base_key();
        batch.put_key_value(base, &stored_indices)?;
        Ok(())
    }

    fn delete(
        &mut self,
        stored_indices: Range<usize>,
        batch: &mut Batch,
    ) -> Result<(), Self::Error> {
        let base = self.base_key();
        batch.delete_key(base);
        for index in stored_indices {
            let key = self.derive_key(&index)?;
            batch.delete_key(key);
        }
        Ok(())
    }
}

#[async_trait]
impl<C, T> View<C> for QueueView<C, T>
where
    C: QueueOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(mut context: C) -> Result<Self, ViewError> {
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

    async fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.front_delete_count > 0 {
            self.context
                .delete_front(&mut self.stored_indices, batch, self.front_delete_count)?;
        }
        if !self.new_back_values.is_empty() {
            self.context.append_back(
                &mut self.stored_indices,
                batch,
                mem::take(&mut self.new_back_values).into_iter().collect(),
            )?;
        }
        self.front_delete_count = 0;
        Ok(())
    }

    async fn delete(mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.context.delete(self.stored_indices, batch)?;
        Ok(())
    }

    fn clear(&mut self) {
        self.front_delete_count = self.stored_indices.len();
        self.new_back_values.clear();
    }
}

impl<C, T> QueueView<C, T>
where
    C: QueueOperations<T> + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone,
{
    /// Read the front value, if any.
    pub async fn front(&mut self) -> Result<Option<T>, ViewError> {
        let stored_remainder = self.stored_indices.len() - self.front_delete_count;
        let value = if stored_remainder > 0 {
            self.context
                .get(self.stored_indices.end - stored_remainder)
                .await?
        } else {
            self.new_back_values.front().cloned()
        };
        Ok(value)
    }

    /// Read the back value, if any.
    pub async fn back(&mut self) -> Result<Option<T>, ViewError> {
        let value = match self.new_back_values.back() {
            Some(value) => Some(value.clone()),
            None if self.stored_indices.len() > self.front_delete_count => {
                self.context.get(self.stored_indices.end - 1).await?
            }
            _ => None,
        };
        Ok(value)
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
    pub async fn read_front(&mut self, mut count: usize) -> Result<Vec<T>, ViewError> {
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
    pub async fn read_back(&mut self, mut count: usize) -> Result<Vec<T>, ViewError> {
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
/// key, one subview at a time.
#[derive(Debug, Clone)]
pub struct CollectionView<C, I, W> {
    context: C,
    was_reset_to_default: bool,
    updates: BTreeMap<I, Option<W>>,
}

/// A view that supports accessing a collection of views of the same kind, indexed by a
/// key, possibly several subviews at a time.
#[derive(Debug)]
pub struct ReentrantCollectionView<C, I, W> {
    context: C,
    was_reset_to_default: bool,
    updates: BTreeMap<I, Option<Arc<Mutex<W>>>>,
}

/// The context operations supporting [`CollectionView`].
#[async_trait]
pub trait CollectionOperations<I>: Context {
    /// Clone the context and advance the (otherwise implicit) base key for all read/write
    /// operations.
    fn clone_with_scope(&self, index: &I) -> Self;

    /// Return the list of indices in the collection.
    async fn indices(&mut self) -> Result<Vec<I>, Self::Error>;

    /// Execute a function on each index.
    async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), Self::Error>
    where
        F: FnMut(I) + Send;

    /// Add the index to the list of indices. Crash-resistant implementations should only write
    /// to `batch`.
    fn add_index(&mut self, batch: &mut Batch, index: I) -> Result<(), Self::Error>;

    /// Remove the index from the list of indices. Crash-resistant implementations should only
    /// write to `batch`.
    fn remove_index(&mut self, batch: &mut Batch, index: I) -> Result<(), Self::Error>;

    // TODO(#149): In contrast to other views, there is no delete operation for CollectionOperation.
}

/// A marker type used to distinguish keys from the current scope from the keys of sub-views.
///
/// Sub-views in a collection share a common key prefix, like in other view types. However,
/// just concatenating the shared prefix with sub-view keys makes it impossible to distinguish if a
/// given key belongs to child sub-view or a grandchild sub-view (consider for example if a
/// collection is stored inside the collection).
///
/// The solution to this is to use a marker type to have two sets of keys, where
/// [`CollectionKey::Index`] serves to indicate the existence of an entry in the collection, and
/// [`CollectionKey::Subvie`] serves as the prefix for the sub-view.
#[derive(Serialize)]
enum CollectionKey<I> {
    Index(I),
    Subview(I),
}

#[async_trait]
impl<I, C: Context + Send> CollectionOperations<I> for C
where
    I: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn clone_with_scope(&self, index: &I) -> Self {
        let key = self
            .derive_key(&CollectionKey::Subview(index))
            .expect("derive_key should not fail");
        self.clone_self(key)
    }

    fn add_index(&mut self, batch: &mut Batch, index: I) -> Result<(), Self::Error> {
        let key = self.derive_key(&CollectionKey::Index(index))?;
        batch.put_key_value(key, &())?;
        Ok(())
    }

    fn remove_index(&mut self, batch: &mut Batch, index: I) -> Result<(), Self::Error> {
        let key = self.derive_key(&CollectionKey::Index(index))?;
        batch.delete_key(key);
        Ok(())
    }

    async fn indices(&mut self) -> Result<Vec<I>, Self::Error> {
        let base = self.derive_key(&CollectionKey::Index(()))?;
        self.get_sub_keys(&base).await
    }

    async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), Self::Error>
    where
        F: FnMut(I) + Send,
    {
        let base = self.derive_key(&CollectionKey::Index(()))?;
        for index in self.get_sub_keys(&base).await? {
            f(index);
        }
        Ok(())
    }
}

#[async_trait]
impl<C, I, W> View<C> for CollectionView<C, I, W>
where
    C: CollectionOperations<I> + Send,
    ViewError: From<C::Error>,
    I: Send + Ord + Sync + Debug + Clone,
    W: View<C> + Send,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Ok(Self {
            context,
            was_reset_to_default: false,
            updates: BTreeMap::new(),
        })
    }

    fn rollback(&mut self) {
        self.was_reset_to_default = false;
        self.updates.clear();
    }

    async fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_reset_to_default {
            self.was_reset_to_default = false;
            self.delete_entries(batch).await?;
            for (index, update) in mem::take(&mut self.updates) {
                if let Some(mut view) = update {
                    view.flush(batch).await?;
                    self.context.add_index(batch, index)?;
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                match update {
                    Some(mut view) => {
                        view.flush(batch).await?;
                        self.context.add_index(batch, index)?;
                    }
                    None => {
                        let context = self.context.clone_with_scope(&index);
                        self.context.remove_index(batch, index)?;
                        let view = W::load(context).await?;
                        view.delete(batch).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.delete_entries(batch).await
    }

    fn clear(&mut self) {
        self.was_reset_to_default = true;
        self.updates.clear();
    }
}

impl<C, I, W> CollectionView<C, I, W>
where
    C: CollectionOperations<I> + Send,
    ViewError: From<C::Error>,
    I: Eq + Ord + Sync + Clone + Send + Debug,
    W: View<C>,
{
    /// Delete all entries in this [`Collection`].
    async fn delete_entries(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        let stored_indices = self.context.indices().await?;
        for index in &stored_indices {
            let context = self.context.clone_with_scope(index);
            self.context.remove_index(batch, index.clone())?;
            let view = W::load(context).await?;
            view.delete(batch).await?;
        }
        Ok(())
    }

    /// Obtain a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    pub async fn load_entry(&mut self, index: I) -> Result<&mut W, ViewError> {
        match self.updates.entry(index.clone()) {
            btree_map::Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    Some(view) => Ok(view),
                    None => {
                        let context = self.context.clone_with_scope(&index);
                        // Obtain a view and set its pending state to the default (e.g. empty) state
                        let mut view = W::load(context).await?;
                        view.clear();
                        *entry = Some(view);
                        Ok(entry.as_mut().unwrap())
                    }
                }
            }
            btree_map::Entry::Vacant(entry) => {
                let context = self.context.clone_with_scope(&index);
                let mut view = W::load(context).await?;
                if self.was_reset_to_default {
                    view.clear();
                }
                Ok(entry.insert(Some(view)).as_mut().unwrap())
            }
        }
    }

    /// Mark the entry so that it is removed in the next flush
    pub fn remove_entry(&mut self, index: I) {
        if self.was_reset_to_default {
            self.updates.remove(&index);
        } else {
            self.updates.insert(index, None);
        }
    }

    /// Mark the entry so that it is removed in the next flush
    pub async fn reset_entry_to_default(&mut self, index: I) -> Result<(), ViewError> {
        let view = self.load_entry(index).await?;
        view.clear();
        Ok(())
    }

    /// Return the list of indices in the collection.
    pub async fn indices(&mut self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        if !self.was_reset_to_default {
            for index in self.context.indices().await? {
                if !self.updates.contains_key(&index) {
                    indices.push(index);
                }
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

impl<C, I, W> CollectionView<C, I, W>
where
    C: CollectionOperations<I> + Send,
    ViewError: From<C::Error>,
    I: Eq + Ord + Clone + Debug + Sync,
    W: View<C> + Sync,
{
    /// Execute a function on each index.
    pub async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) + Send,
    {
        if !self.was_reset_to_default {
            self.context
                .for_each_index(|index: I| {
                    if !self.updates.contains_key(&index) {
                        f(index);
                    }
                })
                .await?;
        }
        for (index, entry) in &self.updates {
            if entry.is_some() {
                f(index.clone());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<C, I, W> View<C> for ReentrantCollectionView<C, I, W>
where
    C: CollectionOperations<I> + Send,
    ViewError: From<C::Error>,
    I: Send + Ord + Sync + Debug + Clone,
    W: View<C> + Send,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Ok(Self {
            context,
            was_reset_to_default: false,
            updates: BTreeMap::new(),
        })
    }

    fn rollback(&mut self) {
        self.was_reset_to_default = false;
        self.updates.clear();
    }

    async fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_reset_to_default {
            self.was_reset_to_default = false;
            self.delete_entries(batch).await?;
            for (index, update) in mem::take(&mut self.updates) {
                if let Some(view) = update {
                    let mut view = Arc::try_unwrap(view)
                        .map_err(|_| ViewError::CannotAcquireCollectionEntry)?
                        .into_inner();
                    view.flush(batch).await?;
                    self.context.add_index(batch, index)?;
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                match update {
                    Some(view) => {
                        let mut view = Arc::try_unwrap(view)
                            .map_err(|_| ViewError::CannotAcquireCollectionEntry)?
                            .into_inner();
                        view.flush(batch).await?;
                        self.context.add_index(batch, index)?;
                    }
                    None => {
                        let context = self.context.clone_with_scope(&index);
                        self.context.remove_index(batch, index)?;
                        let view = W::load(context).await?;
                        view.delete(batch).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.delete_entries(batch).await
    }

    fn clear(&mut self) {
        self.was_reset_to_default = true;
        self.updates.clear();
    }
}

impl<C, I, W> ReentrantCollectionView<C, I, W>
where
    C: CollectionOperations<I> + Send,
    ViewError: From<C::Error>,
    I: Eq + Ord + Sync + Clone + Send + Debug,
    W: View<C>,
{
    /// Delete all entries in this [`Collection`].
    async fn delete_entries(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        let stored_indices = self.context.indices().await?;
        for index in &stored_indices {
            let context = self.context.clone_with_scope(index);
            self.context.remove_index(batch, index.clone())?;
            let view = W::load(context).await?;
            view.delete(batch).await?;
        }
        Ok(())
    }

    /// Obtain a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    pub async fn try_load_entry(&mut self, index: I) -> Result<OwnedMutexGuard<W>, ViewError> {
        match self.updates.entry(index.clone()) {
            btree_map::Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    Some(view) => Ok(view.clone().try_lock_owned()?),
                    None => {
                        let context = self.context.clone_with_scope(&index);
                        // Obtain a view and set its pending state to the default (e.g. empty) state
                        let mut view = W::load(context).await?;
                        view.clear();
                        let wrapped_view = Arc::new(Mutex::new(view));
                        *entry = Some(wrapped_view.clone());
                        Ok(wrapped_view.try_lock_owned()?)
                    }
                }
            }
            btree_map::Entry::Vacant(entry) => {
                let context = self.context.clone_with_scope(&index);
                let mut view = W::load(context).await?;
                if self.was_reset_to_default {
                    view.clear();
                }
                let wrapped_view = Arc::new(Mutex::new(view));
                entry.insert(Some(wrapped_view.clone()));
                Ok(wrapped_view.try_lock_owned()?)
            }
        }
    }

    /// Mark the entry so that it is removed in the next flush
    pub fn remove_entry(&mut self, index: I) {
        if self.was_reset_to_default {
            self.updates.remove(&index);
        } else {
            self.updates.insert(index, None);
        }
    }

    /// Mark the entry so that it is removed in the next flush
    pub async fn try_reset_entry_to_default(&mut self, index: I) -> Result<(), ViewError> {
        let mut view = self.try_load_entry(index).await?;
        view.clear();
        Ok(())
    }

    /// Return the list of indices in the collection.
    pub async fn indices(&mut self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        if !self.was_reset_to_default {
            for index in self.context.indices().await? {
                if !self.updates.contains_key(&index) {
                    indices.push(index);
                }
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

impl<C, I, W> ReentrantCollectionView<C, I, W>
where
    C: CollectionOperations<I> + Send,
    ViewError: From<C::Error>,
    I: Eq + Ord + Clone + Debug + Send + Sync,
    W: View<C> + Send + Sync,
{
    /// Execute a function on each index.
    pub async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) + Send + Sync,
    {
        if !self.was_reset_to_default {
            self.context
                .for_each_index(|index: I| {
                    if !self.updates.contains_key(&index) {
                        f(index);
                    }
                })
                .await?;
        }
        for (index, entry) in &self.updates {
            if entry.is_some() {
                f(index.clone());
            }
        }
        Ok(())
    }
}
