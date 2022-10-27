// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
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
use serde::{Serialize, de::DeserializeOwned};

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

    /// The error type in use by internal operations.
    /// In practice, we always want `ViewError: Self::Error` here.
    type Error: std::error::Error + Debug + Send + Sync + 'static;

    /// Getter for the user provided data.
    fn extra(&self) -> &Self::Extra;

    /// Get the used base_key
    fn get_base_key(&self) -> Vec<u8>;

    /// Obtain the Vec<u8> key from the key by serialization and using the base_key
    fn derive_key<I: Serialize>(&self, index: &I) -> Vec<u8>;

    /// Insert a put a key/value in the batch
    fn put_item_batch(
        &self,
        batch: &mut Self::Batch,
        key: Vec<u8>,
        value: &impl Serialize,
    ) -> Result<(), Self::Error>;

    /// Delete a key and put that command into the batch
    fn remove_item_batch(&self, batch: &mut Self::Batch, key: Vec<u8>);

    /// Retrieve a generic `Item` from the table using the provided `key` prefixed by the current
    /// context.
    /// The `Item` is deserialized using [`bcs`].
    async fn read_key<Item: DeserializeOwned>(&mut self, key: &Vec<u8>) -> Result<Option<Item>, Self::Error>;

    /// Find keys matching the prefix. The full keys are returned, that is including the prefix.
    async fn find_keys_with_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, Self::Error>;

    /// Find the keys matching the prefix. The remainder of the key are parsed back into elements.
    async fn get_sub_keys<Key: DeserializeOwned + Send>(
        &mut self,
        key_prefix: &Vec<u8>,
    ) -> Result<Vec<Key>, Self::Error>;

    /// Provide a reference to a new batch to the builder then execute the batch.
    async fn run_with_batch<F>(&self, builder: F) -> Result<(), ViewError>
    where
        F: FnOnce(&mut Self::Batch) -> futures::future::BoxFuture<Result<(), ViewError>>
            + Send
            + Sync;

    /// Create a new [`Self::Batch`] to collect write operations.
    fn create_batch(&self) -> Self::Batch;

    /// Apply the operations from the `batch`, persisting the changes.
    async fn write_batch(&self, batch: Self::Batch) -> Result<(), ViewError>;

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

    /// Discard all pending changes. After that `commit` should have no effect to storage.
    fn rollback(&mut self);

    /// Reset to the default values.
    fn reset_to_default(&mut self);

    /// Persist changes to storage. This leaves the view still usable and is essentially neutral to the
    /// program running. Crash-resistant storage implementations are expected to accumulate the desired
    /// changes in the `batch` variable first. If the view is dropped without calling `commit`, staged
    /// changes are simply lost.
    async fn flush(&mut self, batch: &mut C::Batch) -> Result<(), ViewError>;

    /// Instead of persisting changes, clear all the data that belong to this view and its
    /// subviews. Crash-resistant storage implementations are expected to accumulate the
    /// desired changes into the `batch` variable first.
    async fn delete(self, batch: &mut C::Batch) -> Result<(), ViewError>;
}

#[derive(Error, Debug)]
pub enum ViewError {
    #[error("the entry with key {0} was removed thus cannot be loaded any more")]
    RemovedEntry(String),

    #[error("failed to serialize value to calculate its hash")]
    Serialization(#[from] bcs::Error),

    #[error(
        "trying to commit or delete a collection view while some entries are still being accessed"
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

/// FIXME(#148): This belongs to each future refinements of `linera_base::error::Error`.
impl From<ViewError> for linera_base::error::Error {
    fn from(error: ViewError) -> Self {
        Self::ViewError {
            error: error.to_string(),
        }
    }
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

impl<C: Context> ScopedOperations for C
{
    fn clone_with_scope(&self, index: u64) -> Self {
        self.clone_self(self.derive_key(&index))
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

    async fn flush(&mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
        self.view.flush(batch).await
    }

    async fn delete(self, batch: &mut C::Batch) -> Result<(), ViewError> {
        self.view.delete(batch).await
    }

    fn reset_to_default(&mut self) {
        self.view.reset_to_default();
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
    fn set(&mut self, batch: &mut Self::Batch, value: &T) -> Result<(), Self::Error>;

    /// Delete the register. Crash-resistant implementations should only write to `batch`.
    fn delete(&mut self, batch: &mut Self::Batch) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T, C: Context + Send> RegisterOperations<T> for C
where
    T: Default + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn get(&mut self) -> Result<T, Self::Error> {
        let base = self.get_base_key();
        let value = self
            .read_key(&base)
            .await?
            .unwrap_or_default();
        Ok(value)
    }

    fn set(&mut self, batch: &mut Self::Batch, value: &T) -> Result<(), Self::Error> {
        self.put_item_batch(batch, self.get_base_key(), value)?;
        Ok(())
    }

    fn delete(&mut self, batch: &mut Self::Batch) -> Result<(), Self::Error> {
        self.remove_item_batch(batch, self.get_base_key());
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

    async fn flush(&mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
        if let Some(value) = self.update.take() {
            self.context.set(batch, &value)?;
            self.stored_value = value;
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
        self.context.delete(batch)?;
        Ok(())
    }

    fn reset_to_default(&mut self) {
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
        batch: &mut Self::Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error>;

    /// Delete the log. Crash-resistant implementations should only write to `batch`.
    /// The stored_count is an invariant of the structure. It is a leaky abstraction
    /// but allows a priori better performance.
    fn delete(&mut self, stored_count: usize, batch: &mut Self::Batch) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T, C: Context + Send> LogOperations<T> for C
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn count(&mut self) -> Result<usize, Self::Error> {
        let base = self.get_base_key();
        let count = self
            .read_key(&base)
            .await?
            .unwrap_or_default();
        Ok(count)
    }

    async fn get(&mut self, index: usize) -> Result<Option<T>, Self::Error> {
        let key = self.derive_key(&index);
        self.read_key(&key).await
    }

    async fn read(&mut self, range: Range<usize>) -> Result<Vec<T>, Self::Error> {
        let mut values = Vec::with_capacity(range.len());
        for index in range {
            let key = self.derive_key(&index);
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
        batch: &mut Self::Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error> {
        if values.is_empty() {
            return Ok(());
        }
        let mut count = stored_count;
        for value in values {
            self.put_item_batch(batch, self.derive_key(&count), &value)?;
            count += 1;
        }
        self.put_item_batch(batch, self.get_base_key(), &count)?;
        Ok(())
    }

    fn delete(&mut self, stored_count: usize, batch: &mut Self::Batch) -> Result<(), Self::Error> {
        self.remove_item_batch(batch, self.get_base_key());
        for index in 0..stored_count {
            self.remove_item_batch(batch, self.derive_key(&index));
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

    async fn flush(&mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
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

    async fn delete(mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
        self.context.delete(self.stored_count, batch)?;
        Ok(())
    }

    fn reset_to_default(&mut self) {
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
    fn insert(&mut self, batch: &mut Self::Batch, index: I, value: V) -> Result<(), Self::Error>;

    /// Remove the entry at the given index. Crash-resistant implementations should only write
    /// to `batch`.
    fn remove(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error>;

    /// Delete the map and its entries from storage. Crash-resistant implementations should only
    /// write to `batch`.
    async fn delete(&mut self, batch: &mut Self::Batch) -> Result<(), Self::Error>;
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

    async fn flush(&mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
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

    async fn delete(mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
        self.context.delete(batch).await?;
        Ok(())
    }

    fn reset_to_default(&mut self) {
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
        batch: &mut Self::Batch,
        count: usize,
    ) -> Result<(), Self::Error>;

    /// Append the given values from the back of the queue. Crash-resistant
    /// implementations should only write to `batch`.
    fn append_back(
        &mut self,
        stored_indices: &mut Range<usize>,
        batch: &mut Self::Batch,
        values: Vec<T>,
    ) -> Result<(), Self::Error>;

    /// Delete the queue from storage. Crash-resistant implementations should only write to `batch`.
    fn delete(
        &mut self,
        stored_indices: Range<usize>,
        batch: &mut Self::Batch,
    ) -> Result<(), Self::Error>;
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

    async fn flush(&mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
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

    async fn delete(mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
        self.context.delete(self.stored_indices, batch)?;
        Ok(())
    }

    fn reset_to_default(&mut self) {
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
    fn add_index(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error>;

    /// Remove the index from the list of indices. Crash-resistant implementations should only
    /// write to `batch`.
    fn remove_index(&mut self, batch: &mut Self::Batch, index: I) -> Result<(), Self::Error>;

    // TODO(#149): In contrast to other views, there is no delete operation for CollectionOperation.
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

    async fn flush(&mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
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

    async fn delete(mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
        self.delete_entries(batch).await
    }

    fn reset_to_default(&mut self) {
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
    async fn delete_entries(&mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
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
                        view.reset_to_default();
                        *entry = Some(view);
                        Ok(entry.as_mut().unwrap())
                    }
                }
            }
            btree_map::Entry::Vacant(entry) => {
                let context = self.context.clone_with_scope(&index);
                let mut view = W::load(context).await?;
                if self.was_reset_to_default {
                    view.reset_to_default();
                }
                Ok(entry.insert(Some(view)).as_mut().unwrap())
            }
        }
    }

    /// Mark the entry so that it is removed in the next commit.
    pub fn remove_entry(&mut self, index: I) {
        if self.was_reset_to_default {
            self.updates.remove(&index);
        } else {
            self.updates.insert(index, None);
        }
    }

    /// Mark the entry so that it is removed in the next commit.
    pub async fn reset_entry_to_default(&mut self, index: I) -> Result<(), ViewError> {
        let view = self.load_entry(index).await?;
        view.reset_to_default();
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

    async fn flush(&mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
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

    async fn delete(mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
        self.delete_entries(batch).await
    }

    fn reset_to_default(&mut self) {
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
    async fn delete_entries(&mut self, batch: &mut C::Batch) -> Result<(), ViewError> {
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
                        view.reset_to_default();
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
                    view.reset_to_default();
                }
                let wrapped_view = Arc::new(Mutex::new(view));
                entry.insert(Some(wrapped_view.clone()));
                Ok(wrapped_view.try_lock_owned()?)
            }
        }
    }

    /// Mark the entry so that it is removed in the next commit.
    pub fn remove_entry(&mut self, index: I) {
        if self.was_reset_to_default {
            self.updates.remove(&index);
        } else {
            self.updates.insert(index, None);
        }
    }

    /// Mark the entry so that it is removed in the next commit.
    pub async fn try_reset_entry_to_default(&mut self, index: I) -> Result<(), ViewError> {
        let mut view = self.try_load_entry(index).await?;
        view.reset_to_default();
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
