// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_metrics)]
use std::sync::LazyLock;
use std::{
    borrow::Borrow,
    collections::{btree_map, BTreeMap},
    io::Write,
    marker::PhantomData,
    mem,
    sync::{Arc, Mutex},
};

use async_lock::{RwLock, RwLockReadGuardArc, RwLockWriteGuardArc};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{bucket_latencies, register_histogram_vec, MeasureLatency},
    prometheus::HistogramVec,
};

use crate::{
    batch::Batch,
    common::{CustomSerialize, HasherOutput, Update},
    context::Context,
    hashable_wrapper::WrappedHashableContainerView,
    store::KeyIterable,
    views::{ClonableView, HashableView, Hasher, View, ViewError, MIN_VIEW_TAG},
};

#[cfg(with_metrics)]
/// The runtime of hash computation
static REENTRANT_COLLECTION_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "reentrant_collection_view_hash_runtime",
        "ReentrantCollectionView hash runtime",
        &[],
        bucket_latencies(5.0),
    )
});

/// A read-only accessor for a particular subview in a [`ReentrantCollectionView`].
#[derive(Debug)]
pub struct ReadGuardedView<T>(RwLockReadGuardArc<T>);

impl<T> std::ops::Deref for ReadGuardedView<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.0.deref()
    }
}

/// A read-write accessor for a particular subview in a [`ReentrantCollectionView`].
#[derive(Debug)]
pub struct WriteGuardedView<T>(RwLockWriteGuardArc<T>);

impl<T> std::ops::Deref for WriteGuardedView<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.0.deref()
    }
}

impl<T> std::ops::DerefMut for WriteGuardedView<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.0.deref_mut()
    }
}

/// A view that supports accessing a collection of views of the same kind, indexed by `Vec<u8>`,
/// possibly several subviews at a time.
#[derive(Debug)]
pub struct ReentrantByteCollectionView<C, W> {
    /// The view [`Context`].
    context: C,
    /// If the current persisted data will be completely erased and replaced on the next flush.
    delete_storage_first: bool,
    /// Entries that may have staged changes.
    updates: BTreeMap<Vec<u8>, Update<Arc<RwLock<W>>>>,
    /// Entries cached in memory that have the exact same state as in the persistent storage.
    cached_entries: Mutex<BTreeMap<Vec<u8>, Arc<RwLock<W>>>>,
}

/// We need to find new base keys in order to implement the collection_view.
/// We do this by appending a value to the base_key.
///
/// Sub-views in a collection share a common key prefix, like in other view types. However,
/// just concatenating the shared prefix with sub-view keys makes it impossible to distinguish if a
/// given key belongs to a child sub-view or a grandchild sub-view (consider for example if a
/// collection is stored inside the collection).
#[repr(u8)]
enum KeyTag {
    /// Prefix for specifying an index and serves to indicate the existence of an entry in the collection.
    Index = MIN_VIEW_TAG,
    /// Prefix for specifying as the prefix for the sub-view.
    Subview,
}

#[async_trait]
impl<C, W> View<C> for ReentrantByteCollectionView<C, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    W: View<C> + Send + Sync,
{
    const NUM_INIT_KEYS: usize = 0;

    fn context(&self) -> &C {
        &self.context
    }

    fn pre_load(_context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(Vec::new())
    }

    fn post_load(context: C, _values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        Ok(Self {
            context,
            delete_storage_first: false,
            updates: BTreeMap::new(),
            cached_entries: Mutex::new(BTreeMap::new()),
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Self::post_load(context, &[])
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.updates.clear();
    }

    async fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        !self.updates.is_empty()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            delete_view = true;
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(&mut self.updates) {
                if let Update::Set(view) = update {
                    let mut view = Arc::try_unwrap(view)
                        .map_err(|_| ViewError::CannotAcquireCollectionEntry)?
                        .into_inner();
                    view.flush(batch)?;
                    self.add_index(batch, &index);
                    delete_view = false;
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                match update {
                    Update::Set(view) => {
                        let mut view = Arc::try_unwrap(view)
                            .map_err(|_| ViewError::CannotAcquireCollectionEntry)?
                            .into_inner();
                        view.flush(batch)?;
                        self.add_index(batch, &index);
                    }
                    Update::Removed => {
                        let key_subview = self.get_subview_key(&index);
                        let key_index = self.get_index_key(&index);
                        batch.delete_key(key_index);
                        batch.delete_key_prefix(key_subview);
                    }
                }
            }
        }
        self.delete_storage_first = false;
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.updates.clear();
        self.cached_entries.get_mut().unwrap().clear();
    }
}

impl<C, W> ClonableView<C> for ReentrantByteCollectionView<C, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    W: ClonableView<C> + Send + Sync,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let cloned_updates = self
            .updates
            .iter()
            .map(|(key, value)| {
                let cloned_value = match value {
                    Update::Removed => Update::Removed,
                    Update::Set(view_lock) => {
                        let mut view = view_lock
                            .try_write()
                            .ok_or(ViewError::CannotAcquireCollectionEntry)?;

                        Update::Set(Arc::new(RwLock::new(view.clone_unchecked()?)))
                    }
                };
                Ok((key.clone(), cloned_value))
            })
            .collect::<Result<_, ViewError>>()?;

        Ok(ReentrantByteCollectionView {
            context: self.context.clone(),
            delete_storage_first: self.delete_storage_first,
            updates: cloned_updates,
            cached_entries: Mutex::new(BTreeMap::new()),
        })
    }
}

impl<C: Context, W> ReentrantByteCollectionView<C, W> {
    fn get_index_key(&self, index: &[u8]) -> Vec<u8> {
        self.context.base_tag_index(KeyTag::Index as u8, index)
    }

    fn get_subview_key(&self, index: &[u8]) -> Vec<u8> {
        self.context.base_tag_index(KeyTag::Subview as u8, index)
    }

    fn add_index(&self, batch: &mut Batch, index: &[u8]) {
        let key = self.get_index_key(index);
        batch.put_key_value_bytes(key, vec![]);
    }
}

impl<C, W> ReentrantByteCollectionView<C, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    W: View<C> + Send + Sync,
{
    /// Reads the view and if missing returns the default view
    async fn wrapped_view(
        context: &C,
        delete_storage_first: bool,
        short_key: &[u8],
    ) -> Result<Arc<RwLock<W>>, ViewError> {
        let key = context.base_tag_index(KeyTag::Subview as u8, short_key);
        let context = context.clone_with_base_key(key);
        // Obtain a view and set its pending state to the default (e.g. empty) state
        let view = if delete_storage_first {
            W::new(context)?
        } else {
            W::load(context).await?
        };
        Ok(Arc::new(RwLock::new(view)))
    }

    /// Load the view and insert it into the updates if needed.
    /// If the entry is missing, then it is set to default.
    async fn try_load_view_mut(&mut self, short_key: &[u8]) -> Result<Arc<RwLock<W>>, ViewError> {
        use btree_map::Entry::*;
        Ok(match self.updates.entry(short_key.to_owned()) {
            Occupied(mut entry) => match entry.get_mut() {
                Update::Set(view) => view.clone(),
                entry @ Update::Removed => {
                    let wrapped_view = Self::wrapped_view(&self.context, true, short_key).await?;
                    *entry = Update::Set(wrapped_view.clone());
                    wrapped_view
                }
            },
            Vacant(entry) => {
                let wrapped_view = match self.cached_entries.get_mut().unwrap().remove(short_key) {
                    Some(view) => view,
                    None => {
                        Self::wrapped_view(&self.context, self.delete_storage_first, short_key)
                            .await?
                    }
                };
                entry.insert(Update::Set(wrapped_view.clone()));
                wrapped_view
            }
        })
    }

    /// Load the view from the update is available.
    /// If missing, then the entry is loaded from storage and if
    /// missing there an error is reported.
    async fn try_load_view(&self, short_key: &[u8]) -> Result<Option<Arc<RwLock<W>>>, ViewError> {
        Ok(if let Some(entry) = self.updates.get(short_key) {
            match entry {
                Update::Set(view) => Some(view.clone()),
                _entry @ Update::Removed => None,
            }
        } else if self.delete_storage_first {
            None
        } else {
            let view = {
                let cached_entries = self.cached_entries.lock().unwrap();
                let view = cached_entries.get(short_key);
                view.cloned()
            };
            if let Some(view) = view {
                Some(view.clone())
            } else {
                let key_index = self.context.base_tag_index(KeyTag::Index as u8, short_key);
                if self.context.contains_key(&key_index).await? {
                    let view = Self::wrapped_view(&self.context, false, short_key).await?;
                    let mut cached_entries = self.cached_entries.lock().unwrap();
                    cached_entries.insert(short_key.to_owned(), view.clone());
                    Some(view)
                } else {
                    None
                }
            }
        })
    }

    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then a default entry is added to the collection. The resulting view
    /// can be modified.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantByteCollectionView<_, RegisterView<_, String>> =
    ///     ReentrantByteCollectionView::load(context).await.unwrap();
    /// let subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    /// let value = subview.get();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry_mut(
        &mut self,
        short_key: &[u8],
    ) -> Result<WriteGuardedView<W>, ViewError> {
        Ok(WriteGuardedView(
            self.try_load_view_mut(short_key)
                .await?
                .try_write_arc()
                .ok_or_else(|| ViewError::TryLockError(short_key.to_vec()))?,
        ))
    }

    /// Loads a subview at the given index in the collection and gives read-only access to the data.
    /// If an entry is absent then `None` is returned.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantByteCollectionView<_, RegisterView<_, String>> =
    ///     ReentrantByteCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    /// }
    /// let subview = view.try_load_entry(&[0, 1]).await.unwrap().unwrap();
    /// let value = subview.get();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry(
        &self,
        short_key: &[u8],
    ) -> Result<Option<ReadGuardedView<W>>, ViewError> {
        match self.try_load_view(short_key).await? {
            None => Ok(None),
            Some(view) => Ok(Some(ReadGuardedView(
                view.try_read_arc()
                    .ok_or_else(|| ViewError::TryLockError(short_key.to_vec()))?,
            ))),
        }
    }

    /// Returns `true` if the collection contains a value for the specified key.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantByteCollectionView<_, RegisterView<_, String>> =
    ///     ReentrantByteCollectionView::load(context).await.unwrap();
    /// let _subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    /// assert!(view.contains_key(&[0, 1]).await.unwrap());
    /// assert!(!view.contains_key(&[0, 2]).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key(&self, short_key: &[u8]) -> Result<bool, ViewError> {
        Ok(if let Some(entry) = self.updates.get(short_key) {
            match entry {
                Update::Set(_view) => true,
                Update::Removed => false,
            }
        } else if self.delete_storage_first {
            false
        } else if self.cached_entries.lock().unwrap().contains_key(short_key) {
            true
        } else {
            let key_index = self.context.base_tag_index(KeyTag::Index as u8, short_key);
            self.context.contains_key(&key_index).await?
        })
    }

    /// Removes an entry. If absent then nothing happens.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantByteCollectionView<_, RegisterView<_, String>> =
    ///     ReentrantByteCollectionView::load(context).await.unwrap();
    /// let mut subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// view.remove_entry(vec![0, 1]);
    /// let keys = view.keys().await.unwrap();
    /// assert_eq!(keys.len(), 0);
    /// # })
    /// ```
    pub fn remove_entry(&mut self, short_key: Vec<u8>) {
        self.cached_entries.get_mut().unwrap().remove(&short_key);
        if self.delete_storage_first {
            // Optimization: No need to mark `short_key` for deletion as we are going to remove all the keys at once.
            self.updates.remove(&short_key);
        } else {
            self.updates.insert(short_key, Update::Removed);
        }
    }

    /// Marks the entry so that it is removed in the next flush.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantByteCollectionView<_, RegisterView<_, String>> =
    ///     ReentrantByteCollectionView::load(context).await.unwrap();
    /// {
    ///     let mut subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    ///     let value = subview.get_mut();
    ///     *value = String::from("Hello");
    /// }
    /// view.try_reset_entry_to_default(&[0, 1]).unwrap();
    /// let mut subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub fn try_reset_entry_to_default(&mut self, short_key: &[u8]) -> Result<(), ViewError> {
        let key = self
            .context
            .base_tag_index(KeyTag::Subview as u8, short_key);
        let context = self.context.clone_with_base_key(key);
        let view = W::new(context)?;
        let view = Arc::new(RwLock::new(view));
        let view = Update::Set(view);
        self.updates.insert(short_key.to_vec(), view);
        self.cached_entries.get_mut().unwrap().remove(short_key);
        Ok(())
    }

    /// Gets the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, W> ReentrantByteCollectionView<C, W>
where
    C: Context + Send + Clone + 'static,
    ViewError: From<C::Error>,
    W: View<C> + Send + Sync + 'static,
{
    /// Loads multiple entries for writing at once.
    /// The entries in `short_keys` have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantByteCollectionView<_, RegisterView<_, String>> =
    ///     ReentrantByteCollectionView::load(context).await.unwrap();
    /// {
    ///     let mut subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    ///     *subview.get_mut() = "Bonjour".to_string();
    /// }
    /// let short_keys = vec![vec![0, 1], vec![2, 3]];
    /// let subviews = view.try_load_entries_mut(short_keys).await.unwrap();
    /// let value1 = subviews[0].get();
    /// let value2 = subviews[1].get();
    /// assert_eq!(*value1, "Bonjour".to_string());
    /// assert_eq!(*value2, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries_mut(
        &mut self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<WriteGuardedView<W>>, ViewError> {
        let cached_entries = self.cached_entries.get_mut().unwrap();
        let mut short_keys_to_load = Vec::new();
        let mut keys = Vec::new();
        for short_key in &short_keys {
            let key = self
                .context
                .base_tag_index(KeyTag::Subview as u8, short_key);
            let context = self.context.clone_with_base_key(key);
            match self.updates.entry(short_key.to_vec()) {
                btree_map::Entry::Occupied(mut entry) => {
                    if let Update::Removed = entry.get() {
                        let view = W::new(context)?;
                        let view = Arc::new(RwLock::new(view));
                        entry.insert(Update::Set(view));
                        cached_entries.remove(short_key);
                    }
                }
                btree_map::Entry::Vacant(entry) => {
                    if self.delete_storage_first {
                        cached_entries.remove(short_key);
                        let view = W::new(context)?;
                        let view = Arc::new(RwLock::new(view));
                        entry.insert(Update::Set(view));
                    } else if let Some(view) = cached_entries.remove(short_key) {
                        entry.insert(Update::Set(view));
                    } else {
                        keys.extend(W::pre_load(&context)?);
                        short_keys_to_load.push(short_key.to_vec());
                    }
                }
            }
        }
        let values = self.context.read_multi_values_bytes(keys).await?;
        for (loaded_values, short_key) in values
            .chunks_exact(W::NUM_INIT_KEYS)
            .zip(short_keys_to_load)
        {
            let key = self
                .context
                .base_tag_index(KeyTag::Subview as u8, &short_key);
            let context = self.context.clone_with_base_key(key);
            let view = W::post_load(context, loaded_values)?;
            let wrapped_view = Arc::new(RwLock::new(view));
            self.updates
                .insert(short_key.to_vec(), Update::Set(wrapped_view));
        }

        short_keys
            .into_iter()
            .map(|short_key| {
                let Some(Update::Set(view)) = self.updates.get(&short_key) else {
                    unreachable!()
                };
                Ok(WriteGuardedView(
                    view.clone()
                        .try_write_arc()
                        .ok_or_else(|| ViewError::TryLockError(short_key))?,
                ))
            })
            .collect()
    }

    /// Load multiple entries for reading at once.
    /// The entries in short_keys have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantByteCollectionView<_, RegisterView<_, String>> =
    ///     ReentrantByteCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    /// }
    /// let short_keys = vec![vec![0, 1], vec![2, 3]];
    /// let subviews = view.try_load_entries(short_keys).await.unwrap();
    /// assert!(subviews[1].is_none());
    /// let value0 = subviews[0].as_ref().unwrap().get();
    /// assert_eq!(*value0, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries(
        &self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<ReadGuardedView<W>>>, ViewError> {
        let mut results = vec![None; short_keys.len()];
        let mut keys_to_check = Vec::new();
        let mut keys_to_check_metadata = Vec::new();

        {
            let cached_entries = self.cached_entries.lock().unwrap();
            for (position, short_key) in short_keys.into_iter().enumerate() {
                if let Some(update) = self.updates.get(&short_key) {
                    if let Update::Set(view) = update {
                        results[position] = Some((short_key, view.clone()));
                    }
                } else if let Some(view) = cached_entries.get(&short_key) {
                    results[position] = Some((short_key, view.clone()));
                } else if !self.delete_storage_first {
                    let key_index = self.context.base_tag_index(KeyTag::Index as u8, &short_key);
                    keys_to_check.push(key_index);
                    keys_to_check_metadata.push((position, short_key));
                }
            }
        }

        let found_keys = self.context.contains_keys(keys_to_check).await?;
        let entries_to_load = keys_to_check_metadata
            .into_iter()
            .zip(found_keys)
            .filter_map(|(metadata, found)| found.then_some(metadata))
            .map(|(position, short_key)| {
                let subview_key = self
                    .context
                    .base_tag_index(KeyTag::Subview as u8, &short_key);
                let subview_context = self.context.clone_with_base_key(subview_key);
                (position, short_key.to_owned(), subview_context)
            })
            .collect::<Vec<_>>();
        if !entries_to_load.is_empty() {
            let mut keys_to_load = Vec::with_capacity(entries_to_load.len() * W::NUM_INIT_KEYS);
            for (_, _, context) in &entries_to_load {
                keys_to_load.extend(W::pre_load(context)?);
            }
            let values = self.context.read_multi_values_bytes(keys_to_load).await?;
            let mut cached_entries = self.cached_entries.lock().unwrap();
            for (loaded_values, (position, short_key, context)) in
                values.chunks_exact(W::NUM_INIT_KEYS).zip(entries_to_load)
            {
                let view = W::post_load(context, loaded_values)?;
                let wrapped_view = Arc::new(RwLock::new(view));
                cached_entries.insert(short_key.clone(), wrapped_view.clone());
                results[position] = Some((short_key, wrapped_view));
            }
        }

        results
            .into_iter()
            .map(|maybe_view| match maybe_view {
                Some((short_key, view)) => Ok(Some(ReadGuardedView(
                    view.try_read_arc()
                        .ok_or_else(|| ViewError::TryLockError(short_key))?,
                ))),
                None => Ok(None),
            })
            .collect()
    }

    /// Loads all the entries for reading at once.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantByteCollectionView<_, RegisterView<_, String>> =
    ///     ReentrantByteCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    /// }
    /// let subviews = view.try_load_all_entries().await.unwrap();
    /// assert_eq!(subviews.len(), 1);
    /// # })
    /// ```
    pub async fn try_load_all_entries(
        &self,
    ) -> Result<Vec<(Vec<u8>, ReadGuardedView<W>)>, ViewError> {
        let short_keys = self.keys().await?;
        if !self.delete_storage_first {
            let mut keys = Vec::new();
            let mut short_keys_to_load = Vec::new();
            {
                let cached_entries = self.cached_entries.lock().unwrap();
                for short_key in &short_keys {
                    if !self.updates.contains_key(short_key)
                        && !cached_entries.contains_key(short_key)
                    {
                        let key = self
                            .context
                            .base_tag_index(KeyTag::Subview as u8, short_key);
                        let context = self.context.clone_with_base_key(key);
                        keys.extend(W::pre_load(&context)?);
                        short_keys_to_load.push(short_key.to_vec());
                    }
                }
            }
            let values = self.context.read_multi_values_bytes(keys).await?;
            {
                let mut cached_entries = self.cached_entries.lock().unwrap();
                for (loaded_values, short_key) in values
                    .chunks_exact(W::NUM_INIT_KEYS)
                    .zip(short_keys_to_load)
                {
                    let key = self
                        .context
                        .base_tag_index(KeyTag::Subview as u8, &short_key);
                    let context = self.context.clone_with_base_key(key);
                    let view = W::post_load(context, loaded_values)?;
                    let wrapped_view = Arc::new(RwLock::new(view));
                    cached_entries.insert(short_key.to_vec(), wrapped_view);
                }
            }
        }
        let cached_entries = self.cached_entries.lock().unwrap();
        short_keys
            .into_iter()
            .map(|short_key| {
                let view = if let Some(Update::Set(view)) = self.updates.get(&short_key) {
                    view.clone()
                } else if let Some(view) = cached_entries.get(&short_key) {
                    view.clone()
                } else {
                    unreachable!("All entries should have been loaded into memory");
                };
                let guard = ReadGuardedView(
                    view.try_read_arc()
                        .ok_or_else(|| ViewError::TryLockError(short_key.clone()))?,
                );
                Ok((short_key, guard))
            })
            .collect()
    }

    /// Loads all the entries for writing at once.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantByteCollectionView<_, RegisterView<_, String>> =
    ///     ReentrantByteCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    /// }
    /// let subviews = view.try_load_all_entries_mut().await.unwrap();
    /// assert_eq!(subviews.len(), 1);
    /// # })
    /// ```
    pub async fn try_load_all_entries_mut(
        &mut self,
    ) -> Result<Vec<(Vec<u8>, WriteGuardedView<W>)>, ViewError> {
        let short_keys = self.keys().await?;
        if !self.delete_storage_first {
            let mut keys = Vec::new();
            let mut short_keys_to_load = Vec::new();
            {
                let cached_entries = self.cached_entries.get_mut().unwrap();
                for short_key in &short_keys {
                    if !self.updates.contains_key(short_key) {
                        if let Some(view) = cached_entries.remove(short_key) {
                            self.updates.insert(short_key.to_vec(), Update::Set(view));
                        } else {
                            let key = self
                                .context
                                .base_tag_index(KeyTag::Subview as u8, short_key);
                            let context = self.context.clone_with_base_key(key);
                            keys.extend(W::pre_load(&context)?);
                            short_keys_to_load.push(short_key.to_vec());
                        }
                    }
                }
            }
            let values = self.context.read_multi_values_bytes(keys).await?;
            for (loaded_values, short_key) in values
                .chunks_exact(W::NUM_INIT_KEYS)
                .zip(short_keys_to_load)
            {
                let key = self
                    .context
                    .base_tag_index(KeyTag::Subview as u8, &short_key);
                let context = self.context.clone_with_base_key(key);
                let view = W::post_load(context, loaded_values)?;
                let wrapped_view = Arc::new(RwLock::new(view));
                self.updates
                    .insert(short_key.to_vec(), Update::Set(wrapped_view));
            }
        }
        short_keys
            .into_iter()
            .map(|short_key| {
                let Some(Update::Set(view)) = self.updates.get(&short_key) else {
                    unreachable!("All entries should have been loaded into `updates`")
                };
                let guard = WriteGuardedView(
                    view.clone()
                        .try_write_arc()
                        .ok_or_else(|| ViewError::TryLockError(short_key.clone()))?,
                );
                Ok((short_key, guard))
            })
            .collect()
    }
}

impl<C, W> ReentrantByteCollectionView<C, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    W: View<C> + Send + Sync,
{
    /// Returns the list of indices in the collection in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantByteCollectionView<_, RegisterView<_, String>> =
    ///     ReentrantByteCollectionView::load(context).await.unwrap();
    /// view.try_load_entry_mut(&[0, 1]).await.unwrap();
    /// view.try_load_entry_mut(&[0, 2]).await.unwrap();
    /// let keys = view.keys().await.unwrap();
    /// assert_eq!(keys, vec![vec![0, 1], vec![0, 2]]);
    /// # })
    /// ```
    pub async fn keys(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut keys = Vec::new();
        self.for_each_key(|key| {
            keys.push(key.to_vec());
            Ok(())
        })
        .await?;
        Ok(keys)
    }

    /// Returns the number of indices of the collection.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantByteCollectionView<_, RegisterView<_, String>> =
    ///     ReentrantByteCollectionView::load(context).await.unwrap();
    /// view.try_load_entry_mut(&[0, 1]).await.unwrap();
    /// view.try_load_entry_mut(&[0, 2]).await.unwrap();
    /// assert_eq!(view.count().await.unwrap(), 2);
    /// # })
    /// ```
    pub async fn count(&self) -> Result<usize, ViewError> {
        let mut count = 0;
        self.for_each_key(|_key| {
            count += 1;
            Ok(())
        })
        .await?;
        Ok(count)
    }

    /// Applies a function f on each index (aka key). Keys are visited in a
    /// lexicographic order. If the function returns false then the loop
    /// ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantByteCollectionView<_, RegisterView<_, String>> =
    ///     ReentrantByteCollectionView::load(context).await.unwrap();
    /// view.try_load_entry_mut(&[0, 1]).await.unwrap();
    /// view.try_load_entry_mut(&[0, 2]).await.unwrap();
    /// let mut count = 0;
    /// view.for_each_key_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 1)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 1);
    /// # })
    /// ```
    pub async fn for_each_key_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let mut updates = self.updates.iter();
        let mut update = updates.next();
        if !self.delete_storage_first {
            let base = self.get_index_key(&[]);
            for index in self.context.find_keys_by_prefix(&base).await?.iterator() {
                let index = index?;
                loop {
                    match update {
                        Some((key, value)) if key.as_slice() <= index => {
                            if let Update::Set(_) = value {
                                if !f(key)? {
                                    return Ok(());
                                }
                            }
                            update = updates.next();
                            if key == index {
                                break;
                            }
                        }
                        _ => {
                            if !f(index)? {
                                return Ok(());
                            }
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = update {
            if let Update::Set(_) = value {
                if !f(key)? {
                    return Ok(());
                }
            }
            update = updates.next();
        }
        Ok(())
    }

    /// Applies a function f on each index (aka key). Keys are visited in a
    /// lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantByteCollectionView<_, RegisterView<_, String>> =
    ///     ReentrantByteCollectionView::load(context).await.unwrap();
    /// view.try_load_entry_mut(&[0, 1]).await.unwrap();
    /// view.try_load_entry_mut(&[0, 2]).await.unwrap();
    /// let mut count = 0;
    /// view.for_each_key(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// # })
    /// ```
    pub async fn for_each_key<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_key_while(|key| {
            f(key)?;
            Ok(true)
        })
        .await
    }
}

#[async_trait]
impl<C, W> HashableView<C> for ReentrantByteCollectionView<C, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    W: HashableView<C> + Send + Sync + 'static,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = REENTRANT_COLLECTION_VIEW_HASH_RUNTIME.measure_latency();
        let mut hasher = sha3::Sha3_256::default();
        let keys = self.keys().await?;
        let count = keys.len() as u32;
        hasher.update_with_bcs_bytes(&count)?;
        let cached_entries = self.cached_entries.get_mut().unwrap();
        for key in keys {
            hasher.update_with_bytes(&key)?;
            let hash = if let Some(entry) = self.updates.get_mut(&key) {
                let Update::Set(view) = entry else {
                    unreachable!();
                };
                let mut view = view
                    .try_write_arc()
                    .ok_or_else(|| ViewError::TryLockError(key))?;
                view.hash_mut().await?
            } else if let Some(view) = cached_entries.get_mut(&key) {
                let mut view = view
                    .try_write_arc()
                    .ok_or_else(|| ViewError::TryLockError(key))?;
                view.hash_mut().await?
            } else {
                let key = self.context.base_tag_index(KeyTag::Subview as u8, &key);
                let context = self.context.clone_with_base_key(key);
                let mut view = W::load(context).await?;
                view.hash_mut().await?
            };
            hasher.write_all(hash.as_ref())?;
        }
        Ok(hasher.finalize())
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = REENTRANT_COLLECTION_VIEW_HASH_RUNTIME.measure_latency();
        let mut hasher = sha3::Sha3_256::default();
        let keys = self.keys().await?;
        let count = keys.len() as u32;
        hasher.update_with_bcs_bytes(&count)?;
        let mut cached_entries_result = Vec::new();
        {
            let cached_entries = self.cached_entries.lock().unwrap();
            for key in &keys {
                cached_entries_result.push(cached_entries.get(key).cloned());
            }
        }
        for (key, cached_entry) in keys.into_iter().zip(cached_entries_result) {
            hasher.update_with_bytes(&key)?;
            let hash = if let Some(entry) = self.updates.get(&key) {
                let Update::Set(view) = entry else {
                    unreachable!();
                };
                let view = view
                    .try_read_arc()
                    .ok_or_else(|| ViewError::TryLockError(key))?;
                view.hash().await?
            } else if let Some(view) = cached_entry {
                let view = view
                    .try_read_arc()
                    .ok_or_else(|| ViewError::TryLockError(key))?;
                view.hash().await?
            } else {
                let key = self.context.base_tag_index(KeyTag::Subview as u8, &key);
                let context = self.context.clone_with_base_key(key);
                let view = W::load(context).await?;
                view.hash().await?
            };
            hasher.write_all(hash.as_ref())?;
        }
        Ok(hasher.finalize())
    }
}

/// A view that supports accessing a collection of views of the same kind, indexed by keys,
/// possibly several subviews at a time.
#[derive(Debug)]
pub struct ReentrantCollectionView<C, I, W> {
    collection: ReentrantByteCollectionView<C, W>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I, W> View<C> for ReentrantCollectionView<C, I, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize + DeserializeOwned,
    W: View<C> + Send + Sync,
{
    const NUM_INIT_KEYS: usize = ReentrantByteCollectionView::<C, W>::NUM_INIT_KEYS;

    fn context(&self) -> &C {
        self.collection.context()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        ReentrantByteCollectionView::<C, W>::pre_load(context)
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let collection = ReentrantByteCollectionView::post_load(context, values)?;
        Ok(ReentrantCollectionView {
            collection,
            _phantom: PhantomData,
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Self::post_load(context, &[])
    }

    fn rollback(&mut self) {
        self.collection.rollback()
    }

    async fn has_pending_changes(&self) -> bool {
        self.collection.has_pending_changes().await
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.collection.flush(batch)
    }

    fn clear(&mut self) {
        self.collection.clear()
    }
}

impl<C, I, W> ClonableView<C> for ReentrantCollectionView<C, I, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize + DeserializeOwned,
    W: ClonableView<C> + Send + Sync,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(ReentrantCollectionView {
            collection: self.collection.clone_unchecked()?,
            _phantom: PhantomData,
        })
    }
}

impl<C, I, W> ReentrantCollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Serialize + DeserializeOwned,
    W: View<C> + Send + Sync,
{
    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then a default entry is put on the collection. The obtained view can
    /// then be modified.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCollectionView<_, u64, RegisterView<_, String>> =
    ///     ReentrantCollectionView::load(context).await.unwrap();
    /// let subview = view.try_load_entry_mut(&23).await.unwrap();
    /// let value = subview.get();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry_mut<Q>(
        &mut self,
        index: &Q,
    ) -> Result<WriteGuardedView<W>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.try_load_entry_mut(&short_key).await
    }

    /// Loads a subview at the given index in the collection and gives read-only access to the data.
    /// If an entry is absent then `None` is returned.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCollectionView<_, u64, RegisterView<_, String>> =
    ///     ReentrantCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.try_load_entry_mut(&23).await.unwrap();
    /// }
    /// let subview = view.try_load_entry(&23).await.unwrap().unwrap();
    /// let value = subview.get();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry<Q>(
        &self,
        index: &Q,
    ) -> Result<Option<ReadGuardedView<W>>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.try_load_entry(&short_key).await
    }

    /// Returns `true` if the collection contains a value for the specified key.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCollectionView<_, u64, RegisterView<_, String>> =
    ///     ReentrantCollectionView::load(context).await.unwrap();
    /// let _subview = view.try_load_entry_mut(&23).await.unwrap();
    /// assert!(view.contains_key(&23).await.unwrap());
    /// assert!(!view.contains_key(&24).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.contains_key(&short_key).await
    }

    /// Marks the entry so that it is removed in the next flush.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCollectionView<_, u64, RegisterView<_, String>> =
    ///     ReentrantCollectionView::load(context).await.unwrap();
    /// let mut subview = view.try_load_entry_mut(&23).await.unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// view.remove_entry(&23);
    /// let keys = view.indices().await.unwrap();
    /// assert_eq!(keys.len(), 0);
    /// # })
    /// ```
    pub fn remove_entry<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.remove_entry(short_key);
        Ok(())
    }

    /// Marks the entry so that it is removed in the next flush.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCollectionView<_, u64, RegisterView<_, String>> =
    ///     ReentrantCollectionView::load(context).await.unwrap();
    /// {
    ///     let mut subview = view.try_load_entry_mut(&23).await.unwrap();
    ///     let value = subview.get_mut();
    ///     *value = String::from("Hello");
    /// }
    /// view.try_reset_entry_to_default(&23).unwrap();
    /// let mut subview = view.try_load_entry_mut(&23).await.unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub fn try_reset_entry_to_default<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.try_reset_entry_to_default(&short_key)
    }

    /// Gets the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.collection.extra()
    }
}

impl<C, I, W> ReentrantCollectionView<C, I, W>
where
    C: Context + Send + Clone + 'static,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Serialize + DeserializeOwned,
    W: View<C> + Send + Sync + 'static,
{
    /// Load multiple entries for writing at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCollectionView<_, u64, RegisterView<_, String>> =
    ///     ReentrantCollectionView::load(context).await.unwrap();
    /// let indices = vec![23, 42];
    /// let subviews = view.try_load_entries_mut(&indices).await.unwrap();
    /// let value1 = subviews[0].get();
    /// let value2 = subviews[1].get();
    /// assert_eq!(*value1, String::default());
    /// assert_eq!(*value2, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries_mut<'a, Q>(
        &'a mut self,
        indices: impl IntoIterator<Item = &'a Q>,
    ) -> Result<Vec<WriteGuardedView<W>>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + 'a,
    {
        let short_keys = indices
            .into_iter()
            .map(|index| C::derive_short_key(index))
            .collect::<Result<_, _>>()?;
        self.collection.try_load_entries_mut(short_keys).await
    }

    /// Load multiple entries for reading at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCollectionView<_, u64, RegisterView<_, String>> =
    ///     ReentrantCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.try_load_entry_mut(&23).await.unwrap();
    /// }
    /// let indices = vec![23, 42];
    /// let subviews = view.try_load_entries(&indices).await.unwrap();
    /// assert!(subviews[1].is_none());
    /// let value0 = subviews[0].as_ref().unwrap().get();
    /// assert_eq!(*value0, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries<'a, Q>(
        &'a self,
        indices: impl IntoIterator<Item = &'a Q>,
    ) -> Result<Vec<Option<ReadGuardedView<W>>>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + 'a,
    {
        let short_keys = indices
            .into_iter()
            .map(|index| C::derive_short_key(index))
            .collect::<Result<_, _>>()?;
        self.collection.try_load_entries(short_keys).await
    }

    /// Loads all entries for writing at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCollectionView<_, u64, RegisterView<_, String>> =
    ///     ReentrantCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.try_load_entry_mut(&23).await.unwrap();
    /// }
    /// let subviews = view.try_load_all_entries_mut().await.unwrap();
    /// assert_eq!(subviews.len(), 1);
    /// # })
    /// ```
    pub async fn try_load_all_entries_mut<'a, Q>(
        &'a mut self,
    ) -> Result<Vec<(I, WriteGuardedView<W>)>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + 'a,
    {
        let results = self.collection.try_load_all_entries_mut().await?;
        results
            .into_iter()
            .map(|(short_key, view)| {
                let index = C::deserialize_value(&short_key)?;
                Ok((index, view))
            })
            .collect()
    }

    /// Load multiple entries for reading at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCollectionView<_, u64, RegisterView<_, String>> =
    ///     ReentrantCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.try_load_entry_mut(&23).await.unwrap();
    /// }
    /// let subviews = view.try_load_all_entries().await.unwrap();
    /// assert_eq!(subviews.len(), 1);
    /// # })
    /// ```
    pub async fn try_load_all_entries<'a, Q>(
        &'a self,
    ) -> Result<Vec<(I, ReadGuardedView<W>)>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + 'a,
    {
        let results = self.collection.try_load_all_entries().await?;
        results
            .into_iter()
            .map(|(short_key, view)| {
                let index = C::deserialize_value(&short_key)?;
                Ok((index, view))
            })
            .collect()
    }
}

impl<C, I, W> ReentrantCollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Serialize + DeserializeOwned,
    W: View<C> + Send + Sync,
{
    /// Returns the list of indices in the collection in an order determined
    /// by serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCollectionView<_, u64, RegisterView<_, String>> =
    ///     ReentrantCollectionView::load(context).await.unwrap();
    /// view.try_load_entry_mut(&23).await.unwrap();
    /// view.try_load_entry_mut(&25).await.unwrap();
    /// let indices = view.indices().await.unwrap();
    /// assert_eq!(indices.len(), 2);
    /// # })
    /// ```
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Returns the number of indices in the collection.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCollectionView<_, u64, RegisterView<_, String>> =
    ///     ReentrantCollectionView::load(context).await.unwrap();
    /// view.try_load_entry_mut(&23).await.unwrap();
    /// view.try_load_entry_mut(&25).await.unwrap();
    /// assert_eq!(view.count().await.unwrap(), 2);
    /// # })
    /// ```
    pub async fn count(&self) -> Result<usize, ViewError> {
        self.collection.count().await
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization. If the function f returns false then
    /// the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCollectionView<_, u64, RegisterView<_, String>> =
    ///     ReentrantCollectionView::load(context).await.unwrap();
    /// view.try_load_entry_mut(&23).await.unwrap();
    /// view.try_load_entry_mut(&24).await.unwrap();
    /// let mut count = 0;
    /// view.for_each_index_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 1)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 1);
    /// # })
    /// ```
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.collection
            .for_each_key_while(|key| {
                let index = C::deserialize_value(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCollectionView<_, u64, RegisterView<_, String>> =
    ///     ReentrantCollectionView::load(context).await.unwrap();
    /// view.try_load_entry_mut(&23).await.unwrap();
    /// view.try_load_entry_mut(&28).await.unwrap();
    /// let mut count = 0;
    /// view.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// # })
    /// ```
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.collection
            .for_each_key(|key| {
                let index = C::deserialize_value(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<C, I, W> HashableView<C> for ReentrantCollectionView<C, I, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize + DeserializeOwned,
    W: HashableView<C> + Send + Sync + 'static,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.collection.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.collection.hash().await
    }
}

/// A view that supports accessing a collection of views of the same kind, indexed by an ordered key,
/// possibly several subviews at a time.
#[derive(Debug)]
pub struct ReentrantCustomCollectionView<C, I, W> {
    collection: ReentrantByteCollectionView<C, W>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I, W> View<C> for ReentrantCustomCollectionView<C, I, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
    W: View<C> + Send + Sync,
{
    const NUM_INIT_KEYS: usize = ReentrantByteCollectionView::<C, W>::NUM_INIT_KEYS;

    fn context(&self) -> &C {
        self.collection.context()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        ReentrantByteCollectionView::<C, W>::pre_load(context)
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let collection = ReentrantByteCollectionView::post_load(context, values)?;
        Ok(ReentrantCustomCollectionView {
            collection,
            _phantom: PhantomData,
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Self::post_load(context, &[])
    }

    fn rollback(&mut self) {
        self.collection.rollback()
    }

    async fn has_pending_changes(&self) -> bool {
        self.collection.has_pending_changes().await
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.collection.flush(batch)
    }

    fn clear(&mut self) {
        self.collection.clear()
    }
}

impl<C, I, W> ClonableView<C> for ReentrantCustomCollectionView<C, I, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
    W: ClonableView<C> + Send + Sync,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(ReentrantCustomCollectionView {
            collection: self.collection.clone_unchecked()?,
            _phantom: PhantomData,
        })
    }
}

impl<C, I, W> ReentrantCustomCollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + CustomSerialize,
    W: View<C> + Send + Sync,
{
    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then a default entry is put in the collection on this index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     ReentrantCustomCollectionView::load(context).await.unwrap();
    /// let subview = view.try_load_entry_mut(&23).await.unwrap();
    /// let value = subview.get();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry_mut<Q>(
        &mut self,
        index: &Q,
    ) -> Result<WriteGuardedView<W>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.try_load_entry_mut(&short_key).await
    }

    /// Loads a subview at the given index in the collection and gives read-only access to the data.
    /// If an entry is absent then `None` is returned.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     ReentrantCustomCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.try_load_entry_mut(&23).await.unwrap();
    /// }
    /// let subview = view.try_load_entry(&23).await.unwrap().unwrap();
    /// let value = subview.get();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry<Q>(
        &self,
        index: &Q,
    ) -> Result<Option<ReadGuardedView<W>>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.try_load_entry(&short_key).await
    }

    /// Returns `true` if the collection contains a value for the specified key.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     ReentrantCustomCollectionView::load(context).await.unwrap();
    /// let _subview = view.try_load_entry_mut(&23).await.unwrap();
    /// assert!(view.contains_key(&23).await.unwrap());
    /// assert!(!view.contains_key(&24).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.contains_key(&short_key).await
    }

    /// Removes an entry. If absent then nothing happens.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     ReentrantCustomCollectionView::load(context).await.unwrap();
    /// let mut subview = view.try_load_entry_mut(&23).await.unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// view.remove_entry(&23);
    /// let keys = view.indices().await.unwrap();
    /// assert_eq!(keys.len(), 0);
    /// # })
    /// ```
    pub fn remove_entry<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.remove_entry(short_key);
        Ok(())
    }

    /// Marks the entry so that it is removed in the next flush.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     ReentrantCustomCollectionView::load(context).await.unwrap();
    /// {
    ///     let mut subview = view.try_load_entry_mut(&23).await.unwrap();
    ///     let value = subview.get_mut();
    ///     *value = String::from("Hello");
    /// }
    /// {
    ///     view.try_reset_entry_to_default(&23).unwrap();
    ///     let subview = view.try_load_entry(&23).await.unwrap().unwrap();
    ///     let value = subview.get();
    ///     assert_eq!(*value, String::default());
    /// }
    /// # })
    /// ```
    pub fn try_reset_entry_to_default<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.try_reset_entry_to_default(&short_key)
    }

    /// Gets the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.collection.extra()
    }
}

impl<C, I, W> ReentrantCustomCollectionView<C, I, W>
where
    C: Context + Send + Clone + 'static,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + CustomSerialize,
    W: View<C> + Send + Sync + 'static,
{
    /// Load multiple entries for writing at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     ReentrantCustomCollectionView::load(context).await.unwrap();
    /// let indices = vec![23, 42];
    /// let subviews = view.try_load_entries_mut(indices).await.unwrap();
    /// let value1 = subviews[0].get();
    /// let value2 = subviews[1].get();
    /// assert_eq!(*value1, String::default());
    /// assert_eq!(*value2, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries_mut<Q>(
        &mut self,
        indices: impl IntoIterator<Item = Q>,
    ) -> Result<Vec<WriteGuardedView<W>>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_keys = indices
            .into_iter()
            .map(|index| index.to_custom_bytes())
            .collect::<Result<_, _>>()?;
        self.collection.try_load_entries_mut(short_keys).await
    }

    /// Load multiple entries for reading at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     ReentrantCustomCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.try_load_entry_mut(&23).await.unwrap();
    /// }
    /// let indices = vec![23, 42];
    /// let subviews = view.try_load_entries(indices).await.unwrap();
    /// assert!(subviews[1].is_none());
    /// let value0 = subviews[0].as_ref().unwrap().get();
    /// assert_eq!(*value0, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries<Q>(
        &self,
        indices: impl IntoIterator<Item = Q>,
    ) -> Result<Vec<Option<ReadGuardedView<W>>>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_keys = indices
            .into_iter()
            .map(|index| index.to_custom_bytes())
            .collect::<Result<_, _>>()?;
        self.collection.try_load_entries(short_keys).await
    }

    /// Loads all entries for writing at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     ReentrantCustomCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.try_load_entry_mut(&23).await.unwrap();
    /// }
    /// let subviews = view.try_load_all_entries_mut().await.unwrap();
    /// assert_eq!(subviews.len(), 1);
    /// # })
    /// ```
    pub async fn try_load_all_entries_mut<Q>(
        &mut self,
    ) -> Result<Vec<(I, WriteGuardedView<W>)>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let results = self.collection.try_load_all_entries_mut().await?;
        results
            .into_iter()
            .map(|(short_key, view)| {
                let index = I::from_custom_bytes(&short_key)?;
                Ok((index, view))
            })
            .collect()
    }

    /// Load multiple entries for reading at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     ReentrantCustomCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.try_load_entry_mut(&23).await.unwrap();
    /// }
    /// let subviews = view.try_load_all_entries().await.unwrap();
    /// assert_eq!(subviews.len(), 1);
    /// # })
    /// ```
    pub async fn try_load_all_entries<Q>(&self) -> Result<Vec<(I, ReadGuardedView<W>)>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let results = self.collection.try_load_all_entries().await?;
        results
            .into_iter()
            .map(|(short_key, view)| {
                let index = I::from_custom_bytes(&short_key)?;
                Ok((index, view))
            })
            .collect()
    }
}

impl<C, I, W> ReentrantCustomCollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + CustomSerialize,
    W: View<C> + Send + Sync,
{
    /// Returns the list of indices in the collection. The order is determined by
    /// the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     ReentrantCustomCollectionView::load(context).await.unwrap();
    /// view.try_load_entry_mut(&23).await.unwrap();
    /// view.try_load_entry_mut(&25).await.unwrap();
    /// let indices = view.indices().await.unwrap();
    /// assert_eq!(indices, vec![23, 25]);
    /// # })
    /// ```
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Returns the number of entries in the collection.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     ReentrantCustomCollectionView::load(context).await.unwrap();
    /// view.try_load_entry_mut(&23).await.unwrap();
    /// view.try_load_entry_mut(&25).await.unwrap();
    /// assert_eq!(view.count().await.unwrap(), 2);
    /// # })
    /// ```
    pub async fn count(&self) -> Result<usize, ViewError> {
        self.collection.count().await
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization. If the function f returns false
    /// then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     ReentrantCustomCollectionView::load(context).await.unwrap();
    /// view.try_load_entry_mut(&28).await.unwrap();
    /// view.try_load_entry_mut(&24).await.unwrap();
    /// view.try_load_entry_mut(&23).await.unwrap();
    /// let mut part_indices = Vec::new();
    /// view.for_each_index_while(|index| {
    ///     part_indices.push(index);
    ///     Ok(part_indices.len() < 2)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(part_indices, vec![23, 24]);
    /// # })
    /// ```
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.collection
            .for_each_key_while(|key| {
                let index = I::from_custom_bytes(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::{create_test_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut view: ReentrantCustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     ReentrantCustomCollectionView::load(context).await.unwrap();
    /// view.try_load_entry_mut(&28).await.unwrap();
    /// view.try_load_entry_mut(&24).await.unwrap();
    /// view.try_load_entry_mut(&23).await.unwrap();
    /// let mut indices = Vec::new();
    /// view.for_each_index(|index| {
    ///     indices.push(index);
    ///     Ok(())
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(indices, vec![23, 24, 28]);
    /// # })
    /// ```
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.collection
            .for_each_key(|key| {
                let index = I::from_custom_bytes(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<C, I, W> HashableView<C> for ReentrantCustomCollectionView<C, I, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
    W: HashableView<C> + Send + Sync + 'static,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.collection.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.collection.hash().await
    }
}

/// Type wrapping `ReentrantByteCollectionView` while memoizing the hash.
pub type HashedReentrantByteCollectionView<C, W> =
    WrappedHashableContainerView<C, ReentrantByteCollectionView<C, W>, HasherOutput>;

/// Type wrapping `ReentrantCollectionView` while memoizing the hash.
pub type HashedReentrantCollectionView<C, I, W> =
    WrappedHashableContainerView<C, ReentrantCollectionView<C, I, W>, HasherOutput>;

/// Type wrapping `ReentrantCustomCollectionView` while memoizing the hash.
pub type HashedReentrantCustomCollectionView<C, I, W> =
    WrappedHashableContainerView<C, ReentrantCustomCollectionView<C, I, W>, HasherOutput>;

mod graphql {
    use std::borrow::Cow;

    use super::{ReadGuardedView, ReentrantCollectionView};
    use crate::{
        context::Context,
        graphql::{hash_name, mangle, missing_key_error, Entry, MapFilters, MapInput},
        views::View,
    };

    impl<T: async_graphql::OutputType> async_graphql::OutputType for ReadGuardedView<T> {
        fn type_name() -> Cow<'static, str> {
            T::type_name()
        }

        fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
            T::create_type_info(registry)
        }

        async fn resolve(
            &self,
            ctx: &async_graphql::ContextSelectionSet<'_>,
            field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
        ) -> async_graphql::ServerResult<async_graphql::Value> {
            (**self).resolve(ctx, field).await
        }
    }

    impl<C: Send + Sync, K: async_graphql::OutputType, V: async_graphql::OutputType>
        async_graphql::TypeName for ReentrantCollectionView<C, K, V>
    {
        fn type_name() -> Cow<'static, str> {
            format!(
                "ReentrantCollectionView_{}_{}_{}",
                mangle(K::type_name()),
                mangle(V::type_name()),
                hash_name::<(K, V)>(),
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C, K, V> ReentrantCollectionView<C, K, V>
    where
        C: Send + Sync + Context,
        K: async_graphql::InputType
            + async_graphql::OutputType
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::fmt::Debug
            + Clone,
        V: View<C> + async_graphql::OutputType,
        MapInput<K>: async_graphql::InputType,
        MapFilters<K>: async_graphql::InputType,
    {
        async fn keys(&self) -> Result<Vec<K>, async_graphql::Error> {
            Ok(self.indices().await?)
        }

        async fn entry(
            &self,
            key: K,
        ) -> Result<Entry<K, ReadGuardedView<V>>, async_graphql::Error> {
            let value = self
                .try_load_entry(&key)
                .await?
                .ok_or_else(|| missing_key_error(&key))?;
            Ok(Entry { value, key })
        }

        async fn entries(
            &self,
            input: Option<MapInput<K>>,
        ) -> Result<Vec<Entry<K, ReadGuardedView<V>>>, async_graphql::Error> {
            let keys = if let Some(keys) = input
                .and_then(|input| input.filters)
                .and_then(|filters| filters.keys)
            {
                keys
            } else {
                self.indices().await?
            };

            let mut values = vec![];
            for key in keys {
                let value = self
                    .try_load_entry(&key)
                    .await?
                    .ok_or_else(|| missing_key_error(&key))?;
                values.push(Entry { value, key })
            }

            Ok(values)
        }
    }

    use crate::reentrant_collection_view::ReentrantCustomCollectionView;
    impl<C: Send + Sync, K: async_graphql::OutputType, V: async_graphql::OutputType>
        async_graphql::TypeName for ReentrantCustomCollectionView<C, K, V>
    {
        fn type_name() -> Cow<'static, str> {
            format!(
                "ReentrantCustomCollectionView_{}_{}_{:08x}",
                mangle(K::type_name()),
                mangle(V::type_name()),
                hash_name::<(K, V)>(),
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C, K, V> ReentrantCustomCollectionView<C, K, V>
    where
        C: Send + Sync + Context,
        K: async_graphql::InputType
            + async_graphql::OutputType
            + crate::common::CustomSerialize
            + std::fmt::Debug
            + Clone,
        V: View<C> + async_graphql::OutputType,
        MapInput<K>: async_graphql::InputType,
        MapFilters<K>: async_graphql::InputType,
    {
        async fn keys(&self) -> Result<Vec<K>, async_graphql::Error> {
            Ok(self.indices().await?)
        }

        async fn entry(
            &self,
            key: K,
        ) -> Result<Entry<K, ReadGuardedView<V>>, async_graphql::Error> {
            let value = self
                .try_load_entry(&key)
                .await?
                .ok_or_else(|| missing_key_error(&key))?;
            Ok(Entry { value, key })
        }

        async fn entries(
            &self,
            input: Option<MapInput<K>>,
        ) -> Result<Vec<Entry<K, ReadGuardedView<V>>>, async_graphql::Error> {
            let keys = if let Some(keys) = input
                .and_then(|input| input.filters)
                .and_then(|filters| filters.keys)
            {
                keys
            } else {
                self.indices().await?
            };

            let mut values = vec![];
            for key in keys {
                let value = self
                    .try_load_entry(&key)
                    .await?
                    .ok_or_else(|| missing_key_error(&key))?;
                values.push(Entry { value, key })
            }

            Ok(values)
        }
    }
}
