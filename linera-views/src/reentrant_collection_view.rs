// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Borrow,
    collections::{btree_map, BTreeMap},
    fmt::Debug,
    io::Write,
    marker::PhantomData,
    mem,
    sync::Arc,
};

use async_lock::{Mutex, RwLock, RwLockReadGuardArc, RwLockWriteGuardArc};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{self, MeasureLatency},
    linera_base::sync::Lazy,
    prometheus::HistogramVec,
};

use crate::{
    batch::Batch,
    common::{Context, CustomSerialize, HasherOutput, KeyIterable, Update, MIN_VIEW_TAG},
    hashable_wrapper::WrappedHashableContainerView,
    views::{ClonableView, HashableView, Hasher, View, ViewError},
};

#[cfg(with_metrics)]
/// The runtime of hash computation
static REENTRANT_COLLECTION_VIEW_HASH_RUNTIME: Lazy<HistogramVec> = Lazy::new(|| {
    prometheus_util::register_histogram_vec(
        "reentrant_collection_view_hash_runtime",
        "ReentrantCollectionView hash runtime",
        &[],
        Some(vec![
            0.001, 0.003, 0.01, 0.03, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 2.0, 5.0,
        ]),
    )
    .expect("Histogram can be created")
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
#[allow(clippy::type_complexity)]
pub struct ReentrantByteCollectionView<C, W> {
    context: C,
    delete_storage_first: bool,
    updates: Mutex<BTreeMap<Vec<u8>, Update<Arc<RwLock<W>>>>>,
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
            updates: Mutex::new(BTreeMap::new()),
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Self::post_load(context, &[])
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.updates.get_mut().clear();
    }

    async fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        let updates = self.updates.lock().await;
        !updates.is_empty()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            delete_view = true;
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(self.updates.get_mut()) {
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
            for (index, update) in mem::take(self.updates.get_mut()) {
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
        self.updates.get_mut().clear();
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
            .get_mut()
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
            updates: Mutex::new(cloned_updates),
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
        let updates = self.updates.get_mut();
        Ok(match updates.entry(short_key.to_owned()) {
            Occupied(mut entry) => match entry.get_mut() {
                Update::Set(view) => view.clone(),
                entry @ Update::Removed => {
                    let wrapped_view = Self::wrapped_view(&self.context, true, short_key).await?;
                    *entry = Update::Set(wrapped_view.clone());
                    wrapped_view
                }
            },
            Vacant(entry) => {
                let wrapped_view =
                    Self::wrapped_view(&self.context, self.delete_storage_first, short_key).await?;
                entry.insert(Update::Set(wrapped_view.clone()));
                wrapped_view
            }
        })
    }

    /// Load the view from the update is available.
    /// If missing, then the entry is loaded from storage and if
    /// missing there an error is reported.
    async fn try_load_view(&self, short_key: &[u8]) -> Result<Option<Arc<RwLock<W>>>, ViewError> {
        let updates = self.updates.lock().await;
        Ok(match updates.get(short_key) {
            Some(entry) => match entry {
                Update::Set(view) => Some(view.clone()),
                _entry @ Update::Removed => None,
            },
            None => {
                let key_index = self.context.base_tag_index(KeyTag::Index as u8, short_key);
                if self.delete_storage_first || !self.context.contains_key(&key_index).await? {
                    None
                } else {
                    Some(Self::wrapped_view(&self.context, false, short_key).await?)
                }
            }
        })
    }

    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then a default entry is added to the collection. The resulting view
    /// can be modified.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantByteCollectionView<_, RegisterView<_,String>> = ReentrantByteCollectionView::load(context).await.unwrap();
    ///   let subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
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
    /// If an entry is absent then a default entry is added to the collection. The resulting view
    /// cannot be modified.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantByteCollectionView<_, RegisterView<_,String>> = ReentrantByteCollectionView::load(context).await.unwrap();
    ///   let subview = view.try_load_entry_or_insert(&[0, 1]).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry_or_insert(
        &mut self,
        short_key: &[u8],
    ) -> Result<ReadGuardedView<W>, ViewError> {
        Ok(ReadGuardedView(
            self.try_load_view_mut(short_key)
                .await?
                .try_read_arc()
                .ok_or_else(|| ViewError::TryLockError(short_key.to_vec()))?,
        ))
    }

    /// Loads a subview at the given index in the collection and gives read-only access to the data.
    /// If an entry is absent then `None` is returned.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantByteCollectionView<_, RegisterView<_,String>> = ReentrantByteCollectionView::load(context).await.unwrap();
    ///   let _subview = view.try_load_entry_or_insert(&[0, 1]).await.unwrap();
    ///   let subview = view.try_load_entry(&[0, 1]).await.unwrap().unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
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
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantByteCollectionView<_, RegisterView<_,String>> = ReentrantByteCollectionView::load(context).await.unwrap();
    ///   let _subview = view.try_load_entry_or_insert(&[0, 1]).await.unwrap();
    ///   assert!(view.contains_key(&[0, 1]).await.unwrap());
    ///   assert!(!view.contains_key(&[0, 2]).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key(&self, short_key: &[u8]) -> Result<bool, ViewError> {
        let updates = self.updates.lock().await;
        Ok(match updates.get(short_key) {
            Some(entry) => match entry {
                Update::Set(_view) => true,
                Update::Removed => false,
            },
            None => {
                let key_index = self.context.base_tag_index(KeyTag::Index as u8, short_key);
                !self.delete_storage_first && self.context.contains_key(&key_index).await?
            }
        })
    }

    /// Removes an entry. If absent then nothing happens.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantByteCollectionView<_, RegisterView<_,String>> = ReentrantByteCollectionView::load(context).await.unwrap();
    ///   let mut subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    ///   let value = subview.get_mut();
    ///   assert_eq!(*value, String::default());
    ///   view.remove_entry(vec![0, 1]);
    ///   let keys = view.keys().await.unwrap();
    ///   assert_eq!(keys.len(), 0);
    /// # })
    /// ```
    pub fn remove_entry(&mut self, short_key: Vec<u8>) {
        if self.delete_storage_first {
            // Optimization: No need to mark `short_key` for deletion as we are going to remove all the keys at once.
            self.updates.get_mut().remove(&short_key);
        } else {
            self.updates.get_mut().insert(short_key, Update::Removed);
        }
    }

    /// Marks the entry so that it is removed in the next flush.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantByteCollectionView<_, RegisterView<_,String>> = ReentrantByteCollectionView::load(context).await.unwrap();
    ///   {
    ///     let mut subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    ///     let value = subview.get_mut();
    ///     *value = String::from("Hello");
    ///   }
    ///   view.try_reset_entry_to_default(&[0, 1]).unwrap();
    ///   let mut subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    ///   let value = subview.get_mut();
    ///   assert_eq!(*value, String::default());
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
        let updates = self.updates.get_mut();
        updates.insert(short_key.to_vec(), view);
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
    async fn do_load_entries(
        context: &C,
        updates: &mut BTreeMap<Vec<u8>, Update<Arc<RwLock<W>>>>,
        delete_storage_first: bool,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Arc<RwLock<W>>)>, ViewError> {
        let mut selected_short_keys = Vec::new();
        for short_key in &short_keys {
            match updates.entry(short_key.to_vec()) {
                btree_map::Entry::Occupied(entry) => {
                    let entry = entry.into_mut();
                    if let Update::Removed = entry {
                        let key = context.base_tag_index(KeyTag::Subview as u8, short_key);
                        let context = context.clone_with_base_key(key);
                        let view = W::new(context)?;
                        let view = Arc::new(RwLock::new(view));
                        *entry = Update::Set(view);
                    }
                }
                btree_map::Entry::Vacant(entry) => {
                    if delete_storage_first {
                        let key = context.base_tag_index(KeyTag::Subview as u8, short_key);
                        let context = context.clone_with_base_key(key);
                        let view = W::new(context)?;
                        let view = Arc::new(RwLock::new(view));
                        entry.insert(Update::Set(view));
                    } else {
                        selected_short_keys.push(short_key.to_vec());
                    }
                }
            }
        }
        let mut handles = Vec::new();
        for short_key in &selected_short_keys {
            let key = context.base_tag_index(KeyTag::Subview as u8, short_key);
            let context = context.clone_with_base_key(key);
            handles.push(tokio::spawn(async move { W::load(context).await }));
        }
        let response = futures::future::join_all(handles).await;
        for (short_key, view) in selected_short_keys.into_iter().zip(response) {
            let view = view??;
            let wrapped_view = Arc::new(RwLock::new(view));
            updates.insert(short_key, Update::Set(wrapped_view));
        }

        Ok(short_keys
            .into_iter()
            .map(|short_key| {
                let btree_map::Entry::Occupied(entry) = updates.entry(short_key.clone()) else {
                    unreachable!()
                };
                let Update::Set(view) = entry.into_mut() else {
                    unreachable!()
                };
                (short_key, view.clone())
            })
            .collect())
    }

    /// Load multiple entries for writing at once.
    /// The entries in short_keys have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantByteCollectionView<_, RegisterView<_,String>> = ReentrantByteCollectionView::load(context).await.unwrap();
    ///   {
    ///     let mut subview = view.try_load_entry_mut(&[0, 1]).await.unwrap();
    ///     *subview.get_mut() = "Bonjour".to_string();
    ///   }
    ///   let short_keys = vec![vec![0, 1], vec![2, 3],];
    ///   let subviews = view.try_load_entries_mut(short_keys).await.unwrap();
    ///   let value1 = subviews[0].get();
    ///   let value2 = subviews[1].get();
    ///   assert_eq!(*value1, "Bonjour".to_string());
    ///   assert_eq!(*value2, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries_mut(
        &mut self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<WriteGuardedView<W>>, ViewError> {
        let updates = self.updates.get_mut();
        let entries = Self::do_load_entries(
            &self.context,
            updates,
            self.delete_storage_first,
            short_keys,
        )
        .await?;
        entries
            .into_iter()
            .map(|(short_key, view)| {
                Ok(WriteGuardedView(
                    view.try_write_arc()
                        .ok_or_else(|| ViewError::TryLockError(short_key))?,
                ))
            })
            .collect()
    }

    /// Load multiple entries for reading at once.
    /// The entries in short_keys have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantByteCollectionView<_, RegisterView<_,String>> = ReentrantByteCollectionView::load(context).await.unwrap();
    ///   let short_keys = vec![vec![0, 1], vec![2, 3]];
    ///   let subviews = view.try_load_entries(short_keys).await.unwrap();
    ///   let value1 = subviews[0].get();
    ///   let value2 = subviews[1].get();
    ///   assert_eq!(*value1, String::default());
    ///   assert_eq!(*value2, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries(
        &self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<ReadGuardedView<W>>, ViewError> {
        let mut updates = self.updates.lock().await;
        let entries = Self::do_load_entries(
            &self.context,
            &mut updates,
            self.delete_storage_first,
            short_keys,
        )
        .await?;
        entries
            .into_iter()
            .map(|(short_key, view)| {
                Ok(ReadGuardedView(
                    view.try_read_arc()
                        .ok_or_else(|| ViewError::TryLockError(short_key))?,
                ))
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
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantByteCollectionView<_, RegisterView<_,String>> = ReentrantByteCollectionView::load(context).await.unwrap();
    ///   view.try_load_entry_mut(&[0, 1]).await.unwrap();
    ///   view.try_load_entry_mut(&[0, 2]).await.unwrap();
    ///   let keys = view.keys().await.unwrap();
    ///   assert_eq!(keys, vec![vec![0, 1], vec![0, 2]]);
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

    /// Applies a function f on each index (aka key). Keys are visited in a
    /// lexicographic order. If the function returns false then the loop
    /// ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantByteCollectionView<_, RegisterView<_,String>> = ReentrantByteCollectionView::load(context).await.unwrap();
    ///   view.try_load_entry_mut(&[0, 1]).await.unwrap();
    ///   view.try_load_entry_mut(&[0, 2]).await.unwrap();
    ///   let mut count = 0;
    ///   view.for_each_key_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 1)
    ///   }).await.unwrap();
    ///   assert_eq!(count, 1);
    /// # })
    /// ```
    pub async fn for_each_key_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let updates = self.updates.lock().await;
        let mut updates = updates.iter();
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
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantByteCollectionView<_, RegisterView<_,String>> = ReentrantByteCollectionView::load(context).await.unwrap();
    ///   view.try_load_entry_mut(&[0, 1]).await.unwrap();
    ///   view.try_load_entry_mut(&[0, 2]).await.unwrap();
    ///   let mut count = 0;
    ///   view.for_each_key(|_key| {
    ///     count += 1;
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(count, 2);
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
        hasher.update_with_bcs_bytes(&keys.len())?;
        let updates = self.updates.get_mut();
        for key in keys {
            hasher.update_with_bytes(&key)?;
            let hash = match updates.get_mut(&key) {
                Some(entry) => {
                    let Update::Set(view) = entry else {
                        unreachable!();
                    };
                    let mut view = view
                        .try_write_arc()
                        .ok_or_else(|| ViewError::TryLockError(key))?;
                    view.hash_mut().await?
                }
                None => {
                    let key = self.context.base_tag_index(KeyTag::Subview as u8, &key);
                    let context = self.context.clone_with_base_key(key);
                    let mut view = W::load(context).await?;
                    view.hash_mut().await?
                }
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
        hasher.update_with_bcs_bytes(&keys.len())?;
        let updates = self.updates.lock().await;
        for key in keys {
            hasher.update_with_bytes(&key)?;
            let hash = match updates.get(&key) {
                Some(entry) => {
                    let Update::Set(view) = entry else {
                        unreachable!();
                    };
                    let view = view
                        .try_read_arc()
                        .ok_or_else(|| ViewError::TryLockError(key))?;
                    view.hash().await?
                }
                None => {
                    let key = self.context.base_tag_index(KeyTag::Subview as u8, &key);
                    let context = self.context.clone_with_base_key(key);
                    let view = W::load(context).await?;
                    view.hash().await?
                }
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
    I: Send + Sync + Debug + Serialize + DeserializeOwned,
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
    I: Send + Sync + Debug + Serialize + DeserializeOwned,
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
    I: Sync + Clone + Send + Debug + Serialize + DeserializeOwned,
    W: View<C> + Send + Sync,
{
    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then a default entry is put on the collection. The obtained view can
    /// then be modified.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   let subview = view.try_load_entry_mut(&23).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
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
    /// If an entry is absent, then a default entry is inserted into the collection. The obtained view
    /// cannot be modified.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   let subview = view.try_load_entry_or_insert(&23).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry_or_insert<Q>(
        &mut self,
        index: &Q,
    ) -> Result<ReadGuardedView<W>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.try_load_entry_or_insert(&short_key).await
    }

    /// Loads a subview at the given index in the collection and gives read-only access to the data.
    /// If an entry is absent then `None` is returned.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   let _subview = view.try_load_entry_or_insert(&23).await.unwrap();
    ///   let subview = view.try_load_entry(&23).await.unwrap().unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
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
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   let _subview = view.try_load_entry_or_insert(&23).await.unwrap();
    ///   assert!(view.contains_key(&23).await.unwrap());
    ///   assert!(!view.contains_key(&24).await.unwrap());
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
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   let mut subview = view.try_load_entry_mut(&23).await.unwrap();
    ///   let value = subview.get_mut();
    ///   assert_eq!(*value, String::default());
    ///   view.remove_entry(&23);
    ///   let keys = view.indices().await.unwrap();
    ///   assert_eq!(keys.len(), 0);
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
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   {
    ///     let mut subview = view.try_load_entry_mut(&23).await.unwrap();
    ///     let value = subview.get_mut();
    ///     *value = String::from("Hello");
    ///   }
    ///   view.try_reset_entry_to_default(&23).unwrap();
    ///   let mut subview = view.try_load_entry_mut(&23).await.unwrap();
    ///   let value = subview.get_mut();
    ///   assert_eq!(*value, String::default());
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
    I: Sync + Clone + Send + Debug + Serialize + DeserializeOwned,
    W: View<C> + Send + Sync + 'static,
{
    /// Load multiple entries for writing at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   let indices = vec![23, 42];
    ///   let subviews = view.try_load_entries_mut(&indices).await.unwrap();
    ///   let value1 = subviews[0].get();
    ///   let value2 = subviews[1].get();
    ///   assert_eq!(*value1, String::default());
    ///   assert_eq!(*value2, String::default());
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
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   let indices = vec![23, 42];
    ///   let subviews = view.try_load_entries(&indices).await.unwrap();
    ///   let value1 = subviews[0].get();
    ///   let value2 = subviews[1].get();
    ///   assert_eq!(*value1, String::default());
    ///   assert_eq!(*value2, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries<'a, Q>(
        &'a self,
        indices: impl IntoIterator<Item = &'a Q>,
    ) -> Result<Vec<ReadGuardedView<W>>, ViewError>
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
}

impl<C, I, W> ReentrantCollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Debug + Serialize + DeserializeOwned,
    W: View<C> + Send + Sync,
{
    /// Returns the list of indices in the collection in an order determined
    /// by serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   view.try_load_entry_mut(&23).await.unwrap();
    ///   view.try_load_entry_mut(&25).await.unwrap();
    ///   let indices = view.indices().await.unwrap();
    ///   assert_eq!(indices.len(), 2);
    /// # })
    /// ```
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization. If the function f returns false then
    /// the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   view.try_load_entry_mut(&23).await.unwrap();
    ///   view.try_load_entry_mut(&24).await.unwrap();
    ///   let mut count = 0;
    ///   view.for_each_index_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 1)
    ///   }).await.unwrap();
    ///   assert_eq!(count, 1);
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
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   view.try_load_entry_mut(&23).await.unwrap();
    ///   view.try_load_entry_mut(&28).await.unwrap();
    ///   let mut count = 0;
    ///   view.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(count, 2);
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
    I: Debug + Send + Sync + Serialize + DeserializeOwned,
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
    I: Send + Sync + Debug + CustomSerialize,
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
    I: Send + Sync + Debug + CustomSerialize,
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
    I: Sync + Clone + Send + Debug + CustomSerialize,
    W: View<C> + Send + Sync,
{
    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then a default entry is put in the collection on this index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   let subview = view.try_load_entry_mut(&23).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
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
    /// If an entry is absent before then a default entry is put in the collection on this index.
    /// The obtained view cannot be modified.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   let subview = view.try_load_entry_or_insert(&23).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry_or_insert<Q>(
        &mut self,
        index: &Q,
    ) -> Result<ReadGuardedView<W>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.try_load_entry_or_insert(&short_key).await
    }

    /// Loads a subview at the given index in the collection and gives read-only access to the data.
    /// If an entry is absent then `None` is returned.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   let _subview = view.try_load_entry_or_insert(&23).await.unwrap();
    ///   let subview = view.try_load_entry(&23).await.unwrap().unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
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
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   let _subview = view.try_load_entry_or_insert(&23).await.unwrap();
    ///   assert!(view.contains_key(&23).await.unwrap());
    ///   assert!(!view.contains_key(&24).await.unwrap());
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
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   let mut subview = view.try_load_entry_mut(&23).await.unwrap();
    ///   let value = subview.get_mut();
    ///   assert_eq!(*value, String::default());
    ///   view.remove_entry(&23);
    ///   let keys = view.indices().await.unwrap();
    ///   assert_eq!(keys.len(), 0);
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
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   {
    ///     let mut subview = view.try_load_entry_mut(&23).await.unwrap();
    ///     let value = subview.get_mut();
    ///     *value = String::from("Hello");
    ///   }
    ///   {
    ///     view.try_reset_entry_to_default(&23).unwrap();
    ///     let subview = view.try_load_entry(&23).await.unwrap().unwrap();
    ///     let value = subview.get();
    ///     assert_eq!(*value, String::default());
    ///   }
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
    I: Sync + Clone + Send + Debug + Serialize + DeserializeOwned,
    W: View<C> + Send + Sync + 'static,
{
    /// Load multiple entries for writing at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   let indices = vec![23, 42];
    ///   let subviews = view.try_load_entries_mut(indices).await.unwrap();
    ///   let value1 = subviews[0].get();
    ///   let value2 = subviews[1].get();
    ///   assert_eq!(*value1, String::default());
    ///   assert_eq!(*value2, String::default());
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
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   let indices = vec![23, 42];
    ///   let subviews = view.try_load_entries(indices).await.unwrap();
    ///   let value1 = subviews[0].get();
    ///   let value2 = subviews[1].get();
    ///   assert_eq!(*value1, String::default());
    ///   assert_eq!(*value2, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries<Q>(
        &self,
        indices: impl IntoIterator<Item = Q>,
    ) -> Result<Vec<ReadGuardedView<W>>, ViewError>
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
}

impl<C, I, W> ReentrantCustomCollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Debug + CustomSerialize,
    W: View<C> + Send + Sync,
{
    /// Returns the list of indices in the collection. The order is determined by
    /// the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   view.try_load_entry_mut(&23).await.unwrap();
    ///   view.try_load_entry_mut(&25).await.unwrap();
    ///   let indices = view.indices().await.unwrap();
    ///   assert_eq!(indices, vec![23, 25]);
    /// # })
    /// ```
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization. If the function f returns false
    /// then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   view.try_load_entry_mut(&28).await.unwrap();
    ///   view.try_load_entry_mut(&24).await.unwrap();
    ///   view.try_load_entry_mut(&23).await.unwrap();
    ///   let mut part_indices = Vec::new();
    ///   view.for_each_index_while(|index| {
    ///     part_indices.push(index);
    ///     Ok(part_indices.len() < 2)
    ///   }).await.unwrap();
    ///   assert_eq!(part_indices, vec![23, 24]);
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
    /// # use linera_views::memory::{create_memory_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   view.try_load_entry_mut(&28).await.unwrap();
    ///   view.try_load_entry_mut(&24).await.unwrap();
    ///   view.try_load_entry_mut(&23).await.unwrap();
    ///   let mut indices = Vec::new();
    ///   view.for_each_index(|index| {
    ///     indices.push(index);
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(indices, vec![23, 24, 28]);
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
    I: Debug + Send + Sync + CustomSerialize,
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
