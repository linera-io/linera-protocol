// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Borrow,
    collections::{btree_map, BTreeMap},
    io::Write,
    marker::PhantomData,
    mem,
    ops::Deref,
};

use allocative::{Allocative, Key, Visitor};
use async_lock::{RwLock, RwLockReadGuard};
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::{CustomSerialize, HasherOutput, SliceExt as _, Update},
    context::{BaseKey, Context},
    hashable_wrapper::WrappedHashableContainerView,
    historical_hash_wrapper::HistoricallyHashableView,
    store::ReadableKeyValueStore as _,
    views::{ClonableView, HashableView, Hasher, View, ViewError, MIN_VIEW_TAG},
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    /// The runtime of hash computation
    pub static COLLECTION_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "collection_view_hash_runtime",
            "CollectionView hash runtime",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });
}

/// A view that supports accessing a collection of views of the same kind, indexed by a
/// `Vec<u8>`, one subview at a time.
#[derive(Debug)]
pub struct ByteCollectionView<C, W> {
    /// The view context.
    context: C,
    /// Whether to clear storage before applying updates.
    delete_storage_first: bool,
    /// Entries that may have staged changes.
    updates: RwLock<BTreeMap<Vec<u8>, Update<W>>>,
}

impl<C, W: Allocative> Allocative for ByteCollectionView<C, W> {
    fn visit<'a, 'b: 'a>(&self, visitor: &'a mut Visitor<'b>) {
        let name = Key::new("ByteCollectionView");
        let size = mem::size_of::<Self>();
        let mut visitor = visitor.enter(name, size);
        if let Some(updates) = self.updates.try_read() {
            updates.deref().visit(&mut visitor);
        }
        visitor.exit();
    }
}

/// A read-only accessor for a particular subview in a [`CollectionView`].
pub enum ReadGuardedView<'a, W> {
    /// The view is loaded in the updates
    Loaded {
        /// The guard for the updates.
        updates: RwLockReadGuard<'a, BTreeMap<Vec<u8>, Update<W>>>,
        /// The key in question.
        short_key: Vec<u8>,
    },
    /// The view is not loaded in the updates
    NotLoaded {
        /// The guard for the updates. It is needed so that it prevents
        /// opening the view as write separately.
        _updates: RwLockReadGuard<'a, BTreeMap<Vec<u8>, Update<W>>>,
        /// The view obtained from the storage
        view: W,
    },
}

impl<W> std::ops::Deref for ReadGuardedView<'_, W> {
    type Target = W;

    fn deref(&self) -> &W {
        match self {
            ReadGuardedView::Loaded { updates, short_key } => {
                let Update::Set(view) = updates.get(short_key).unwrap() else {
                    unreachable!();
                };
                view
            }
            ReadGuardedView::NotLoaded { _updates, view } => view,
        }
    }
}

/// We need to find new base keys in order to implement `CollectionView`.
/// We do this by appending a value to the base key.
///
/// Sub-views in a collection share a common key prefix, like in other view types. However,
/// just concatenating the shared prefix with sub-view keys makes it impossible to distinguish if a
/// given key belongs to child sub-view or a grandchild sub-view (consider for example if a
/// collection is stored inside the collection).
#[repr(u8)]
enum KeyTag {
    /// Prefix for specifying an index and serves to indicate the existence of an entry in the collection.
    Index = MIN_VIEW_TAG,
    /// Prefix for specifying as the prefix for the sub-view.
    Subview,
}

impl<W: View> View for ByteCollectionView<W::Context, W> {
    const NUM_INIT_KEYS: usize = 0;

    type Context = W::Context;

    fn context(&self) -> Self::Context {
        self.context.clone()
    }

    fn pre_load(_context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![])
    }

    fn post_load(context: Self::Context, _values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        Ok(Self {
            context,
            delete_storage_first: false,
            updates: RwLock::new(BTreeMap::new()),
        })
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.updates.get_mut().clear();
    }

    async fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        let updates = self.updates.read().await;
        !updates.is_empty()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        let updates = self
            .updates
            .try_read()
            .ok_or_else(|| ViewError::TryLockError(vec![]))?;
        if self.delete_storage_first {
            delete_view = true;
            batch.delete_key_prefix(self.context.base_key().bytes.clone());
            for (index, update) in updates.iter() {
                if let Update::Set(view) = update {
                    view.pre_save(batch)?;
                    self.add_index(batch, index);
                    delete_view = false;
                }
            }
        } else {
            for (index, update) in updates.iter() {
                match update {
                    Update::Set(view) => {
                        view.pre_save(batch)?;
                        self.add_index(batch, index);
                    }
                    Update::Removed => {
                        let key_subview = self.get_subview_key(index);
                        let key_index = self.get_index_key(index);
                        batch.delete_key(key_index);
                        batch.delete_key_prefix(key_subview);
                    }
                }
            }
        }
        Ok(delete_view)
    }

    fn post_save(&mut self) {
        for (_, update) in self.updates.get_mut().iter_mut() {
            if let Update::Set(view) = update {
                view.post_save();
            }
        }
        self.delete_storage_first = false;
        self.updates.get_mut().clear();
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.updates.get_mut().clear();
    }
}

impl<W: ClonableView> ClonableView for ByteCollectionView<W::Context, W> {
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        let cloned_updates = self
            .updates
            .get_mut()
            .iter_mut()
            .map(|(key, value)| {
                let cloned_value: Result<_, ViewError> = match value {
                    Update::Removed => Ok(Update::Removed),
                    Update::Set(view) => Ok(Update::Set(view.clone_unchecked()?)),
                };
                cloned_value.map(|v| (key.clone(), v))
            })
            .collect::<Result<_, ViewError>>()?;

        Ok(ByteCollectionView {
            context: self.context.clone(),
            delete_storage_first: self.delete_storage_first,
            updates: RwLock::new(cloned_updates),
        })
    }
}

impl<W: View> ByteCollectionView<W::Context, W> {
    fn get_index_key(&self, index: &[u8]) -> Vec<u8> {
        self.context
            .base_key()
            .base_tag_index(KeyTag::Index as u8, index)
    }

    fn get_subview_key(&self, index: &[u8]) -> Vec<u8> {
        self.context
            .base_key()
            .base_tag_index(KeyTag::Subview as u8, index)
    }

    fn add_index(&self, batch: &mut Batch, index: &[u8]) {
        let key = self.get_index_key(index);
        batch.put_key_value_bytes(key, vec![]);
    }

    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then a default entry is added to the collection. The resulting view
    /// can be modified.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: ByteCollectionView<_, RegisterView<_, String>> =
    ///     ByteCollectionView::load(context).await.unwrap();
    /// let subview = view.load_entry_mut(&[0, 1]).await.unwrap();
    /// let value = subview.get();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn load_entry_mut(&mut self, short_key: &[u8]) -> Result<&mut W, ViewError> {
        match self.updates.get_mut().entry(short_key.to_vec()) {
            btree_map::Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    Update::Set(view) => Ok(view),
                    Update::Removed => {
                        let key = self
                            .context
                            .base_key()
                            .base_tag_index(KeyTag::Subview as u8, short_key);
                        let context = self.context.clone_with_base_key(key);
                        // Obtain a view and set its pending state to the default (e.g. empty) state
                        let view = W::new(context)?;
                        *entry = Update::Set(view);
                        let Update::Set(view) = entry else {
                            unreachable!();
                        };
                        Ok(view)
                    }
                }
            }
            btree_map::Entry::Vacant(entry) => {
                let key = self
                    .context
                    .base_key()
                    .base_tag_index(KeyTag::Subview as u8, short_key);
                let context = self.context.clone_with_base_key(key);
                let view = if self.delete_storage_first {
                    W::new(context)?
                } else {
                    W::load(context).await?
                };
                let Update::Set(view) = entry.insert(Update::Set(view)) else {
                    unreachable!();
                };
                Ok(view)
            }
        }
    }

    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then `None` is returned. The resulting view cannot be modified.
    /// May fail if one subview is already being visited.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: ByteCollectionView<_, RegisterView<_, String>> =
    ///     ByteCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&[0, 1]).await.unwrap();
    /// }
    /// {
    ///     let subview = view.try_load_entry(&[0, 1]).await.unwrap().unwrap();
    ///     let value = subview.get();
    ///     assert_eq!(*value, String::default());
    /// }
    /// assert!(view.try_load_entry(&[0, 2]).await.unwrap().is_none());
    /// # })
    /// ```
    pub async fn try_load_entry(
        &self,
        short_key: &[u8],
    ) -> Result<Option<ReadGuardedView<'_, W>>, ViewError> {
        let updates = self.updates.read().await;
        match updates.get(short_key) {
            Some(update) => match update {
                Update::Removed => Ok(None),
                Update::Set(_) => Ok(Some(ReadGuardedView::Loaded {
                    updates,
                    short_key: short_key.to_vec(),
                })),
            },
            None => {
                let key_index = self
                    .context
                    .base_key()
                    .base_tag_index(KeyTag::Index as u8, short_key);
                if !self.delete_storage_first
                    && self.context.store().contains_key(&key_index).await?
                {
                    let key = self
                        .context
                        .base_key()
                        .base_tag_index(KeyTag::Subview as u8, short_key);
                    let context = self.context.clone_with_base_key(key);
                    let view = W::load(context).await?;
                    Ok(Some(ReadGuardedView::NotLoaded {
                        _updates: updates,
                        view,
                    }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Load multiple entries for reading at once.
    /// The entries in `short_keys` have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: ByteCollectionView<_, RegisterView<_, String>> =
    ///     ByteCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&[0, 1]).await.unwrap();
    /// }
    /// let short_keys = vec![vec![0, 1], vec![2, 3]];
    /// let subviews = view.try_load_entries(short_keys).await.unwrap();
    /// let value0 = subviews[0].as_ref().unwrap().get();
    /// assert_eq!(*value0, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries(
        &self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<ReadGuardedView<'_, W>>>, ViewError> {
        let mut results = Vec::with_capacity(short_keys.len());
        let mut keys_to_check = Vec::new();
        let mut keys_to_check_metadata = Vec::new();
        let updates = self.updates.read().await;

        for (position, short_key) in short_keys.into_iter().enumerate() {
            match updates.get(&short_key) {
                Some(update) => match update {
                    Update::Removed => {
                        results.push(None);
                    }
                    Update::Set(_) => {
                        let updates = self.updates.read().await;
                        results.push(Some(ReadGuardedView::Loaded {
                            updates,
                            short_key: short_key.clone(),
                        }));
                    }
                },
                None => {
                    results.push(None); // Placeholder, may be updated later
                    if !self.delete_storage_first {
                        let key = self
                            .context
                            .base_key()
                            .base_tag_index(KeyTag::Subview as u8, &short_key);
                        let subview_context = self.context.clone_with_base_key(key);
                        let key = self
                            .context
                            .base_key()
                            .base_tag_index(KeyTag::Index as u8, &short_key);
                        keys_to_check.push(key);
                        keys_to_check_metadata.push((position, subview_context));
                    }
                }
            }
        }

        let found_keys = self.context.store().contains_keys(&keys_to_check).await?;
        let entries_to_load = keys_to_check_metadata
            .into_iter()
            .zip(found_keys)
            .filter_map(|(metadata, found)| found.then_some(metadata))
            .collect::<Vec<_>>();

        let mut keys_to_load = Vec::with_capacity(entries_to_load.len() * W::NUM_INIT_KEYS);
        for (_, context) in &entries_to_load {
            keys_to_load.extend(W::pre_load(context)?);
        }
        let values = self
            .context
            .store()
            .read_multi_values_bytes(&keys_to_load)
            .await?;

        for (loaded_values, (position, context)) in values
            .chunks_exact_or_repeat(W::NUM_INIT_KEYS)
            .zip(entries_to_load)
        {
            let view = W::post_load(context, loaded_values)?;
            let updates = self.updates.read().await;
            results[position] = Some(ReadGuardedView::NotLoaded {
                _updates: updates,
                view,
            });
        }

        Ok(results)
    }

    /// Loads multiple entries for reading at once with their keys.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: ByteCollectionView<_, RegisterView<_, String>> =
    ///     ByteCollectionView::load(context).await.unwrap();
    /// {
    ///     let subview = view.load_entry_mut(&vec![0, 1]).await.unwrap();
    ///     subview.set("Bonjour".into());
    /// }
    /// let short_keys = vec![vec![0, 1], vec![0, 2]];
    /// let pairs = view.try_load_entries_pairs(short_keys).await.unwrap();
    /// assert_eq!(pairs[0].0, vec![0, 1]);
    /// assert_eq!(pairs[1].0, vec![0, 2]);
    /// let value0 = pairs[0].1.as_ref().unwrap().get();
    /// assert_eq!(*value0, "Bonjour".to_string());
    /// assert!(pairs[1].1.is_none());
    /// # })
    /// ```
    pub async fn try_load_entries_pairs(
        &self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Option<ReadGuardedView<'_, W>>)>, ViewError> {
        let values = self.try_load_entries(short_keys.clone()).await?;
        Ok(short_keys.into_iter().zip(values).collect())
    }

    /// Load all entries for reading at once.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: ByteCollectionView<_, RegisterView<_, String>> =
    ///     ByteCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&[0, 1]).await.unwrap();
    /// }
    /// let subviews = view.try_load_all_entries().await.unwrap();
    /// assert_eq!(subviews.len(), 1);
    /// # })
    /// ```
    pub async fn try_load_all_entries(
        &self,
    ) -> Result<Vec<(Vec<u8>, ReadGuardedView<'_, W>)>, ViewError> {
        let updates = self.updates.read().await; // Acquire the read lock to prevent writes.
        let short_keys = self.keys().await?;
        let mut results = Vec::with_capacity(short_keys.len());

        let mut keys_to_load = Vec::new();
        let mut keys_to_load_metadata = Vec::new();
        for (position, short_key) in short_keys.iter().enumerate() {
            match updates.get(short_key) {
                Some(update) => {
                    let Update::Set(_) = update else {
                        unreachable!();
                    };
                    let updates = self.updates.read().await;
                    let view = ReadGuardedView::Loaded {
                        updates,
                        short_key: short_key.clone(),
                    };
                    results.push((short_key.clone(), Some(view)));
                }
                None => {
                    // If a key is not in `updates`, then it is in storage.
                    // The key exists since otherwise it would not be in `short_keys`.
                    // Therefore we have `self.delete_storage_first = false`.
                    assert!(!self.delete_storage_first);
                    results.push((short_key.clone(), None));
                    let key = self
                        .context
                        .base_key()
                        .base_tag_index(KeyTag::Subview as u8, short_key);
                    let subview_context = self.context.clone_with_base_key(key);
                    keys_to_load.extend(W::pre_load(&subview_context)?);
                    keys_to_load_metadata.push((position, subview_context, short_key.clone()));
                }
            }
        }

        let values = self
            .context
            .store()
            .read_multi_values_bytes(&keys_to_load)
            .await?;

        for (loaded_values, (position, context, short_key)) in values
            .chunks_exact_or_repeat(W::NUM_INIT_KEYS)
            .zip(keys_to_load_metadata)
        {
            let view = W::post_load(context, loaded_values)?;
            let updates = self.updates.read().await;
            let guarded_view = ReadGuardedView::NotLoaded {
                _updates: updates,
                view,
            };
            results[position] = (short_key, Some(guarded_view));
        }

        Ok(results
            .into_iter()
            .map(|(short_key, view)| (short_key, view.unwrap()))
            .collect::<Vec<_>>())
    }

    /// Resets an entry to the default value.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: ByteCollectionView<_, RegisterView<_, String>> =
    ///     ByteCollectionView::load(context).await.unwrap();
    /// let subview = view.load_entry_mut(&[0, 1]).await.unwrap();
    /// let value = subview.get_mut();
    /// *value = String::from("Hello");
    /// view.reset_entry_to_default(&[0, 1]).unwrap();
    /// let subview = view.load_entry_mut(&[0, 1]).await.unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub fn reset_entry_to_default(&mut self, short_key: &[u8]) -> Result<(), ViewError> {
        let key = self
            .context
            .base_key()
            .base_tag_index(KeyTag::Subview as u8, short_key);
        let context = self.context.clone_with_base_key(key);
        let view = W::new(context)?;
        self.updates
            .get_mut()
            .insert(short_key.to_vec(), Update::Set(view));
        Ok(())
    }

    /// Tests if the collection contains a specified key and returns a boolean.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: ByteCollectionView<_, RegisterView<_, String>> =
    ///     ByteCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&[0, 1]).await.unwrap();
    /// }
    /// assert!(view.contains_key(&[0, 1]).await.unwrap());
    /// assert!(!view.contains_key(&[0, 2]).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key(&self, short_key: &[u8]) -> Result<bool, ViewError> {
        let updates = self.updates.read().await;
        Ok(match updates.get(short_key) {
            Some(entry) => match entry {
                Update::Set(_view) => true,
                _entry @ Update::Removed => false,
            },
            None => {
                let key_index = self
                    .context
                    .base_key()
                    .base_tag_index(KeyTag::Index as u8, short_key);
                !self.delete_storage_first && self.context.store().contains_key(&key_index).await?
            }
        })
    }

    /// Marks the entry as removed. If absent then nothing is done.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: ByteCollectionView<_, RegisterView<_, String>> =
    ///     ByteCollectionView::load(context).await.unwrap();
    /// let subview = view.load_entry_mut(&[0, 1]).await.unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// view.remove_entry(vec![0, 1]);
    /// let keys = view.keys().await.unwrap();
    /// assert_eq!(keys.len(), 0);
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

    /// Gets the extra data.
    pub fn extra(&self) -> &<W::Context as Context>::Extra {
        self.context.extra()
    }
}

impl<W: View> ByteCollectionView<W::Context, W> {
    /// Applies a function f on each index (aka key). Keys are visited in the
    /// lexicographic order. If the function returns false, then the loop
    /// ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: ByteCollectionView<_, RegisterView<_, String>> =
    ///     ByteCollectionView::load(context).await.unwrap();
    /// view.load_entry_mut(&[0, 1]).await.unwrap();
    /// view.load_entry_mut(&[0, 2]).await.unwrap();
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
        let updates = self.updates.read().await;
        let mut updates = updates.iter();
        let mut update = updates.next();
        if !self.delete_storage_first {
            let base = self.get_index_key(&[]);
            for index in self.context.store().find_keys_by_prefix(&base).await? {
                loop {
                    match update {
                        Some((key, value)) if key <= &index => {
                            if let Update::Set(_) = value {
                                if !f(key)? {
                                    return Ok(());
                                }
                            }
                            update = updates.next();
                            if key == &index {
                                break;
                            }
                        }
                        _ => {
                            if !f(&index)? {
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: ByteCollectionView<_, RegisterView<_, String>> =
    ///     ByteCollectionView::load(context).await.unwrap();
    /// view.load_entry_mut(&[0, 1]).await.unwrap();
    /// view.load_entry_mut(&[0, 2]).await.unwrap();
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

    /// Returns the list of keys in the collection. The order is lexicographic.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: ByteCollectionView<_, RegisterView<_, String>> =
    ///     ByteCollectionView::load(context).await.unwrap();
    /// view.load_entry_mut(&[0, 1]).await.unwrap();
    /// view.load_entry_mut(&[0, 2]).await.unwrap();
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

    /// Returns the number of entries in the collection.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: ByteCollectionView<_, RegisterView<_, String>> =
    ///     ByteCollectionView::load(context).await.unwrap();
    /// view.load_entry_mut(&[0, 1]).await.unwrap();
    /// view.load_entry_mut(&[0, 2]).await.unwrap();
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
}

impl<W: HashableView> HashableView for ByteCollectionView<W::Context, W> {
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = metrics::COLLECTION_VIEW_HASH_RUNTIME.measure_latency();
        let mut hasher = sha3::Sha3_256::default();
        let keys = self.keys().await?;
        let count = keys.len() as u32;
        hasher.update_with_bcs_bytes(&count)?;
        let updates = self.updates.get_mut();
        for key in keys {
            hasher.update_with_bytes(&key)?;
            let hash = match updates.get_mut(&key) {
                Some(entry) => {
                    let Update::Set(view) = entry else {
                        unreachable!();
                    };
                    view.hash_mut().await?
                }
                None => {
                    let key = self
                        .context
                        .base_key()
                        .base_tag_index(KeyTag::Subview as u8, &key);
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
        let _hash_latency = metrics::COLLECTION_VIEW_HASH_RUNTIME.measure_latency();
        let mut hasher = sha3::Sha3_256::default();
        let updates = self.updates.read().await; // Acquire the lock to prevent writes.
        let keys = self.keys().await?;
        let count = keys.len() as u32;
        hasher.update_with_bcs_bytes(&count)?;
        for key in keys {
            hasher.update_with_bytes(&key)?;
            let hash = match updates.get(&key) {
                Some(entry) => {
                    let Update::Set(view) = entry else {
                        unreachable!();
                    };
                    view.hash().await?
                }
                None => {
                    let key = self
                        .context
                        .base_key()
                        .base_tag_index(KeyTag::Subview as u8, &key);
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

/// A view that supports accessing a collection of views of the same kind, indexed by a
/// key, one subview at a time.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I, W: Allocative")]
pub struct CollectionView<C, I, W> {
    collection: ByteCollectionView<C, W>,
    #[allocative(skip)]
    _phantom: PhantomData<I>,
}

impl<W: View, I> View for CollectionView<W::Context, I, W>
where
    I: Send + Sync + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = ByteCollectionView::<W::Context, W>::NUM_INIT_KEYS;

    type Context = W::Context;

    fn context(&self) -> Self::Context {
        self.collection.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        ByteCollectionView::<W::Context, W>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let collection = ByteCollectionView::post_load(context, values)?;
        Ok(CollectionView {
            collection,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.collection.rollback()
    }

    async fn has_pending_changes(&self) -> bool {
        self.collection.has_pending_changes().await
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.collection.pre_save(batch)
    }

    fn post_save(&mut self) {
        self.collection.post_save()
    }

    fn clear(&mut self) {
        self.collection.clear()
    }
}

impl<I, W: ClonableView> ClonableView for CollectionView<W::Context, I, W>
where
    I: Send + Sync + Serialize + DeserializeOwned,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(CollectionView {
            collection: self.collection.clone_unchecked()?,
            _phantom: PhantomData,
        })
    }
}

impl<I: Serialize, W: View> CollectionView<W::Context, I, W> {
    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then a default entry is added to the collection. The resulting view
    /// can be modified.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CollectionView<_, u64, RegisterView<_, String>> =
    ///     CollectionView::load(context).await.unwrap();
    /// let subview = view.load_entry_mut(&23).await.unwrap();
    /// let value = subview.get();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn load_entry_mut<Q>(&mut self, index: &Q) -> Result<&mut W, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.collection.load_entry_mut(&short_key).await
    }

    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then `None` is returned. The resulting view cannot be modified.
    /// May fail if one subview is already being visited.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CollectionView<_, u64, RegisterView<_, String>> =
    ///     CollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).await.unwrap();
    /// }
    /// {
    ///     let subview = view.try_load_entry(&23).await.unwrap().unwrap();
    ///     let value = subview.get();
    ///     assert_eq!(*value, String::default());
    /// }
    /// assert!(view.try_load_entry(&24).await.unwrap().is_none());
    /// # })
    /// ```
    pub async fn try_load_entry<Q>(
        &self,
        index: &Q,
    ) -> Result<Option<ReadGuardedView<'_, W>>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.collection.try_load_entry(&short_key).await
    }

    /// Load multiple entries for reading at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CollectionView<_, u64, RegisterView<_, String>> =
    ///     CollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).await.unwrap();
    /// }
    /// let indices = vec![23, 24];
    /// let subviews = view.try_load_entries(&indices).await.unwrap();
    /// let value0 = subviews[0].as_ref().unwrap().get();
    /// assert_eq!(*value0, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries<'a, Q>(
        &self,
        indices: impl IntoIterator<Item = &'a Q>,
    ) -> Result<Vec<Option<ReadGuardedView<'_, W>>>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + 'a,
    {
        let short_keys = indices
            .into_iter()
            .map(|index| BaseKey::derive_short_key(index))
            .collect::<Result<_, _>>()?;
        self.collection.try_load_entries(short_keys).await
    }

    /// Loads multiple entries for reading at once with their keys.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CollectionView<_, u64, RegisterView<_, String>> =
    ///     CollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).await.unwrap();
    /// }
    /// let indices = [23, 24];
    /// let subviews = view.try_load_entries_pairs(indices).await.unwrap();
    /// let value0 = subviews[0].1.as_ref().unwrap().get();
    /// assert_eq!(*value0, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries_pairs<Q>(
        &self,
        indices: impl IntoIterator<Item = Q>,
    ) -> Result<Vec<(Q, Option<ReadGuardedView<'_, W>>)>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + Clone,
    {
        let indices_vec: Vec<Q> = indices.into_iter().collect();
        let values = self.try_load_entries(indices_vec.iter()).await?;
        Ok(indices_vec.into_iter().zip(values).collect())
    }

    /// Load all entries for reading at once.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CollectionView<_, u64, RegisterView<_, String>> =
    ///     CollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).await.unwrap();
    /// }
    /// let subviews = view.try_load_all_entries().await.unwrap();
    /// assert_eq!(subviews.len(), 1);
    /// # })
    /// ```
    pub async fn try_load_all_entries(&self) -> Result<Vec<(I, ReadGuardedView<'_, W>)>, ViewError>
    where
        I: DeserializeOwned,
    {
        let results = self.collection.try_load_all_entries().await?;
        results
            .into_iter()
            .map(|(short_key, view)| {
                let index = BaseKey::deserialize_value(&short_key)?;
                Ok((index, view))
            })
            .collect()
    }

    /// Resets an entry to the default value.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CollectionView<_, u64, RegisterView<_, String>> =
    ///     CollectionView::load(context).await.unwrap();
    /// let subview = view.load_entry_mut(&23).await.unwrap();
    /// let value = subview.get_mut();
    /// *value = String::from("Hello");
    /// view.reset_entry_to_default(&23).unwrap();
    /// let subview = view.load_entry_mut(&23).await.unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub fn reset_entry_to_default<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.collection.reset_entry_to_default(&short_key)
    }

    /// Removes an entry from the `CollectionView`. If absent nothing happens.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CollectionView<_, u64, RegisterView<_, String>> =
    ///     CollectionView::load(context).await.unwrap();
    /// let subview = view.load_entry_mut(&23).await.unwrap();
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
        let short_key = BaseKey::derive_short_key(index)?;
        self.collection.remove_entry(short_key);
        Ok(())
    }

    /// Gets the extra data.
    pub fn extra(&self) -> &<W::Context as Context>::Extra {
        self.collection.extra()
    }
}

impl<I, W: View> CollectionView<W::Context, I, W>
where
    I: Sync + Send + Serialize + DeserializeOwned,
{
    /// Returns the list of indices in the collection in the order determined by
    /// the serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CollectionView<_, u64, RegisterView<_, String>> =
    ///     CollectionView::load(context).await.unwrap();
    /// view.load_entry_mut(&23).await.unwrap();
    /// view.load_entry_mut(&25).await.unwrap();
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

    /// Returns the number of entries in the collection.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CollectionView<_, u64, RegisterView<_, String>> =
    ///     CollectionView::load(context).await.unwrap();
    /// view.load_entry_mut(&23).await.unwrap();
    /// view.load_entry_mut(&25).await.unwrap();
    /// assert_eq!(view.count().await.unwrap(), 2);
    /// # })
    /// ```
    pub async fn count(&self) -> Result<usize, ViewError> {
        self.collection.count().await
    }
}

impl<I: DeserializeOwned, W: View> CollectionView<W::Context, I, W> {
    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization. If the function returns false then
    /// the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CollectionView<_, u64, RegisterView<_, String>> =
    ///     CollectionView::load(context).await.unwrap();
    /// view.load_entry_mut(&23).await.unwrap();
    /// view.load_entry_mut(&24).await.unwrap();
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
                let index = BaseKey::deserialize_value(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CollectionView<_, u64, RegisterView<_, String>> =
    ///     CollectionView::load(context).await.unwrap();
    /// view.load_entry_mut(&23).await.unwrap();
    /// view.load_entry_mut(&28).await.unwrap();
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
                let index = BaseKey::deserialize_value(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }
}

impl<I, W: HashableView> HashableView for CollectionView<W::Context, I, W>
where
    I: Send + Sync + Serialize + DeserializeOwned,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.collection.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.collection.hash().await
    }
}

/// A map view that serializes the indices.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I, W: Allocative")]
pub struct CustomCollectionView<C, I, W> {
    collection: ByteCollectionView<C, W>,
    #[allocative(skip)]
    _phantom: PhantomData<I>,
}

impl<I: Send + Sync, W: View> View for CustomCollectionView<W::Context, I, W> {
    const NUM_INIT_KEYS: usize = ByteCollectionView::<W::Context, W>::NUM_INIT_KEYS;

    type Context = W::Context;

    fn context(&self) -> Self::Context {
        self.collection.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        ByteCollectionView::<_, W>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let collection = ByteCollectionView::post_load(context, values)?;
        Ok(CustomCollectionView {
            collection,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.collection.rollback()
    }

    async fn has_pending_changes(&self) -> bool {
        self.collection.has_pending_changes().await
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.collection.pre_save(batch)
    }

    fn post_save(&mut self) {
        self.collection.post_save()
    }

    fn clear(&mut self) {
        self.collection.clear()
    }
}

impl<I: Send + Sync, W: ClonableView> ClonableView for CustomCollectionView<W::Context, I, W> {
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(CustomCollectionView {
            collection: self.collection.clone_unchecked()?,
            _phantom: PhantomData,
        })
    }
}

impl<I: CustomSerialize, W: View> CustomCollectionView<W::Context, I, W> {
    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then a default entry is added to the collection. The resulting view
    /// can be modified.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     CustomCollectionView::load(context).await.unwrap();
    /// let subview = view.load_entry_mut(&23).await.unwrap();
    /// let value = subview.get();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn load_entry_mut<Q>(&mut self, index: &Q) -> Result<&mut W, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.load_entry_mut(&short_key).await
    }

    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then `None` is returned. The resulting view cannot be modified.
    /// May fail if one subview is already being visited.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     CustomCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).await.unwrap();
    /// }
    /// {
    ///     let subview = view.try_load_entry(&23).await.unwrap().unwrap();
    ///     let value = subview.get();
    ///     assert_eq!(*value, String::default());
    /// }
    /// assert!(view.try_load_entry(&24).await.unwrap().is_none());
    /// # })
    /// ```
    pub async fn try_load_entry<Q>(
        &self,
        index: &Q,
    ) -> Result<Option<ReadGuardedView<'_, W>>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.try_load_entry(&short_key).await
    }

    /// Load multiple entries for reading at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     CustomCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).await.unwrap();
    /// }
    /// let subviews = view.try_load_entries(&[23, 42]).await.unwrap();
    /// let value0 = subviews[0].as_ref().unwrap().get();
    /// assert_eq!(*value0, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries<'a, Q>(
        &self,
        indices: impl IntoIterator<Item = &'a Q>,
    ) -> Result<Vec<Option<ReadGuardedView<'_, W>>>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + 'a,
    {
        let short_keys = indices
            .into_iter()
            .map(|index| index.to_custom_bytes())
            .collect::<Result<_, _>>()?;
        self.collection.try_load_entries(short_keys).await
    }

    /// Loads multiple entries for reading at once with their keys.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     CustomCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).await.unwrap();
    /// }
    /// let indices = [23, 42];
    /// let subviews = view.try_load_entries_pairs(indices).await.unwrap();
    /// let value0 = subviews[0].1.as_ref().unwrap().get();
    /// assert_eq!(*value0, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries_pairs<Q>(
        &self,
        indices: impl IntoIterator<Item = Q>,
    ) -> Result<Vec<(Q, Option<ReadGuardedView<'_, W>>)>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + Clone,
    {
        let indices_vec: Vec<Q> = indices.into_iter().collect();
        let values = self.try_load_entries(indices_vec.iter()).await?;
        Ok(indices_vec.into_iter().zip(values).collect())
    }

    /// Load all entries for reading at once.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     CustomCollectionView::load(context).await.unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).await.unwrap();
    /// }
    /// let subviews = view.try_load_all_entries().await.unwrap();
    /// assert_eq!(subviews.len(), 1);
    /// # })
    /// ```
    pub async fn try_load_all_entries(&self) -> Result<Vec<(I, ReadGuardedView<'_, W>)>, ViewError>
    where
        I: CustomSerialize,
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

    /// Marks the entry so that it is removed in the next flush.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     CustomCollectionView::load(context).await.unwrap();
    /// let subview = view.load_entry_mut(&23).await.unwrap();
    /// let value = subview.get_mut();
    /// *value = String::from("Hello");
    /// view.reset_entry_to_default(&23).unwrap();
    /// let subview = view.load_entry_mut(&23).await.unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub fn reset_entry_to_default<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.reset_entry_to_default(&short_key)
    }

    /// Removes an entry from the `CollectionView`. If absent nothing happens.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     CustomCollectionView::load(context).await.unwrap();
    /// let subview = view.load_entry_mut(&23).await.unwrap();
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

    /// Gets the extra data.
    pub fn extra(&self) -> &<W::Context as Context>::Extra {
        self.collection.extra()
    }
}

impl<I: CustomSerialize + Send, W: View> CustomCollectionView<W::Context, I, W> {
    /// Returns the list of indices in the collection in the order determined by the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     CustomCollectionView::load(context).await.unwrap();
    /// view.load_entry_mut(&23).await.unwrap();
    /// view.load_entry_mut(&25).await.unwrap();
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = CustomCollectionView::<_, u128, RegisterView<_, String>>::load(context)
    ///     .await
    ///     .unwrap();
    /// view.load_entry_mut(&(23 as u128)).await.unwrap();
    /// view.load_entry_mut(&(25 as u128)).await.unwrap();
    /// assert_eq!(view.count().await.unwrap(), 2);
    /// # })
    /// ```
    pub async fn count(&self) -> Result<usize, ViewError> {
        self.collection.count().await
    }
}

impl<I: CustomSerialize, W: View> CustomCollectionView<W::Context, I, W> {
    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization. If the function f returns false,
    /// then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     CustomCollectionView::load(context).await.unwrap();
    /// view.load_entry_mut(&28).await.unwrap();
    /// view.load_entry_mut(&24).await.unwrap();
    /// view.load_entry_mut(&23).await.unwrap();
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

    /// Applies a function on each index. Indices are visited in an order
    /// determined by the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view: CustomCollectionView<_, u128, RegisterView<_, String>> =
    ///     CustomCollectionView::load(context).await.unwrap();
    /// view.load_entry_mut(&28).await.unwrap();
    /// view.load_entry_mut(&24).await.unwrap();
    /// view.load_entry_mut(&23).await.unwrap();
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

impl<I, W: HashableView> HashableView for CustomCollectionView<W::Context, I, W>
where
    Self: View,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.collection.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.collection.hash().await
    }
}

/// Type wrapping `ByteCollectionView` while memoizing the hash.
pub type HashedByteCollectionView<C, W> =
    WrappedHashableContainerView<C, ByteCollectionView<C, W>, HasherOutput>;

/// Wrapper around `ByteCollectionView` to compute hashes based on the history of changes.
pub type HistoricallyHashedByteCollectionView<C, W> =
    HistoricallyHashableView<C, ByteCollectionView<C, W>>;

/// Type wrapping `CollectionView` while memoizing the hash.
pub type HashedCollectionView<C, I, W> =
    WrappedHashableContainerView<C, CollectionView<C, I, W>, HasherOutput>;

/// Wrapper around `CollectionView` to compute hashes based on the history of changes.
pub type HistoricallyHashedCollectionView<C, I, W> =
    HistoricallyHashableView<C, CollectionView<C, I, W>>;

/// Type wrapping `CustomCollectionView` while memoizing the hash.
pub type HashedCustomCollectionView<C, I, W> =
    WrappedHashableContainerView<C, CustomCollectionView<C, I, W>, HasherOutput>;

/// Wrapper around `CustomCollectionView` to compute hashes based on the history of changes.
pub type HistoricallyHashedCustomCollectionView<C, I, W> =
    HistoricallyHashableView<C, CustomCollectionView<C, I, W>>;

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::{CollectionView, CustomCollectionView, ReadGuardedView};
    use crate::{
        graphql::{hash_name, mangle, missing_key_error, Entry, MapInput},
        views::View,
    };

    impl<T: async_graphql::OutputType> async_graphql::OutputType for ReadGuardedView<'_, T> {
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
        async_graphql::TypeName for CollectionView<C, K, V>
    {
        fn type_name() -> Cow<'static, str> {
            format!(
                "CollectionView_{}_{}_{:08x}",
                mangle(K::type_name()),
                mangle(V::type_name()),
                hash_name::<(K, V)>(),
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<K, V> CollectionView<V::Context, K, V>
    where
        K: async_graphql::InputType
            + async_graphql::OutputType
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::fmt::Debug,
        V: View + async_graphql::OutputType,
    {
        async fn keys(&self) -> Result<Vec<K>, async_graphql::Error> {
            Ok(self.indices().await?)
        }

        #[graphql(derived(name = "count"))]
        async fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count().await? as u32)
        }

        async fn entry(
            &self,
            key: K,
        ) -> Result<Entry<K, ReadGuardedView<'_, V>>, async_graphql::Error> {
            let value = self
                .try_load_entry(&key)
                .await?
                .ok_or_else(|| missing_key_error(&key))?;
            Ok(Entry { value, key })
        }

        async fn entries(
            &self,
            input: Option<MapInput<K>>,
        ) -> Result<Vec<Entry<K, ReadGuardedView<'_, V>>>, async_graphql::Error> {
            let keys = if let Some(keys) = input
                .and_then(|input| input.filters)
                .and_then(|filters| filters.keys)
            {
                keys
            } else {
                self.indices().await?
            };

            let values = self.try_load_entries(&keys).await?;
            Ok(values
                .into_iter()
                .zip(keys)
                .filter_map(|(value, key)| value.map(|value| Entry { value, key }))
                .collect())
        }
    }

    impl<C: Send + Sync, K: async_graphql::InputType, V: async_graphql::OutputType>
        async_graphql::TypeName for CustomCollectionView<C, K, V>
    {
        fn type_name() -> Cow<'static, str> {
            format!(
                "CustomCollectionView_{}_{}_{:08x}",
                mangle(K::type_name()),
                mangle(V::type_name()),
                hash_name::<(K, V)>(),
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<K, V> CustomCollectionView<V::Context, K, V>
    where
        K: async_graphql::InputType
            + async_graphql::OutputType
            + crate::common::CustomSerialize
            + std::fmt::Debug,
        V: View + async_graphql::OutputType,
    {
        async fn keys(&self) -> Result<Vec<K>, async_graphql::Error> {
            Ok(self.indices().await?)
        }

        #[graphql(derived(name = "count"))]
        async fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count().await? as u32)
        }

        async fn entry(
            &self,
            key: K,
        ) -> Result<Entry<K, ReadGuardedView<'_, V>>, async_graphql::Error> {
            let value = self
                .try_load_entry(&key)
                .await?
                .ok_or_else(|| missing_key_error(&key))?;
            Ok(Entry { value, key })
        }

        async fn entries(
            &self,
            input: Option<MapInput<K>>,
        ) -> Result<Vec<Entry<K, ReadGuardedView<'_, V>>>, async_graphql::Error> {
            let keys = if let Some(keys) = input
                .and_then(|input| input.filters)
                .and_then(|filters| filters.keys)
            {
                keys
            } else {
                self.indices().await?
            };

            let values = self.try_load_entries(&keys).await?;
            Ok(values
                .into_iter()
                .zip(keys)
                .filter_map(|(value, key)| value.map(|value| Entry { value, key }))
                .collect())
        }
    }
}
