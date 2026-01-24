// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Borrow,
    collections::{btree_map, BTreeMap},
    marker::PhantomData,
    mem,
    ops::Deref,
};

use allocative::{Allocative, Key, Visitor};
use parking_lot::{RwLock, RwLockReadGuard};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::{CustomSerialize, SliceExt as _, Update},
    context::{BaseKey, SyncContext},
    store::ReadableSyncKeyValueStore as _,
    sync_view::{SyncView, MIN_VIEW_TAG},
    ViewError,
};

/// A view that supports accessing a collection of views of the same kind, indexed by a
/// `Vec<u8>`, one subview at a time.
#[derive(Debug)]
pub struct SyncByteCollectionView<C, W> {
    /// The view context.
    context: C,
    /// Whether to clear storage before applying updates.
    delete_storage_first: bool,
    /// Entries that may have staged changes.
    updates: RwLock<BTreeMap<Vec<u8>, Update<W>>>,
}

impl<C, W: Allocative> Allocative for SyncByteCollectionView<C, W> {
    fn visit<'a, 'b: 'a>(&self, visitor: &'a mut Visitor<'b>) {
        let name = Key::new("SyncByteCollectionView");
        let size = mem::size_of::<Self>();
        let mut visitor = visitor.enter(name, size);
        if let Some(updates) = self.updates.try_read() {
            updates.deref().visit(&mut visitor);
        }
        visitor.exit();
    }
}

/// A read-only accessor for a particular subview in a [`SyncCollectionView`].
pub enum SyncReadGuardedView<'a, W> {
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

impl<W> std::ops::Deref for SyncReadGuardedView<'_, W> {
    type Target = W;

    fn deref(&self) -> &W {
        match self {
            SyncReadGuardedView::Loaded { updates, short_key } => {
                let Update::Set(view) = updates.get(short_key).unwrap() else {
                    unreachable!();
                };
                view
            }
            SyncReadGuardedView::NotLoaded { _updates, view } => view,
        }
    }
}

/// We need to find new base keys in order to implement `SyncCollectionView`.
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

impl<W: SyncView> SyncView for SyncByteCollectionView<W::Context, W> {
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

    fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        let updates = self.updates.read();
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
        let updates = self.updates.get_mut();
        for (_, update) in updates.iter_mut() {
            if let Update::Set(view) = update {
                view.post_save();
            }
        }
        self.delete_storage_first = false;
        updates.clear();
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.updates.get_mut().clear();
    }
}

impl<W: SyncView> SyncByteCollectionView<W::Context, W> {
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
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncByteCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncByteCollectionView<_, SyncRegisterView<_, String>> =
    ///     SyncByteCollectionView::load(context).unwrap();
    /// let subview = view.load_entry_mut(&[0, 1]).unwrap();
    /// let value = subview.get();
    /// assert_eq!(*value, String::default());
    /// ```
    pub fn load_entry_mut(&mut self, short_key: &[u8]) -> Result<&mut W, ViewError> {
        let updates = self.updates.get_mut();
        match updates.entry(short_key.to_vec()) {
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
                    W::load(context)?
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
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncByteCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncByteCollectionView<_, SyncRegisterView<_, String>> =
    ///     SyncByteCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&[0, 1]).unwrap();
    /// }
    /// {
    ///     let subview = view.try_load_entry(&[0, 1]).unwrap().unwrap();
    ///     let value = subview.get();
    ///     assert_eq!(*value, String::default());
    /// }
    /// assert!(view.try_load_entry(&[0, 2]).unwrap().is_none());
    /// ```
    pub fn try_load_entry(
        &self,
        short_key: &[u8],
    ) -> Result<Option<SyncReadGuardedView<'_, W>>, ViewError> {
        let updates = self.updates.read();
        match updates.get(short_key) {
            Some(update) => match update {
                Update::Removed => Ok(None),
                Update::Set(_) => Ok(Some(SyncReadGuardedView::Loaded {
                    updates,
                    short_key: short_key.to_vec(),
                })),
            },
            None => {
                let key_index = self
                    .context
                    .base_key()
                    .base_tag_index(KeyTag::Index as u8, short_key);
                if !self.delete_storage_first && self.context.store().contains_key(&key_index)? {
                    let key = self
                        .context
                        .base_key()
                        .base_tag_index(KeyTag::Subview as u8, short_key);
                    let context = self.context.clone_with_base_key(key);
                    let view = W::load(context)?;
                    Ok(Some(SyncReadGuardedView::NotLoaded {
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
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncByteCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncByteCollectionView<_, SyncRegisterView<_, String>> =
    ///     SyncByteCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&[0, 1]).unwrap();
    /// }
    /// let short_keys = vec![vec![0, 1], vec![2, 3]];
    /// let subviews = view.try_load_entries(short_keys).unwrap();
    /// let value0 = subviews[0].as_ref().unwrap().get();
    /// assert_eq!(*value0, String::default());
    /// ```
    pub fn try_load_entries(
        &self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<SyncReadGuardedView<'_, W>>>, ViewError> {
        let mut results = Vec::with_capacity(short_keys.len());
        let mut keys_to_check = Vec::new();
        let mut keys_to_check_metadata = Vec::new();
        let updates = self.updates.read();

        for (position, short_key) in short_keys.into_iter().enumerate() {
            match updates.get(&short_key) {
                Some(update) => match update {
                    Update::Removed => {
                        results.push(None);
                    }
                    Update::Set(_) => {
                        let updates = self.updates.read();
                        results.push(Some(SyncReadGuardedView::Loaded {
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

        let found_keys = self.context.store().contains_keys(&keys_to_check)?;
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
            .read_multi_values_bytes(&keys_to_load)?;

        for (loaded_values, (position, context)) in values
            .chunks_exact_or_repeat(W::NUM_INIT_KEYS)
            .zip(entries_to_load)
        {
            let view = W::post_load(context, loaded_values)?;
            let updates = self.updates.read();
            results[position] = Some(SyncReadGuardedView::NotLoaded {
                _updates: updates,
                view,
            });
        }

        Ok(results)
    }

    /// Loads multiple entries for reading at once with their keys.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncByteCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncByteCollectionView<_, SyncRegisterView<_, String>> =
    ///     SyncByteCollectionView::load(context).unwrap();
    /// {
    ///     let subview = view.load_entry_mut(&vec![0, 1]).unwrap();
    ///     subview.set("Bonjour".into());
    /// }
    /// let short_keys = vec![vec![0, 1], vec![0, 2]];
    /// let pairs = view.try_load_entries_pairs(short_keys).unwrap();
    /// assert_eq!(pairs[0].0, vec![0, 1]);
    /// assert_eq!(pairs[1].0, vec![0, 2]);
    /// let value0 = pairs[0].1.as_ref().unwrap().get();
    /// assert_eq!(*value0, "Bonjour".to_string());
    /// assert!(pairs[1].1.is_none());
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn try_load_entries_pairs(
        &self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Option<SyncReadGuardedView<'_, W>>)>, ViewError> {
        let values = self.try_load_entries(short_keys.clone())?;
        Ok(short_keys.into_iter().zip(values).collect())
    }

    /// Load all entries for reading at once.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncByteCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncByteCollectionView<_, SyncRegisterView<_, String>> =
    ///     SyncByteCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&[0, 1]).unwrap();
    /// }
    /// let subviews = view.try_load_all_entries().unwrap();
    /// assert_eq!(subviews.len(), 1);
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn try_load_all_entries(
        &self,
    ) -> Result<Vec<(Vec<u8>, SyncReadGuardedView<'_, W>)>, ViewError> {
        let updates = self.updates.read(); // Acquire the read lock to prevent writes.
        let short_keys = self.keys()?;
        let mut results = Vec::with_capacity(short_keys.len());

        let mut keys_to_load = Vec::new();
        let mut keys_to_load_metadata = Vec::new();
        for (position, short_key) in short_keys.iter().enumerate() {
            match updates.get(short_key) {
                Some(update) => {
                    let Update::Set(_) = update else {
                        unreachable!();
                    };
                    let updates = self.updates.read();
                    let view = SyncReadGuardedView::Loaded {
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
            .read_multi_values_bytes(&keys_to_load)?;

        for (loaded_values, (position, context, short_key)) in values
            .chunks_exact_or_repeat(W::NUM_INIT_KEYS)
            .zip(keys_to_load_metadata)
        {
            let view = W::post_load(context, loaded_values)?;
            let updates = self.updates.read();
            let guarded_view = SyncReadGuardedView::NotLoaded {
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
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncByteCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncByteCollectionView<_, SyncRegisterView<_, String>> =
    ///     SyncByteCollectionView::load(context).unwrap();
    /// let subview = view.load_entry_mut(&[0, 1]).unwrap();
    /// let value = subview.get_mut();
    /// *value = String::from("Hello");
    /// view.reset_entry_to_default(&[0, 1]).unwrap();
    /// let subview = view.load_entry_mut(&[0, 1]).unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
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
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncByteCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncByteCollectionView<_, SyncRegisterView<_, String>> =
    ///     SyncByteCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&[0, 1]).unwrap();
    /// }
    /// assert!(view.contains_key(&[0, 1]).unwrap());
    /// assert!(!view.contains_key(&[0, 2]).unwrap());
    /// ```
    pub fn contains_key(&self, short_key: &[u8]) -> Result<bool, ViewError> {
        let updates = self.updates.read();
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
                !self.delete_storage_first && self.context.store().contains_key(&key_index)?
            }
        })
    }

    /// Marks the entry as removed. If absent then nothing is done.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncByteCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncByteCollectionView<_, SyncRegisterView<_, String>> =
    ///     SyncByteCollectionView::load(context).unwrap();
    /// let subview = view.load_entry_mut(&[0, 1]).unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// view.remove_entry(vec![0, 1]);
    /// let keys = view.keys().unwrap();
    /// assert_eq!(keys.len(), 0);
    /// ```
    pub fn remove_entry(&mut self, short_key: Vec<u8>) {
        let updates = self.updates.get_mut();
        if self.delete_storage_first {
            // Optimization: No need to mark `short_key` for deletion as we are going to remove all the keys at once.
            updates.remove(&short_key);
        } else {
            updates.insert(short_key, Update::Removed);
        }
    }

    /// Gets the extra data.
    pub fn extra(&self) -> &<W::Context as SyncContext>::Extra {
        self.context.extra()
    }
}

impl<W: SyncView> SyncByteCollectionView<W::Context, W> {
    /// Applies a function f on each index (aka key). Keys are visited in the
    /// lexicographic order. If the function returns false, then the loop
    /// ends prematurely.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncByteCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncByteCollectionView<_, SyncRegisterView<_, String>> =
    ///     SyncByteCollectionView::load(context).unwrap();
    /// view.load_entry_mut(&[0, 1]).unwrap();
    /// view.load_entry_mut(&[0, 2]).unwrap();
    /// let mut count = 0;
    /// view.for_each_key_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 1)
    /// })
    /// .unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn for_each_key_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let updates = self.updates.read();
        let mut updates = updates.iter();
        let mut update = updates.next();
        if !self.delete_storage_first {
            let base = self.get_index_key(&[]);
            for index in self.context.store().find_keys_by_prefix(&base)? {
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
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncByteCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncByteCollectionView<_, SyncRegisterView<_, String>> =
    ///     SyncByteCollectionView::load(context).unwrap();
    /// view.load_entry_mut(&[0, 1]).unwrap();
    /// view.load_entry_mut(&[0, 2]).unwrap();
    /// let mut count = 0;
    /// view.for_each_key(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// ```
    pub fn for_each_key<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_key_while(|key| {
            f(key)?;
            Ok(true)
        })
    }

    /// Returns the list of keys in the collection. The order is lexicographic.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncByteCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncByteCollectionView<_, SyncRegisterView<_, String>> =
    ///     SyncByteCollectionView::load(context).unwrap();
    /// view.load_entry_mut(&[0, 1]).unwrap();
    /// view.load_entry_mut(&[0, 2]).unwrap();
    /// let keys = view.keys().unwrap();
    /// assert_eq!(keys, vec![vec![0, 1], vec![0, 2]]);
    /// ```
    pub fn keys(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut keys = Vec::new();
        self.for_each_key(|key| {
            keys.push(key.to_vec());
            Ok(())
        })?;
        Ok(keys)
    }

    /// Returns the number of entries in the collection.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncByteCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncByteCollectionView<_, SyncRegisterView<_, String>> =
    ///     SyncByteCollectionView::load(context).unwrap();
    /// view.load_entry_mut(&[0, 1]).unwrap();
    /// view.load_entry_mut(&[0, 2]).unwrap();
    /// assert_eq!(view.count().unwrap(), 2);
    /// ```
    pub fn count(&self) -> Result<usize, ViewError> {
        let mut count = 0;
        self.for_each_key(|_key| {
            count += 1;
            Ok(())
        })?;
        Ok(count)
    }
}

/// A view that supports accessing a collection of views of the same kind, indexed by a
/// key, one subview at a time.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I, W: Allocative")]
pub struct SyncCollectionView<C, I, W> {
    collection: SyncByteCollectionView<C, W>,
    #[allocative(skip)]
    _phantom: PhantomData<I>,
}

impl<W: SyncView, I> SyncView for SyncCollectionView<W::Context, I, W>
where
    I: Send + Sync + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = SyncByteCollectionView::<W::Context, W>::NUM_INIT_KEYS;

    type Context = W::Context;

    fn context(&self) -> Self::Context {
        self.collection.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        SyncByteCollectionView::<W::Context, W>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let collection = SyncByteCollectionView::post_load(context, values)?;
        Ok(SyncCollectionView {
            collection,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.collection.rollback()
    }

    fn has_pending_changes(&self) -> bool {
        self.collection.has_pending_changes()
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

impl<I: Serialize, W: SyncView> SyncCollectionView<W::Context, I, W> {
    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then a default entry is added to the collection. The resulting view
    /// can be modified.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCollectionView<_, u64, SyncRegisterView<_, String>> =
    ///     SyncCollectionView::load(context).unwrap();
    /// let subview = view.load_entry_mut(&23).unwrap();
    /// let value = subview.get();
    /// assert_eq!(*value, String::default());
    /// ```
    pub fn load_entry_mut<Q>(&mut self, index: &Q) -> Result<&mut W, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.collection.load_entry_mut(&short_key)
    }

    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then `None` is returned. The resulting view cannot be modified.
    /// May fail if one subview is already being visited.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCollectionView<_, u64, SyncRegisterView<_, String>> =
    ///     SyncCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).unwrap();
    /// }
    /// {
    ///     let subview = view.try_load_entry(&23).unwrap().unwrap();
    ///     let value = subview.get();
    ///     assert_eq!(*value, String::default());
    /// }
    /// assert!(view.try_load_entry(&24).unwrap().is_none());
    /// ```
    pub fn try_load_entry<Q>(
        &self,
        index: &Q,
    ) -> Result<Option<SyncReadGuardedView<'_, W>>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.collection.try_load_entry(&short_key)
    }

    /// Load multiple entries for reading at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCollectionView<_, u64, SyncRegisterView<_, String>> =
    ///     SyncCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).unwrap();
    /// }
    /// let indices = vec![23, 24];
    /// let subviews = view.try_load_entries(&indices).unwrap();
    /// let value0 = subviews[0].as_ref().unwrap().get();
    /// assert_eq!(*value0, String::default());
    /// ```
    pub fn try_load_entries<'a, Q>(
        &self,
        indices: impl IntoIterator<Item = &'a Q>,
    ) -> Result<Vec<Option<SyncReadGuardedView<'_, W>>>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + 'a,
    {
        let short_keys = indices
            .into_iter()
            .map(|index| BaseKey::derive_short_key(index))
            .collect::<Result<_, _>>()?;
        self.collection.try_load_entries(short_keys)
    }

    /// Loads multiple entries for reading at once with their keys.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCollectionView<_, u64, SyncRegisterView<_, String>> =
    ///     SyncCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).unwrap();
    /// }
    /// let indices = [23, 24];
    /// let subviews = view.try_load_entries_pairs(indices).unwrap();
    /// let value0 = subviews[0].1.as_ref().unwrap().get();
    /// assert_eq!(*value0, String::default());
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn try_load_entries_pairs<Q>(
        &self,
        indices: impl IntoIterator<Item = Q>,
    ) -> Result<Vec<(Q, Option<SyncReadGuardedView<'_, W>>)>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + Clone,
    {
        let indices_vec: Vec<Q> = indices.into_iter().collect();
        let values = self.try_load_entries(indices_vec.iter())?;
        Ok(indices_vec.into_iter().zip(values).collect())
    }

    /// Resets an entry to the default value.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCollectionView<_, u64, SyncRegisterView<_, String>> =
    ///     SyncCollectionView::load(context).unwrap();
    /// let subview = view.load_entry_mut(&23).unwrap();
    /// let value = subview.get_mut();
    /// *value = String::from("Hello");
    /// view.reset_entry_to_default(&23).unwrap();
    /// let subview = view.load_entry_mut(&23).unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// ```
    pub fn reset_entry_to_default<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.collection.reset_entry_to_default(&short_key)
    }

    /// Removes an entry from the `SyncCollectionView`. If absent nothing happens.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCollectionView<_, u64, SyncRegisterView<_, String>> =
    ///     SyncCollectionView::load(context).unwrap();
    /// let subview = view.load_entry_mut(&23).unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// view.remove_entry(&23).unwrap();
    /// let keys = view.indices().unwrap();
    /// assert_eq!(keys.len(), 0);
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

    /// Tests if the collection contains a specified key and returns a boolean.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCollectionView<_, u64, SyncRegisterView<_, String>> =
    ///     SyncCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).unwrap();
    /// }
    /// assert!(view.contains_key(&23).unwrap());
    /// assert!(!view.contains_key(&24).unwrap());
    /// ```
    pub fn contains_key<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = BaseKey::derive_short_key(index)?;
        self.collection.contains_key(&short_key)
    }

    /// Gets the extra data.
    pub fn extra(&self) -> &<W::Context as SyncContext>::Extra {
        self.collection.extra()
    }
}

impl<I, W: SyncView> SyncCollectionView<W::Context, I, W>
where
    I: Sync + Send + Serialize + DeserializeOwned,
{
    /// Load all entries for reading at once.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCollectionView<_, u64, SyncRegisterView<_, String>> =
    ///     SyncCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).unwrap();
    /// }
    /// let subviews = view.try_load_all_entries().unwrap();
    /// assert_eq!(subviews.len(), 1);
    /// ```
    pub fn try_load_all_entries(&self) -> Result<Vec<(I, SyncReadGuardedView<'_, W>)>, ViewError> {
        let results = self.collection.try_load_all_entries()?;
        results
            .into_iter()
            .map(|(short_key, view)| {
                let index = BaseKey::deserialize_value(&short_key)?;
                Ok((index, view))
            })
            .collect()
    }

    /// Returns the list of indices in the collection in the order determined by
    /// the serialization.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCollectionView<_, u64, SyncRegisterView<_, String>> =
    ///     SyncCollectionView::load(context).unwrap();
    /// view.load_entry_mut(&23).unwrap();
    /// view.load_entry_mut(&25).unwrap();
    /// let indices = view.indices().unwrap();
    /// assert_eq!(indices.len(), 2);
    /// ```
    pub fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index);
            Ok(())
        })?;
        Ok(indices)
    }

    /// Returns the number of entries in the collection.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCollectionView<_, u64, SyncRegisterView<_, String>> =
    ///     SyncCollectionView::load(context).unwrap();
    /// view.load_entry_mut(&23).unwrap();
    /// view.load_entry_mut(&25).unwrap();
    /// assert_eq!(view.count().unwrap(), 2);
    /// ```
    pub fn count(&self) -> Result<usize, ViewError> {
        self.collection.count()
    }
}

impl<I: DeserializeOwned, W: SyncView> SyncCollectionView<W::Context, I, W> {
    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization. If the function returns false then
    /// the loop ends prematurely.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCollectionView<_, u64, SyncRegisterView<_, String>> =
    ///     SyncCollectionView::load(context).unwrap();
    /// view.load_entry_mut(&23).unwrap();
    /// view.load_entry_mut(&24).unwrap();
    /// let mut count = 0;
    /// view.for_each_index_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 1)
    /// })
    /// .unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.collection.for_each_key_while(|key| {
            let index = BaseKey::deserialize_value(key)?;
            f(index)
        })?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCollectionView<_, u64, SyncRegisterView<_, String>> =
    ///     SyncCollectionView::load(context).unwrap();
    /// view.load_entry_mut(&23).unwrap();
    /// view.load_entry_mut(&28).unwrap();
    /// let mut count = 0;
    /// view.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// ```
    pub fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.collection.for_each_key(|key| {
            let index = BaseKey::deserialize_value(key)?;
            f(index)
        })?;
        Ok(())
    }
}

/// A view that supports accessing a collection of views of the same kind, indexed by a
/// key that uses custom serialization, one subview at a time.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, I, W: Allocative")]
pub struct SyncCustomCollectionView<C, I, W> {
    collection: SyncByteCollectionView<C, W>,
    #[allocative(skip)]
    _phantom: PhantomData<I>,
}

impl<I: Send + Sync, W: SyncView> SyncView for SyncCustomCollectionView<W::Context, I, W> {
    const NUM_INIT_KEYS: usize = SyncByteCollectionView::<W::Context, W>::NUM_INIT_KEYS;

    type Context = W::Context;

    fn context(&self) -> Self::Context {
        self.collection.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        SyncByteCollectionView::<_, W>::pre_load(context)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let collection = SyncByteCollectionView::post_load(context, values)?;
        Ok(SyncCustomCollectionView {
            collection,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.collection.rollback()
    }

    fn has_pending_changes(&self) -> bool {
        self.collection.has_pending_changes()
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

impl<I: CustomSerialize, W: SyncView> SyncCustomCollectionView<W::Context, I, W> {
    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then a default entry is added to the collection. The resulting view
    /// can be modified.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCustomCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCustomCollectionView<_, u128, SyncRegisterView<_, String>> =
    ///     SyncCustomCollectionView::load(context).unwrap();
    /// let subview = view.load_entry_mut(&23).unwrap();
    /// let value = subview.get();
    /// assert_eq!(*value, String::default());
    /// ```
    pub fn load_entry_mut<Q>(&mut self, index: &Q) -> Result<&mut W, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.load_entry_mut(&short_key)
    }

    /// Loads a subview for the data at the given index in the collection. If an entry
    /// is absent then `None` is returned. The resulting view cannot be modified.
    /// May fail if one subview is already being visited.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCustomCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCustomCollectionView<_, u128, SyncRegisterView<_, String>> =
    ///     SyncCustomCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).unwrap();
    /// }
    /// {
    ///     let subview = view.try_load_entry(&23).unwrap().unwrap();
    ///     let value = subview.get();
    ///     assert_eq!(*value, String::default());
    /// }
    /// assert!(view.try_load_entry(&24).unwrap().is_none());
    /// ```
    pub fn try_load_entry<Q>(
        &self,
        index: &Q,
    ) -> Result<Option<SyncReadGuardedView<'_, W>>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.try_load_entry(&short_key)
    }

    /// Load multiple entries for reading at once.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCustomCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCustomCollectionView<_, u128, SyncRegisterView<_, String>> =
    ///     SyncCustomCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).unwrap();
    /// }
    /// let subviews = view.try_load_entries(&[23, 42]).unwrap();
    /// let value0 = subviews[0].as_ref().unwrap().get();
    /// assert_eq!(*value0, String::default());
    /// ```
    pub fn try_load_entries<'a, Q>(
        &self,
        indices: impl IntoIterator<Item = &'a Q>,
    ) -> Result<Vec<Option<SyncReadGuardedView<'_, W>>>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + 'a,
    {
        let short_keys = indices
            .into_iter()
            .map(|index| index.to_custom_bytes())
            .collect::<Result<_, _>>()?;
        self.collection.try_load_entries(short_keys)
    }

    /// Loads multiple entries for reading at once with their keys.
    /// The entries in indices have to be all distinct.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCustomCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCustomCollectionView<_, u128, SyncRegisterView<_, String>> =
    ///     SyncCustomCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).unwrap();
    /// }
    /// let indices = [23, 42];
    /// let subviews = view.try_load_entries_pairs(indices).unwrap();
    /// let value0 = subviews[0].1.as_ref().unwrap().get();
    /// assert_eq!(*value0, String::default());
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn try_load_entries_pairs<Q>(
        &self,
        indices: impl IntoIterator<Item = Q>,
    ) -> Result<Vec<(Q, Option<SyncReadGuardedView<'_, W>>)>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + Clone,
    {
        let indices_vec: Vec<Q> = indices.into_iter().collect();
        let values = self.try_load_entries(indices_vec.iter())?;
        Ok(indices_vec.into_iter().zip(values).collect())
    }

    /// Marks the entry so that it is removed in the next flush.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCustomCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCustomCollectionView<_, u128, SyncRegisterView<_, String>> =
    ///     SyncCustomCollectionView::load(context).unwrap();
    /// let subview = view.load_entry_mut(&23).unwrap();
    /// let value = subview.get_mut();
    /// *value = String::from("Hello");
    /// view.reset_entry_to_default(&23).unwrap();
    /// let subview = view.load_entry_mut(&23).unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// ```
    pub fn reset_entry_to_default<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.reset_entry_to_default(&short_key)
    }

    /// Removes an entry from the `SyncCustomCollectionView`. If absent nothing happens.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCustomCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCustomCollectionView<_, u128, SyncRegisterView<_, String>> =
    ///     SyncCustomCollectionView::load(context).unwrap();
    /// let subview = view.load_entry_mut(&23).unwrap();
    /// let value = subview.get_mut();
    /// assert_eq!(*value, String::default());
    /// view.remove_entry(&23).unwrap();
    /// let keys = view.indices().unwrap();
    /// assert_eq!(keys.len(), 0);
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

    /// Tests if the collection contains a specified key and returns a boolean.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCustomCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCustomCollectionView<_, u128, SyncRegisterView<_, String>> =
    ///     SyncCustomCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).unwrap();
    /// }
    /// assert!(view.contains_key(&23).unwrap());
    /// assert!(!view.contains_key(&24).unwrap());
    /// ```
    pub fn contains_key<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.contains_key(&short_key)
    }

    /// Gets the extra data.
    pub fn extra(&self) -> &<W::Context as SyncContext>::Extra {
        self.collection.extra()
    }
}

impl<I: CustomSerialize + Send, W: SyncView> SyncCustomCollectionView<W::Context, I, W> {
    /// Load all entries for reading at once.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCustomCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCustomCollectionView<_, u128, SyncRegisterView<_, String>> =
    ///     SyncCustomCollectionView::load(context).unwrap();
    /// {
    ///     let _subview = view.load_entry_mut(&23).unwrap();
    /// }
    /// let subviews = view.try_load_all_entries().unwrap();
    /// assert_eq!(subviews.len(), 1);
    /// ```
    pub fn try_load_all_entries(&self) -> Result<Vec<(I, SyncReadGuardedView<'_, W>)>, ViewError> {
        let results = self.collection.try_load_all_entries()?;
        results
            .into_iter()
            .map(|(short_key, view)| {
                let index = I::from_custom_bytes(&short_key)?;
                Ok((index, view))
            })
            .collect()
    }

    /// Returns the list of indices in the collection in the order determined by the custom serialization.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCustomCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCustomCollectionView<_, u128, SyncRegisterView<_, String>> =
    ///     SyncCustomCollectionView::load(context).unwrap();
    /// view.load_entry_mut(&23).unwrap();
    /// view.load_entry_mut(&25).unwrap();
    /// let indices = view.indices().unwrap();
    /// assert_eq!(indices, vec![23, 25]);
    /// ```
    pub fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index);
            Ok(())
        })?;
        Ok(indices)
    }

    /// Returns the number of entries in the collection.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCustomCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view =
    ///     SyncCustomCollectionView::<_, u128, SyncRegisterView<_, String>>::load(context).unwrap();
    /// view.load_entry_mut(&(23 as u128)).unwrap();
    /// view.load_entry_mut(&(25 as u128)).unwrap();
    /// assert_eq!(view.count().unwrap(), 2);
    /// ```
    pub fn count(&self) -> Result<usize, ViewError> {
        self.collection.count()
    }
}

impl<I: CustomSerialize, W: SyncView> SyncCustomCollectionView<W::Context, I, W> {
    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization. If the function f returns false,
    /// then the loop ends prematurely.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCustomCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCustomCollectionView<_, u128, SyncRegisterView<_, String>> =
    ///     SyncCustomCollectionView::load(context).unwrap();
    /// view.load_entry_mut(&28).unwrap();
    /// view.load_entry_mut(&24).unwrap();
    /// view.load_entry_mut(&23).unwrap();
    /// let mut part_indices = Vec::new();
    /// view.for_each_index_while(|index| {
    ///     part_indices.push(index);
    ///     Ok(part_indices.len() < 2)
    /// })
    /// .unwrap();
    /// assert_eq!(part_indices, vec![23, 24]);
    /// ```
    pub fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.collection.for_each_key_while(|key| {
            let index = I::from_custom_bytes(key)?;
            f(index)
        })?;
        Ok(())
    }

    /// Applies a function on each index. Indices are visited in an order
    /// determined by the custom serialization.
    /// ```rust
    /// # use linera_views::context::SyncMemoryContext;
    /// # use linera_views::sync_view::collection_view::SyncCustomCollectionView;
    /// # use linera_views::sync_view::register_view::SyncRegisterView;
    /// # use linera_views::sync_view::SyncView;
    /// # let context = SyncMemoryContext::new_for_testing(());
    /// let mut view: SyncCustomCollectionView<_, u128, SyncRegisterView<_, String>> =
    ///     SyncCustomCollectionView::load(context).unwrap();
    /// view.load_entry_mut(&28).unwrap();
    /// view.load_entry_mut(&24).unwrap();
    /// view.load_entry_mut(&23).unwrap();
    /// let mut indices = Vec::new();
    /// view.for_each_index(|index| {
    ///     indices.push(index);
    ///     Ok(())
    /// })
    /// .unwrap();
    /// assert_eq!(indices, vec![23, 24, 28]);
    /// ```
    pub fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.collection.for_each_key(|key| {
            let index = I::from_custom_bytes(key)?;
            f(index)
        })?;
        Ok(())
    }
}

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::{SyncCollectionView, SyncCustomCollectionView, SyncReadGuardedView};
    use crate::{
        graphql::{hash_name, mangle, missing_key_error, Entry, MapInput},
        sync_view::SyncView,
    };

    impl<T: async_graphql::OutputType> async_graphql::OutputType for SyncReadGuardedView<'_, T> {
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
        async_graphql::TypeName for SyncCollectionView<C, K, V>
    {
        fn type_name() -> Cow<'static, str> {
            format!(
                "SyncCollectionView_{}_{}_{:08x}",
                mangle(K::type_name()),
                mangle(V::type_name()),
                hash_name::<(K, V)>(),
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<K, V> SyncCollectionView<V::Context, K, V>
    where
        K: async_graphql::InputType
            + async_graphql::OutputType
            + Send
            + Sync
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::fmt::Debug,
        V: SyncView + async_graphql::OutputType + Send + Sync,
        V::Context: Send + Sync,
    {
        async fn keys(&self) -> Result<Vec<K>, async_graphql::Error> {
            Ok(self.indices()?)
        }

        #[graphql(derived(name = "count"))]
        async fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count()? as u32)
        }

        async fn entry(
            &self,
            key: K,
        ) -> Result<Entry<K, SyncReadGuardedView<'_, V>>, async_graphql::Error> {
            let value = self
                .try_load_entry(&key)?
                .ok_or_else(|| missing_key_error(&key))?;
            Ok(Entry { value, key })
        }

        async fn entries(
            &self,
            input: Option<MapInput<K>>,
        ) -> Result<Vec<Entry<K, SyncReadGuardedView<'_, V>>>, async_graphql::Error> {
            let keys = if let Some(keys) = input
                .and_then(|input| input.filters)
                .and_then(|filters| filters.keys)
            {
                keys
            } else {
                self.indices()?
            };

            let values = self.try_load_entries(&keys)?;
            Ok(values
                .into_iter()
                .zip(keys)
                .filter_map(|(value, key)| value.map(|value| Entry { value, key }))
                .collect())
        }
    }

    impl<C: Send + Sync, K: async_graphql::InputType, V: async_graphql::OutputType>
        async_graphql::TypeName for SyncCustomCollectionView<C, K, V>
    {
        fn type_name() -> Cow<'static, str> {
            format!(
                "SyncCustomCollectionView_{}_{}_{:08x}",
                mangle(K::type_name()),
                mangle(V::type_name()),
                hash_name::<(K, V)>(),
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<K, V> SyncCustomCollectionView<V::Context, K, V>
    where
        K: async_graphql::InputType
            + async_graphql::OutputType
            + Send
            + Sync
            + crate::common::CustomSerialize
            + std::fmt::Debug,
        V: SyncView + async_graphql::OutputType + Send + Sync,
        V::Context: Send + Sync,
    {
        async fn keys(&self) -> Result<Vec<K>, async_graphql::Error> {
            Ok(self.indices()?)
        }

        #[graphql(derived(name = "count"))]
        async fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count()? as u32)
        }

        async fn entry(
            &self,
            key: K,
        ) -> Result<Entry<K, SyncReadGuardedView<'_, V>>, async_graphql::Error> {
            let value = self
                .try_load_entry(&key)?
                .ok_or_else(|| missing_key_error(&key))?;
            Ok(Entry { value, key })
        }

        async fn entries(
            &self,
            input: Option<MapInput<K>>,
        ) -> Result<Vec<Entry<K, SyncReadGuardedView<'_, V>>>, async_graphql::Error> {
            let keys = if let Some(keys) = input
                .and_then(|input| input.filters)
                .and_then(|filters| filters.keys)
            {
                keys
            } else {
                self.indices()?
            };

            let values = self.try_load_entries(&keys)?;
            Ok(values
                .into_iter()
                .zip(keys)
                .filter_map(|(value, key)| value.map(|value| Entry { value, key }))
                .collect())
        }
    }
}
