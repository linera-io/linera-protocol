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
use std::sync::{RwLock, RwLockReadGuard};
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
        if let Ok(updates) = self.updates.try_read() {
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
        let updates = self.updates.read().expect("SyncCollectionView lock should not be poisoned");
        !updates.is_empty()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        let updates = self
            .updates
            .try_read()
            .map_err(|_| ViewError::TryLockError(vec![]))?;
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
        let updates = self.updates.read().expect("SyncCollectionView lock should not be poisoned");
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
                if !self.delete_storage_first
                    && self.context.store().contains_key(&key_index)?
                {
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
        let updates = self.updates.read().expect("SyncCollectionView lock should not be poisoned");

        for (position, short_key) in short_keys.into_iter().enumerate() {
            match updates.get(&short_key) {
                Some(update) => match update {
                    Update::Removed => {
                        results.push(None);
                    }
                    Update::Set(_) => {
                        let updates = self.updates.read().expect("SyncCollectionView lock should not be poisoned");
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
            .read_multi_values_bytes(&keys_to_load)
            ?;

        for (loaded_values, (position, context)) in values
            .chunks_exact_or_repeat(W::NUM_INIT_KEYS)
            .zip(entries_to_load)
        {
            let view = W::post_load(context, loaded_values)?;
            let updates = self.updates.read().expect("SyncCollectionView lock should not be poisoned");
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
    pub fn try_load_all_entries(
        &self,
    ) -> Result<Vec<(Vec<u8>, SyncReadGuardedView<'_, W>)>, ViewError> {
        let updates = self.updates.read().expect("SyncCollectionView lock should not be poisoned"); // Acquire the read lock to prevent writes.
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
                    let updates = self.updates.read().expect("SyncCollectionView lock should not be poisoned");
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
            .read_multi_values_bytes(&keys_to_load)
            ?;

        for (loaded_values, (position, context, short_key)) in values
            .chunks_exact_or_repeat(W::NUM_INIT_KEYS)
            .zip(keys_to_load_metadata)
        {
            let view = W::post_load(context, loaded_values)?;
            let updates = self.updates.read().expect("SyncCollectionView lock should not be poisoned");
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
        let updates = self.updates.read().expect("SyncCollectionView lock should not be poisoned");
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
        if self.delete_storage_first {
            // Optimization: No need to mark `short_key` for deletion as we are going to remove all the keys at once.
            self.updates.get_mut().remove(&short_key);
        } else {
            self.updates.get_mut().insert(short_key, Update::Removed);
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
    /// 
    /// .unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn for_each_key_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let updates = self.updates.read().expect("SyncCollectionView lock should not be poisoned");
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
    /// 
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
        })
        ?;
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
        })
        ?;
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
