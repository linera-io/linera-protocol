// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::{Context, CustomSerialize, HasherOutput, KeyIterable, Update, MIN_VIEW_TAG},
    views::{HashableView, Hasher, View, ViewError},
};
use async_lock::{Mutex, RwLock, RwLockReadGuardArc, RwLockWriteGuardArc};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    borrow::Borrow,
    collections::{btree_map, BTreeMap},
    fmt::Debug,
    io::Write,
    marker::PhantomData,
    mem,
    sync::Arc,
};

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
    needs_clear: bool,
    updates: Mutex<BTreeMap<Vec<u8>, Update<Arc<RwLock<W>>>>>,
    stored_hash: Option<HasherOutput>,
    hash: Mutex<Option<HasherOutput>>,
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
    /// Prefix for the hash value.
    Hash,
}

#[async_trait]
impl<C, W> View<C> for ReentrantByteCollectionView<C, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    W: View<C> + Send + Sync,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let key = context.base_tag(KeyTag::Hash as u8);
        let hash = context.read_value(&key).await?;
        Ok(Self {
            context,
            needs_clear: false,
            updates: Mutex::new(BTreeMap::new()),
            stored_hash: hash,
            hash: Mutex::new(hash),
        })
    }

    fn rollback(&mut self) {
        self.needs_clear = false;
        self.updates.get_mut().clear();
        *self.hash.get_mut() = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.needs_clear {
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(self.updates.get_mut()) {
                if let Update::Set(view) = update {
                    let mut view = Arc::try_unwrap(view)
                        .map_err(|_| ViewError::CannotAcquireCollectionEntry)?
                        .into_inner();
                    view.flush(batch)?;
                    self.add_index(batch, &index);
                }
            }
            self.stored_hash = None;
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
        let hash = *self.hash.get_mut();
        if self.stored_hash != hash {
            let key = self.context.base_tag(KeyTag::Hash as u8);
            match hash {
                None => batch.delete_key(key),
                Some(hash) => batch.put_key_value(key, &hash)?,
            }
            self.stored_hash = hash;
        }
        self.needs_clear = false;
        Ok(())
    }

    fn clear(&mut self) {
        self.needs_clear = true;
        self.updates.get_mut().clear();
        *self.hash.get_mut() = None;
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
        needs_clear: bool,
        short_key: &[u8],
    ) -> Result<Arc<RwLock<W>>, ViewError> {
        let key = context.base_tag_index(KeyTag::Subview as u8, short_key);
        let context = context.clone_with_base_key(key);
        // Obtain a view and set its pending state to the default (e.g. empty) state
        let mut view = W::load(context).await?;
        if needs_clear {
            view.clear();
        }
        Ok(Arc::new(RwLock::new(view)))
    }

    /// Reads the view if existing, otherwise report an error.
    async fn checked_wrapped_view(
        context: &C,
        needs_clear: bool,
        short_key: &[u8],
    ) -> Result<Option<Arc<RwLock<W>>>, ViewError> {
        let key_index = context.base_tag_index(KeyTag::Index as u8, short_key);
        if needs_clear || !context.contains_key(&key_index).await? {
            Ok(None)
        } else {
            Ok(Some(
                Self::wrapped_view(context, false, short_key).await?,
            ))
        }
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
                    Self::wrapped_view(&self.context, self.needs_clear, short_key).await?;
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
                _entry @ Update::Removed => {
                    None
                }
            },
            None => {
                Self::checked_wrapped_view(&self.context, self.needs_clear, short_key)
                    .await?
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
    ///   let subview = view.try_load_entry_mut(vec![0, 1]).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry_mut(
        &mut self,
        short_key: Vec<u8>,
    ) -> Result<WriteGuardedView<W>, ViewError> {
        *self.hash.get_mut() = None;
        Ok(WriteGuardedView(
            self.try_load_view_mut(&short_key)
                .await?
                .try_write_arc()
                .ok_or_else(|| ViewError::TryLockError(short_key))?,
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
    ///   let subview = view.try_load_entry_or_insert(vec![0, 1]).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry_or_insert(
        &mut self,
        short_key: Vec<u8>,
    ) -> Result<ReadGuardedView<W>, ViewError> {
        *self.hash.get_mut() = None;
        Ok(ReadGuardedView(
            self.try_load_view_mut(&short_key)
                .await?
                .try_read_arc()
                .ok_or_else(|| ViewError::TryLockError(short_key))?,
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
    ///   let _subview = view.try_load_entry_or_insert(vec![0, 1]).await.unwrap();
    ///   let subview = view.try_load_entry(vec![0, 1]).await.unwrap().unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry(
        &self,
        short_key: Vec<u8>,
    ) -> Result<Option<ReadGuardedView<W>>, ViewError> {
        match self.try_load_view(&short_key).await? {
            None => Ok(None),
            Some(view) => Ok(Some(ReadGuardedView(
                view.try_read_arc()
                    .ok_or_else(|| ViewError::TryLockError(short_key))?,
            ))),
        }
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
    ///   let mut subview = view.try_load_entry_mut(vec![0, 1]).await.unwrap();
    ///   let value = subview.get_mut();
    ///   assert_eq!(*value, String::default());
    ///   view.remove_entry(vec![0, 1]);
    ///   let keys = view.keys().await.unwrap();
    ///   assert_eq!(keys.len(), 0);
    /// # })
    /// ```
    pub fn remove_entry(&mut self, short_key: Vec<u8>) {
        *self.hash.get_mut() = None;
        if self.needs_clear {
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
    ///     let mut subview = view.try_load_entry_mut(vec![0, 1]).await.unwrap();
    ///     let value = subview.get_mut();
    ///     *value = String::from("Hello");
    ///   }
    ///   view.try_reset_entry_to_default(vec![0, 1]).await.unwrap();
    ///   let mut subview = view.try_load_entry_mut(vec![0, 1]).await.unwrap();
    ///   let value = subview.get_mut();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_reset_entry_to_default(
        &mut self,
        short_key: Vec<u8>,
    ) -> Result<(), ViewError> {
        let mut view = self.try_load_entry_mut(short_key).await?;
        view.clear();
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
    ///     let mut subview = view.try_load_entry_mut(vec![0, 1]).await.unwrap();
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
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn try_load_entries_mut(
        &mut self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<WriteGuardedView<W>>, ViewError> {
        let mut selected_short_keys = Vec::new();
        *self.hash.get_mut() = None;
        let updates = self.updates.get_mut();
        for short_key in short_keys.clone() {
            match updates.entry(short_key.clone()) {
                btree_map::Entry::Occupied(entry) => {
                    let entry = entry.into_mut();
                    if let Update::Removed = entry {
                        selected_short_keys.push((short_key, true));
                    }
                }
                btree_map::Entry::Vacant(_entry) => {
                    selected_short_keys.push((short_key, self.needs_clear));
                }
            }
        }
        let mut handles = Vec::new();
        for short_key in &selected_short_keys {
            let short_key = short_key.0.clone();
            let context = self.context.clone();
            handles.push(tokio::spawn(async move {
                let key = context.base_tag_index(KeyTag::Subview as u8, &short_key);
                let context = context.clone_with_base_key(key);
                W::load(context).await
            }));
        }
        let response = futures::future::join_all(handles).await;
        for (i, view) in response.into_iter().enumerate() {
            let (short_key, to_be_cleared) = &selected_short_keys[i];
            let mut view = view??;
            if *to_be_cleared {
                view.clear();
            }
            let wrapped_view = Arc::new(RwLock::new(view));
            updates.insert(short_key.clone(), Update::Set(wrapped_view));
        }

        short_keys
            .into_iter()
            .map(|short_key| {
                if let btree_map::Entry::Occupied(entry) = updates.entry(short_key.clone()) {
                    if let Update::Set(view) = entry.into_mut() {
                        Ok(WriteGuardedView(
                            view.clone()
                                .try_write_arc()
                                .ok_or_else(|| ViewError::TryLockError(short_key))?,
                        ))
                    } else {
                        unreachable!()
                    }
                } else {
                    unreachable!()
                }
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
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn try_load_entries(
        &self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<ReadGuardedView<W>>, ViewError> {
        let mut selected_short_keys = Vec::new();
        let mut updates = self.updates.lock().await;
        for short_key in short_keys.clone() {
            match updates.entry(short_key.clone()) {
                btree_map::Entry::Occupied(entry) => {
                    let entry = entry.into_mut();
                    if let Update::Removed = entry {
                        selected_short_keys.push((short_key, true));
                    }
                }
                btree_map::Entry::Vacant(_entry) => {
                    selected_short_keys.push((short_key, self.needs_clear));
                }
            }
        }
        let mut handles = Vec::new();
        for short_key in &selected_short_keys {
            let short_key = short_key.0.clone();
            let context = self.context.clone();
            handles.push(tokio::spawn(async move {
                let key = context.base_tag_index(KeyTag::Subview as u8, &short_key);
                let context = context.clone_with_base_key(key);
                W::load(context).await
            }));
        }
        let response = futures::future::join_all(handles).await;
        for (i, view) in response.into_iter().enumerate() {
            let (short_key, to_be_cleared) = &selected_short_keys[i];
            let mut view = view??;
            if *to_be_cleared {
                view.clear();
            }
            let wrapped_view = Arc::new(RwLock::new(view));
            updates.insert(short_key.clone(), Update::Set(wrapped_view));
        }
        let mut result = Vec::new();
        for short_key in short_keys {
            result.push(ReadGuardedView(
                if let btree_map::Entry::Occupied(entry) = updates.entry(short_key.clone()) {
                    let entry = entry.into_mut();
                    if let Update::Set(view) = entry {
                        view.clone()
                            .try_read_arc()
                            .ok_or_else(|| ViewError::TryLockError(short_key))?
                    } else {
                        unreachable!()
                    }
                } else {
                    unreachable!()
                },
            ));
        }
        Ok(result)
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
    ///   view.try_load_entry_mut(vec![0, 1]).await.unwrap();
    ///   view.try_load_entry_mut(vec![0, 2]).await.unwrap();
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
    ///   view.try_load_entry_mut(vec![0, 1]).await.unwrap();
    ///   view.try_load_entry_mut(vec![0, 2]).await.unwrap();
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
        if !self.needs_clear {
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
    ///   view.try_load_entry_mut(vec![0, 1]).await.unwrap();
    ///   view.try_load_entry_mut(vec![0, 2]).await.unwrap();
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
        let hash = *self.hash.get_mut();
        match hash {
            Some(hash) => Ok(hash),
            None => {
                let mut hasher = Self::Hasher::default();
                let keys = self.keys().await?;
                hasher.update_with_bcs_bytes(&keys.len())?;
                for key in keys {
                    hasher.update_with_bytes(&key)?;
                    let view = self.try_load_entry_mut(key).await?;
                    let hash = view.hash().await?;
                    hasher.write_all(hash.as_ref())?;
                }
                let new_hash = hasher.finalize();
                let hash = self.hash.get_mut();
                *hash = Some(new_hash);
                Ok(new_hash)
            }
        }
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        let mut hash = self.hash.lock().await;
        match *hash {
            Some(hash) => Ok(hash),
            None => {
                let mut hasher = Self::Hasher::default();
                let keys = self.keys().await?;
                hasher.update_with_bcs_bytes(&keys.len())?;
                for key in keys {
                    hasher.update_with_bytes(&key)?;
                    // We can unwrap since key is in keys, so we know it is present.
                    let view = self.try_load_entry(key).await?.unwrap();
                    let hash = view.hash().await?;
                    hasher.write_all(hash.as_ref())?;
                }
                let new_hash = hasher.finalize();
                *hash = Some(new_hash);
                Ok(new_hash)
            }
        }
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
    fn context(&self) -> &C {
        self.collection.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let collection = ReentrantByteCollectionView::load(context).await?;
        Ok(ReentrantCollectionView {
            collection,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.collection.rollback()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.collection.flush(batch)
    }

    fn clear(&mut self) {
        self.collection.clear()
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
        self.collection.try_load_entry_mut(short_key).await
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
        self.collection.try_load_entry_or_insert(short_key).await
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
        self.collection.try_load_entry(short_key).await
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
    ///   view.try_reset_entry_to_default(&23).await.unwrap();
    ///   let mut subview = view.try_load_entry_mut(&23).await.unwrap();
    ///   let value = subview.get_mut();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_reset_entry_to_default<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.try_reset_entry_to_default(short_key).await
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
    #[cfg(not(target_arch = "wasm32"))]
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
    #[cfg(not(target_arch = "wasm32"))]
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
    fn context(&self) -> &C {
        self.collection.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let collection = ReentrantByteCollectionView::load(context).await?;
        Ok(ReentrantCustomCollectionView {
            collection,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.collection.rollback()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.collection.flush(batch)
    }

    fn clear(&mut self) {
        self.collection.clear()
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
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.try_load_entry_mut(short_key).await
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
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.try_load_entry_or_insert(short_key).await
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
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.try_load_entry(short_key).await
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
        Q: CustomSerialize + ?Sized,
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
    ///     view.try_reset_entry_to_default(&23).await.unwrap();
    ///     let subview = view.try_load_entry(&23).await.unwrap().unwrap();
    ///     let value = subview.get();
    ///     assert_eq!(*value, String::default());
    ///   }
    /// # })
    /// ```
    pub async fn try_reset_entry_to_default<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.try_reset_entry_to_default(short_key).await
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
    #[cfg(not(target_arch = "wasm32"))]
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
    #[cfg(not(target_arch = "wasm32"))]
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
