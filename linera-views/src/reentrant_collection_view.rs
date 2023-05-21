// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::{Context, CustomSerialize, HasherOutput, KeyIterable, Update, MIN_VIEW_TAG},
    views::{HashableView, Hasher, View, ViewError},
};
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
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// A view that supports accessing a collection of views of the same kind, indexed by `Vec<u8>`,
/// possibly several subviews at a time.
#[derive(Debug)]
#[allow(clippy::type_complexity)]
pub struct ReentrantByteCollectionView<C, W> {
    context: C,
    was_cleared: bool,
    updates: Mutex<BTreeMap<Vec<u8>, Update<Arc<RwLock<W>>>>>,
    stored_hash: Option<HasherOutput>,
    hash: Mutex<Option<HasherOutput>>,
}

/// We need to find new base keys in order to implement the collection_view.
/// We do this by appending a value to the base_key.
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
        let hash = context.read_key(&key).await?;
        Ok(Self {
            context,
            was_cleared: false,
            updates: Mutex::new(BTreeMap::new()),
            stored_hash: hash,
            hash: Mutex::new(hash),
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.updates.get_mut().clear();
        *self.hash.get_mut() = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_cleared {
            self.was_cleared = false;
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
        Ok(())
    }

    fn delete(self, batch: &mut Batch) {
        batch.delete_key_prefix(self.context.base_key());
    }

    fn clear(&mut self) {
        self.was_cleared = true;
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
    /// Loads a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view : ReentrantByteCollectionView<_, RegisterView<_,String>> = ReentrantByteCollectionView::load(context).await.unwrap();
    ///   let subview = view.try_load_entry_mut(vec![0, 1]).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry_mut(
        &mut self,
        short_key: Vec<u8>,
    ) -> Result<OwnedRwLockWriteGuard<W>, ViewError> {
        *self.hash.get_mut() = None;
        let updates = self.updates.get_mut();
        match updates.entry(short_key.clone()) {
            btree_map::Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    Update::Set(view) => Ok(view.clone().try_write_owned()?),
                    Update::Removed => {
                        let key = self
                            .context
                            .base_tag_index(KeyTag::Subview as u8, &short_key);
                        let context = self.context.clone_with_base_key(key);
                        // Obtain a view and set its pending state to the default (e.g. empty) state
                        let mut view = W::load(context).await?;
                        view.clear();
                        let wrapped_view = Arc::new(RwLock::new(view));
                        *entry = Update::Set(wrapped_view.clone());
                        Ok(wrapped_view.try_write_owned()?)
                    }
                }
            }
            btree_map::Entry::Vacant(entry) => {
                let key = self
                    .context
                    .base_tag_index(KeyTag::Subview as u8, &short_key);
                let context = self.context.clone_with_base_key(key);
                let mut view = W::load(context).await?;
                if self.was_cleared {
                    view.clear();
                }
                let wrapped_view = Arc::new(RwLock::new(view));
                entry.insert(Update::Set(wrapped_view.clone()));
                Ok(wrapped_view.try_write_owned()?)
            }
        }
    }

    /// Loads a read-only access to a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view : ReentrantByteCollectionView<_, RegisterView<_,String>> = ReentrantByteCollectionView::load(context).await.unwrap();
    ///   let subview = view.try_load_entry(vec![0, 1]).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry(
        &self,
        short_key: Vec<u8>,
    ) -> Result<OwnedRwLockReadGuard<W>, ViewError> {
        let mut updates = self.updates.lock().await;
        match updates.entry(short_key.clone()) {
            btree_map::Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    Update::Set(view) => Ok(view.clone().try_read_owned()?),
                    Update::Removed => {
                        let key = self
                            .context
                            .base_tag_index(KeyTag::Subview as u8, &short_key);
                        let context = self.context.clone_with_base_key(key);
                        // Obtain a view and set its pending state to the default (e.g. empty) state
                        let mut view = W::load(context).await?;
                        view.clear();
                        let wrapped_view = Arc::new(RwLock::new(view));
                        *entry = Update::Set(wrapped_view.clone());
                        Ok(wrapped_view.try_read_owned()?)
                    }
                }
            }
            btree_map::Entry::Vacant(entry) => {
                let key = self
                    .context
                    .base_tag_index(KeyTag::Subview as u8, &short_key);
                let context = self.context.clone_with_base_key(key);
                let mut view = W::load(context).await?;
                if self.was_cleared {
                    view.clear();
                }
                let wrapped_view = Arc::new(RwLock::new(view));
                entry.insert(Update::Set(wrapped_view.clone()));
                Ok(wrapped_view.try_read_owned()?)
            }
        }
    }

    /// Removes an entry. If absent then nothing happens.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
        if self.was_cleared {
            self.updates.get_mut().remove(&short_key);
        } else {
            self.updates.get_mut().insert(short_key, Update::Removed);
        }
    }

    /// Marks the entry so that it is removed in the next flush.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
        *self.hash.get_mut() = None;
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
    /// Load multiple entries for write at once.
    /// The entries in short_keys have to be all distincts.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
    pub async fn try_load_entries_mut(
        &mut self,
        short_keys: Vec<Vec<u8>>,
    ) -> Result<Vec<OwnedRwLockWriteGuard<W>>, ViewError> {
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
                    selected_short_keys.push((short_key, self.was_cleared));
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
            result.push(
                if let btree_map::Entry::Occupied(entry) = updates.entry(short_key) {
                    let entry = entry.into_mut();
                    if let Update::Set(view) = entry {
                        view.clone().try_write_owned()?
                    } else {
                        unreachable!()
                    }
                } else {
                    unreachable!()
                },
            );
        }
        Ok(result)
    }

    /// Load multiple entries for read at once.
    /// The entries in short_keys have to be all distincts.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
    ) -> Result<Vec<OwnedRwLockReadGuard<W>>, ViewError> {
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
                    selected_short_keys.push((short_key, self.was_cleared));
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
            result.push(
                if let btree_map::Entry::Occupied(entry) = updates.entry(short_key) {
                    let entry = entry.into_mut();
                    if let Update::Set(view) = entry {
                        view.clone().try_read_owned()?
                    } else {
                        unreachable!()
                    }
                } else {
                    unreachable!()
                },
            );
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
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
        if !self.was_cleared {
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
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
        let mut hash = self.hash.try_lock()?;
        match *hash {
            Some(hash) => Ok(hash),
            None => {
                let mut hasher = Self::Hasher::default();
                let keys = self.keys().await?;
                hasher.update_with_bcs_bytes(&keys.len())?;
                for key in keys {
                    hasher.update_with_bytes(&key)?;
                    let view = self.try_load_entry(key).await?;
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

/// A view that supports accessing a collection of views of the same kind, indexed by key,
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

    fn delete(self, batch: &mut Batch) {
        self.collection.delete(batch)
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
    /// was removed before then a default entry is put on this index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   let subview = view.try_load_entry_mut(&23).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry_mut<Q>(
        &mut self,
        index: &Q,
    ) -> Result<OwnedRwLockWriteGuard<W>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.try_load_entry_mut(short_key).await
    }

    /// Loads a read-only access to a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   let subview = view.try_load_entry(&23).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry<Q>(&self, index: &Q) -> Result<OwnedRwLockReadGuard<W>, ViewError>
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
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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

impl<'a, C, I, W> ReentrantCollectionView<C, I, W>
where
    C: Context + Send + Clone + 'static,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Debug + Serialize + DeserializeOwned,
    W: View<C> + Send + Sync + 'static,
{
    /// Load multiple entries for write at once.
    /// The entries in indices have to be all distincts.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   let indices = vec![23, 42];
    ///   let subviews = view.try_load_entries_mut(&indices).await.unwrap();
    ///   let value1 = subviews[0].get();
    ///   let value2 = subviews[1].get();
    ///   assert_eq!(*value1, String::default());
    ///   assert_eq!(*value2, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries_mut<Q>(
        &'a mut self,
        indices: impl IntoIterator<Item = &'a Q>,
    ) -> Result<Vec<OwnedRwLockWriteGuard<W>>, ViewError>
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

    /// Load multiple entries for read at once.
    /// The entries in indices have to be all distincts.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view : ReentrantCollectionView<_, u64, RegisterView<_,String>> = ReentrantCollectionView::load(context).await.unwrap();
    ///   let indices = vec![23, 42];
    ///   let subviews = view.try_load_entries(&indices).await.unwrap();
    ///   let value1 = subviews[0].get();
    ///   let value2 = subviews[1].get();
    ///   assert_eq!(*value1, String::default());
    ///   assert_eq!(*value2, String::default());
    /// # })
    /// ```
    pub async fn try_load_entries<Q>(
        &'a self,
        indices: impl IntoIterator<Item = &'a Q>,
    ) -> Result<Vec<OwnedRwLockReadGuard<W>>, ViewError>
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
    /// by the serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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

    fn delete(self, batch: &mut Batch) {
        self.collection.delete(batch)
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
    /// was removed before then a default entry is put on this index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   let subview = view.try_load_entry_mut(&23).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry_mut<Q>(
        &mut self,
        index: &Q,
    ) -> Result<OwnedRwLockWriteGuard<W>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes()?;
        self.collection.try_load_entry_mut(short_key).await
    }

    /// Loads a read-only access to a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   let subview = view.try_load_entry(&23).await.unwrap();
    ///   let value = subview.get();
    ///   assert_eq!(*value, String::default());
    /// # })
    /// ```
    pub async fn try_load_entry<Q>(&self, index: &Q) -> Result<OwnedRwLockReadGuard<W>, ViewError>
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
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view : ReentrantCustomCollectionView<_, u128, RegisterView<_,String>> = ReentrantCustomCollectionView::load(context).await.unwrap();
    ///   {
    ///     let mut subview = view.try_load_entry_mut(&23).await.unwrap();
    ///     let value = subview.get_mut();
    ///     *value = String::from("Hello");
    ///   }
    ///   {
    ///     view.try_reset_entry_to_default(&23).await.unwrap();
    ///     let subview = view.try_load_entry(&23).await.unwrap();
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
    /// Load multiple entries for write at once.
    /// The entries in indices have to be all distincts.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
    ) -> Result<Vec<OwnedRwLockWriteGuard<W>>, ViewError>
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

    /// Load multiple entries for read at once.
    /// The entries in indices have to be all distincts.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
    ) -> Result<Vec<OwnedRwLockReadGuard<W>>, ViewError>
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
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::reentrant_collection_view::ReentrantCustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
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
