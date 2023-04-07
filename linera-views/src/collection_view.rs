// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::{Context, CustomSerialize, HasherOutput, KeyIterable, Update, MIN_VIEW_TAG},
    views::{HashableView, Hasher, View, ViewError},
};
use async_lock::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    borrow::Borrow,
    collections::{btree_map, BTreeMap},
    fmt::Debug,
    io::Write,
    marker::PhantomData,
    mem,
};

/// A view that supports accessing a collection of views of the same kind, indexed by a
/// `Vec<u8>`, one subview at a time.
#[derive(Debug)]
pub struct ByteCollectionView<C, W> {
    context: C,
    was_cleared: bool,
    updates: RwLock<BTreeMap<Vec<u8>, Update<W>>>,
    stored_hash: Option<HasherOutput>,
    hash: Mutex<Option<HasherOutput>>,
}

/// A read-only accessor for a particular subview in a [`CollectionView`].
pub struct ReadGuardedView<'a, W> {
    guard: RwLockReadGuard<'a, BTreeMap<Vec<u8>, Update<W>>>,
    short_key: Vec<u8>,
}

impl<'a, W> std::ops::Deref for ReadGuardedView<'a, W> {
    type Target = W;

    fn deref(&self) -> &W {
        let Update::Set(view) = self.guard.get(&self.short_key).unwrap() else { unreachable!(); };
        view
    }
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
impl<C, W> View<C> for ByteCollectionView<C, W>
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
            updates: RwLock::new(BTreeMap::new()),
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
                if let Update::Set(mut view) = update {
                    view.flush(batch)?;
                    self.add_index(batch, &index);
                }
            }
        } else {
            for (index, update) in mem::take(self.updates.get_mut()) {
                match update {
                    Update::Set(mut view) => {
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

impl<C, W> ByteCollectionView<C, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    W: View<C>,
{
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

    /// Loads a subview for the data at the given index in the collection. If the entry
    /// at that index was absent or had not been assigned then the default value is provided.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : ByteCollectionView<_, RegisterView<_,String>> = ByteCollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(vec![0,1]).await.unwrap();
    ///   let value = view.get();
    ///   assert_eq!(*value, String::from(""));
    /// # })
    /// ```
    pub async fn load_entry_mut(&mut self, short_key: Vec<u8>) -> Result<&mut W, ViewError> {
        *self.hash.get_mut() = None;
        self.do_load_entry_mut(short_key).await
    }

    /// Loads a subview for the data at the given index in the collection. If an entry
    /// was removed before or it was absent then a default entry is put on this index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : ByteCollectionView<_, RegisterView<_,String>> = ByteCollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(vec![0,1]).await.unwrap();
    ///   let view = coll.load_entry(vec![0,1]).await.unwrap();
    ///   let value = view.get();
    ///   assert_eq!(*value, String::from(""));
    /// # })
    /// ```
    pub async fn load_entry(&mut self, short_key: Vec<u8>) -> Result<&W, ViewError> {
        Ok(self.do_load_entry_mut(short_key).await?)
    }

    /// Same as `load_entry_mut` but for read-only access. May fail if one subview is
    /// already being visited.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : ByteCollectionView<_, RegisterView<_,String>> = ByteCollectionView::load(context).await.unwrap();
    ///   let view = coll.try_load_entry(vec![0,1]).await.unwrap();
    ///   let value = view.get();
    ///   assert_eq!(*value, String::from(""));
    /// # })
    /// ```
    pub async fn try_load_entry(
        &self,
        short_key: Vec<u8>,
    ) -> Result<ReadGuardedView<W>, ViewError> {
        let mut updates = self
            .updates
            .try_write()
            .ok_or(ViewError::CannotAcquireCollectionEntry)?;
        match updates.entry(short_key.clone()) {
            btree_map::Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    Update::Set(_) => {
                        let guard = RwLockWriteGuard::downgrade(updates);
                        Ok(ReadGuardedView { guard, short_key })
                    }
                    Update::Removed => {
                        let key = self
                            .context
                            .base_tag_index(KeyTag::Subview as u8, &short_key);
                        let context = self.context.clone_with_base_key(key);
                        // Obtain a view and set its pending state to the default (e.g. empty) state
                        let mut view = W::load(context).await?;
                        view.clear();
                        *entry = Update::Set(view);
                        let guard = RwLockWriteGuard::downgrade(updates);
                        Ok(ReadGuardedView { guard, short_key })
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
                entry.insert(Update::Set(view));
                let guard = RwLockWriteGuard::downgrade(updates);
                Ok(ReadGuardedView { guard, short_key })
            }
        }
    }

    /// Resets an entry to the default value.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : ByteCollectionView<_, RegisterView<_,String>> = ByteCollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(vec![0,1]).await.unwrap();
    ///   let value = view.get_mut();
    ///   *value = String::from("Hello");
    ///   coll.reset_entry_to_default(vec![0,1]).await.unwrap();
    ///   let view = coll.load_entry_mut(vec![0,1]).await.unwrap();
    ///   let value = view.get_mut();
    ///   assert_eq!(*value, String::from(""));
    /// # })
    /// ```
    pub async fn reset_entry_to_default(&mut self, short_key: Vec<u8>) -> Result<(), ViewError> {
        *self.hash.get_mut() = None;
        let view = self.load_entry_mut(short_key).await?;
        view.clear();
        Ok(())
    }

    /// Marks the entry as removed. If absent then nothing is done.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : ByteCollectionView<_, RegisterView<_,String>> = ByteCollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(vec![0,1]).await.unwrap();
    ///   let value = view.get_mut();
    ///   assert_eq!(*value, String::from(""));
    ///   coll.remove_entry(vec![0,1]);
    ///   let keys = coll.keys().await.unwrap();
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

    /// Get the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }

    async fn do_load_entry_mut(&mut self, short_key: Vec<u8>) -> Result<&mut W, ViewError> {
        match self.updates.get_mut().entry(short_key.clone()) {
            btree_map::Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    Update::Set(view) => Ok(view),
                    Update::Removed => {
                        let key = self
                            .context
                            .base_tag_index(KeyTag::Subview as u8, &short_key);
                        let context = self.context.clone_with_base_key(key);
                        // Obtain a view and set its pending state to the default (e.g. empty) state
                        let mut view = W::load(context).await?;
                        view.clear();
                        *entry = Update::Set(view);
                        let Update::Set(view) = entry else { unreachable!(); };
                        Ok(view)
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
                let Update::Set(view) = entry.insert(Update::Set(view)) else { unreachable!(); };
                Ok(view)
            }
        }
    }
}

impl<C, W> ByteCollectionView<C, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    W: View<C> + Sync,
{
    /// Applies a function f on each index (aka key). Keys are visited in the
    /// lexicographic order. If the function returns false, then the loop
    /// ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : ByteCollectionView<_, RegisterView<_,String>> = ByteCollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(vec![0,1]).await.unwrap();
    ///   let view = coll.load_entry_mut(vec![0,2]).await.unwrap();
    ///   let mut count = 0;
    ///   coll.for_each_key_while(|_key| {
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
        let updates = self.updates.write().await;
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
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : ByteCollectionView<_, RegisterView<_,String>> = ByteCollectionView::load(context).await.unwrap();
    ///   coll.load_entry_mut(vec![0,1]).await.unwrap();
    ///   coll.load_entry_mut(vec![0,2]).await.unwrap();
    ///   let mut count = 0;
    ///   coll.for_each_key(|_key| {
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

    /// Returns the list of jeys in the collection. The order is lexicographic.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::ByteCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : ByteCollectionView<_, RegisterView<_,String>> = ByteCollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(vec![0,1]).await.unwrap();
    ///   let view = coll.load_entry_mut(vec![0,2]).await.unwrap();
    ///   let keys = coll.keys().await.unwrap();
    ///   assert_eq!(keys, vec![vec![0,1],vec![0,2]]);
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
}

#[async_trait]
impl<C, W> HashableView<C> for ByteCollectionView<C, W>
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
                    let view = self.load_entry_mut(key).await?;
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

/// A view that supports accessing a collection of views of the same kind, indexed by a
/// key, one subview at a time.
#[derive(Debug)]
pub struct CollectionView<C, I, W> {
    collection: ByteCollectionView<C, W>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I, W> View<C> for CollectionView<C, I, W>
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
        let collection = ByteCollectionView::load(context).await?;
        Ok(CollectionView {
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

impl<C, I, W> CollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Serialize,
    W: View<C>,
{
    /// Loads a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CollectionView<_, u64, RegisterView<_,String>> = CollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let value = view.get();
    ///   assert_eq!(*value, String::from(""));
    /// # })
    /// ```
    pub async fn load_entry_mut<Q>(&mut self, index: &Q) -> Result<&mut W, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.load_entry_mut(short_key).await
    }

    /// Loads a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CollectionView<_, u64, RegisterView<_,String>> = CollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let view = coll.load_entry(&23).await.unwrap();
    ///   let value = view.get();
    ///   assert_eq!(*value, String::from(""));
    /// # })
    /// ```
    pub async fn load_entry<Q>(&mut self, index: &Q) -> Result<&W, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.load_entry(short_key).await
    }

    /// Same as `load_entry_mut` but for read-only access. May fail if one subview is
    /// already being visited.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CollectionView<_, u64, RegisterView<_,String>> = CollectionView::load(context).await.unwrap();
    ///   let view = coll.try_load_entry(&23).await.unwrap();
    ///   let value = view.get();
    ///   assert_eq!(*value, String::from(""));
    /// # })
    /// ```
    pub async fn try_load_entry<Q>(&self, index: &Q) -> Result<ReadGuardedView<W>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.try_load_entry(short_key).await
    }

    /// Resets an entry to the default value.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CollectionView<_, u64, RegisterView<_,String>> = CollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let value = view.get_mut();
    ///   *value = String::from("Hello");
    ///   coll.reset_entry_to_default(&23).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let value = view.get_mut();
    ///   assert_eq!(*value, String::from(""));
    /// # })
    /// ```
    pub async fn reset_entry_to_default<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.reset_entry_to_default(short_key).await
    }

    /// Removes an entry from the CollectionView. If absent nothing happens.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CollectionView<_, u64, RegisterView<_,String>> = CollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let value = view.get_mut();
    ///   assert_eq!(*value, String::from(""));
    ///   coll.remove_entry(&23);
    ///   let keys = coll.indices().await.unwrap();
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

    /// Get the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.collection.extra()
    }
}

impl<C, I, W> CollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Debug + Serialize + DeserializeOwned,
    W: View<C> + Sync,
{
    /// Returns the list of indices in the collection in the order determined by
    /// the serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CollectionView<_, u64, RegisterView<_,String>> = CollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let view = coll.load_entry_mut(&25).await.unwrap();
    ///   let indices = coll.indices().await.unwrap();
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
}

impl<C, I, W> CollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Debug + DeserializeOwned,
    W: View<C> + Sync,
{
    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization. If the function returns false then
    /// the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CollectionView<_, u64, RegisterView<_,String>> = CollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let view = coll.load_entry_mut(&24).await.unwrap();
    ///   let mut count = 0;
    ///   coll.for_each_index_while(|_key| {
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
    /// # use linera_views::collection_view::CollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CollectionView<_, u64, RegisterView<_,String>> = CollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let view = coll.load_entry_mut(&28).await.unwrap();
    ///   let mut count = 0;
    ///   coll.for_each_index(|_key| {
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
impl<C, I, W> HashableView<C> for CollectionView<C, I, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Clone + Debug + Send + Sync + Serialize + DeserializeOwned,
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

/// A MapView that serialize the indices.
#[derive(Debug)]
pub struct CustomCollectionView<C, I, W> {
    collection: ByteCollectionView<C, W>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I, W> View<C> for CustomCollectionView<C, I, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Debug,
    W: View<C> + Send + Sync,
{
    fn context(&self) -> &C {
        self.collection.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let collection = ByteCollectionView::load(context).await?;
        Ok(CustomCollectionView {
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

impl<C, I, W> CustomCollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: CustomSerialize,
    W: View<C>,
{
    /// Loads a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CustomCollectionView<_, u128, RegisterView<_,String>> = CustomCollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let value = view.get();
    ///   assert_eq!(*value, String::from(""));
    /// # })
    /// ```
    pub async fn load_entry_mut<Q>(&mut self, index: &Q) -> Result<&mut W, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.collection.load_entry_mut(short_key).await
    }

    /// Loads a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CustomCollectionView<_, u128, RegisterView<_,String>> = CustomCollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let view = coll.load_entry(&23).await.unwrap();
    ///   let value = view.get();
    ///   assert_eq!(*value, String::from(""));
    /// # })
    /// ```
    pub async fn load_entry<Q>(&mut self, index: &Q) -> Result<&W, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.collection.load_entry(short_key).await
    }

    /// Same as `load_entry_mut` but for read-only access. May fail if one subview is
    /// already being visited.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CustomCollectionView<_, u128, RegisterView<_,String>> = CustomCollectionView::load(context).await.unwrap();
    ///   let view = coll.try_load_entry(&23).await.unwrap();
    ///   let value = view.get();
    ///   assert_eq!(*value, String::from(""));
    /// # })
    /// ```
    pub async fn try_load_entry<Q>(&self, index: &Q) -> Result<ReadGuardedView<W>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.collection.try_load_entry(short_key).await
    }

    /// Marks the entry so that it is removed in the next flush.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CustomCollectionView<_, u128, RegisterView<_,String>> = CustomCollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let value = view.get_mut();
    ///   *value = String::from("Hello");
    ///   coll.reset_entry_to_default(&23).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let value = view.get_mut();
    ///   assert_eq!(*value, String::from(""));
    /// # })
    /// ```
    pub async fn reset_entry_to_default<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.collection.reset_entry_to_default(short_key).await
    }

    /// Removes an entry from the CollectionView. If absent nothing happens.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CustomCollectionView<_, u128, RegisterView<_,String>> = CustomCollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let value = view.get_mut();
    ///   assert_eq!(*value, String::from(""));
    ///   coll.remove_entry(&23);
    ///   let keys = coll.indices().await.unwrap();
    ///   assert_eq!(keys.len(), 0);
    /// # })
    /// ```
    pub fn remove_entry<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.collection.remove_entry(short_key);
        Ok(())
    }

    /// Get the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.collection.extra()
    }
}

impl<C, I, W> CustomCollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Send + Debug + CustomSerialize,
    W: View<C> + Sync,
{
    /// Returns the list of indices in the collection in the order determined by the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CustomCollectionView<_, u128, RegisterView<_,String>> = CustomCollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let view = coll.load_entry_mut(&25).await.unwrap();
    ///   let indices = coll.indices().await.unwrap();
    ///   assert_eq!(indices, vec![23,25]);
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
}

impl<C, I, W> CustomCollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Debug + CustomSerialize,
    W: View<C> + Sync,
{
    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization. If the function f returns false
    /// then the loops ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CustomCollectionView<_, u128, RegisterView<_,String>> = CustomCollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&28).await.unwrap();
    ///   let view = coll.load_entry_mut(&24).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let mut part_indices = Vec::new();
    ///   coll.for_each_index_while(|index| {
    ///     part_indices.push(index);
    ///     Ok(part_indices.len() < 2)
    ///   }).await.unwrap();
    ///   assert_eq!(part_indices, vec![23,24]);
    /// # })
    /// ```
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.collection
            .for_each_key_while(|key| {
                let index = I::from_custom_bytes::<C>(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }

    /// Applies a function on each index. Indices are visited in an order
    /// determined by the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::{create_test_context, MemoryContext};
    /// # use linera_views::collection_view::CustomCollectionView;
    /// # use linera_views::register_view::RegisterView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut coll : CustomCollectionView<_, u128, RegisterView<_,String>> = CustomCollectionView::load(context).await.unwrap();
    ///   let view = coll.load_entry_mut(&28).await.unwrap();
    ///   let view = coll.load_entry_mut(&24).await.unwrap();
    ///   let view = coll.load_entry_mut(&23).await.unwrap();
    ///   let mut indices = Vec::new();
    ///   coll.for_each_index(|index| {
    ///     indices.push(index);
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(indices, vec![23,24,28]);
    /// # })
    /// ```
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.collection
            .for_each_key(|key| {
                let index = I::from_custom_bytes::<C>(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<C, I, W> HashableView<C> for CustomCollectionView<C, I, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Clone + Debug + Send + Sync + CustomSerialize,
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
