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
    /// Prefix for specifying an index and serves to indicate the existence of an entry in the collection
    Index = MIN_VIEW_TAG,
    /// Prefix for specifying as the prefix for the sub-view.
    Subview,
    /// Prefix for the hash value
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
                    self.add_index(batch, &index)?;
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
                        self.add_index(batch, &index)?;
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

    fn add_index(&self, batch: &mut Batch, index: &[u8]) -> Result<(), ViewError> {
        let key = self.get_index_key(index);
        batch.put_key_value(key, &())?;
        Ok(())
    }
}

impl<C, W> ReentrantByteCollectionView<C, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    W: View<C> + Send + Sync,
{
    /// Obtain a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
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

    /// Obtain a read-only access to a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
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

    /// Mark the entry so that it is removed in the next flush
    pub fn remove_entry(&mut self, short_key: Vec<u8>) -> Result<(), ViewError> {
        *self.hash.get_mut() = None;
        if self.was_cleared {
            self.updates.get_mut().remove(&short_key);
        } else {
            self.updates.get_mut().insert(short_key, Update::Removed);
        }
        Ok(())
    }

    /// Mark the entry so that it is removed in the next flush
    pub async fn try_reset_entry_to_default(
        &mut self,
        short_key: Vec<u8>,
    ) -> Result<(), ViewError> {
        *self.hash.get_mut() = None;
        let mut view = self.try_load_entry_mut(short_key).await?;
        view.clear();
        Ok(())
    }

    /// Obtain the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, W> ReentrantByteCollectionView<C, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    W: View<C> + Send + Sync,
{
    /// Return the list of indices in the collection.
    pub async fn keys(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut keys = Vec::new();
        self.for_each_key(|key| {
            keys.push(key.to_vec());
            Ok(())
        })
        .await?;
        Ok(keys)
    }

    /// Executes a function on each serialized index (aka key). Keys are visited in a
    /// lexicographic order. If the function returns false then the loop
    /// prematurely ends.
    async fn for_each_key_while<F>(&self, mut f: F) -> Result<(), ViewError>
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

    /// Executes a function on each serialized index (aka key). Keys are visited in a
    /// lexicographic order.
    async fn for_each_key<F>(&self, mut f: F) -> Result<(), ViewError>
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
    /// Obtain a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
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

    /// Obtain a read-only access to a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    pub async fn try_load_entry<Q>(&self, index: &Q) -> Result<OwnedRwLockReadGuard<W>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.try_load_entry(short_key).await
    }

    /// Mark the entry so that it is removed in the next flush
    pub fn remove_entry<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.remove_entry(short_key)
    }

    /// Mark the entry so that it is removed in the next flush
    pub async fn try_reset_entry_to_default<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.collection.try_reset_entry_to_default(short_key).await
    }

    /// Obtain the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.collection.extra()
    }
}

impl<C, I, W> ReentrantCollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Debug + Serialize + DeserializeOwned,
    W: View<C> + Send + Sync,
{
    /// Return the list of indices in the collection.
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Executes a function on each index. Indices are visited in an order
    /// determined by the serialization. If the function f returns false then
    /// the loop prematurely ends.
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

    /// Executes a function on each index. Indices are visited in an order
    /// determined by the serialization.
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
    /// Obtain a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    pub async fn try_load_entry_mut<Q>(
        &mut self,
        index: &Q,
    ) -> Result<OwnedRwLockWriteGuard<W>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.collection.try_load_entry_mut(short_key).await
    }

    /// Obtain a read-only access to a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    pub async fn try_load_entry<Q>(&self, index: &Q) -> Result<OwnedRwLockReadGuard<W>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.collection.try_load_entry(short_key).await
    }

    /// Mark the entry so that it is removed in the next flush
    pub fn remove_entry<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.collection.remove_entry(short_key)
    }

    /// Mark the entry so that it is removed in the next flush
    pub async fn try_reset_entry_to_default<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.collection.try_reset_entry_to_default(short_key).await
    }

    /// Obtain the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.collection.extra()
    }
}

impl<C, I, W> ReentrantCustomCollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Debug + CustomSerialize,
    W: View<C> + Send + Sync,
{
    /// Return the list of indices in the collection.
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Executes a function on each index. Indices are visited in an order
    /// determined by the custom serialization.
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.collection
            .for_each_key(|key| {
                let index = I::from_custom_bytes::<C>(key)?;
                f(index)?;
                Ok(())
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
