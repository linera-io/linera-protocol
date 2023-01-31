// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    common::{Batch, Context, HashOutput, KeyIterable, Update},
    views::{HashableView, Hasher, View, ViewError},
};
use async_std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::{btree_map, BTreeMap},
    fmt::Debug,
    io::Write,
    marker::PhantomData,
    mem,
};

/// A view that supports accessing a collection of views of the same kind, indexed by a
/// key, one subview at a time.
#[derive(Debug)]
pub struct CollectionView<C, I, W> {
    context: C,
    was_cleared: bool,
    updates: RwLock<BTreeMap<Vec<u8>, Update<W>>>,
    _phantom: PhantomData<I>,
    stored_hash: Option<HashOutput>,
    hash: Option<HashOutput>,
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
    /// Prefix for specifying an index and serves to indicate the existence of an entry in the collection
    Index = 0,
    /// Prefix for specifying as the prefix for the sub-view.
    Subview = 1,
    /// Prefix for the hash value
    Hash = 2,
}

#[async_trait]
impl<C, I, W> View<C> for CollectionView<C, I, W>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Debug + Clone + Serialize + DeserializeOwned,
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
            _phantom: PhantomData,
            stored_hash: hash,
            hash,
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.updates.get_mut().clear();
        self.hash = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_cleared {
            self.was_cleared = false;
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(self.updates.get_mut()) {
                if let Update::Set(mut view) = update {
                    view.flush(batch)?;
                    self.add_index(batch, &index)?;
                }
            }
        } else {
            for (index, update) in mem::take(self.updates.get_mut()) {
                match update {
                    Update::Set(mut view) => {
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
        if self.stored_hash != self.hash {
            let key = self.context.base_tag(KeyTag::Hash as u8);
            match self.hash {
                None => batch.delete_key(key),
                Some(hash) => batch.put_key_value(key, &hash)?,
            }
            self.stored_hash = self.hash;
        }
        Ok(())
    }

    fn delete(self, batch: &mut Batch) {
        batch.delete_key_prefix(self.context.base_key());
    }

    fn clear(&mut self) {
        self.was_cleared = true;
        self.updates.get_mut().clear();
        self.hash = None;
    }
}

impl<C, I, W> CollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Debug + Serialize + DeserializeOwned,
    W: View<C> + Sync,
{
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

    /// Obtain a subview for the data at the given index in the collection. If an entry
    /// was removed before then a default entry is put on this index.
    pub async fn load_entry_mut(&mut self, index: I) -> Result<&mut W, ViewError> {
        self.hash = None;
        let short_key = C::derive_short_key(&index)?;
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

    /// Same as `load_entry_mut` but for read-only access. May block the current async thread.
    pub async fn load_entry(&self, index: I) -> Result<ReadGuardedView<W>, ViewError> {
        let short_key = C::derive_short_key(&index)?;
        let mut updates = self.updates.write().await;
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

    /// Mark the entry so that it is removed in the next flush
    pub fn remove_entry(&mut self, index: I) -> Result<(), ViewError> {
        self.hash = None;
        let short_key = C::derive_short_key(&index)?;
        if self.was_cleared {
            self.updates.get_mut().remove(&short_key);
        } else {
            self.updates.get_mut().insert(short_key, Update::Removed);
        }
        Ok(())
    }

    /// Mark the entry so that it is removed in the next flush
    pub async fn reset_entry_to_default(&mut self, index: I) -> Result<(), ViewError> {
        self.hash = None;
        let view = self.load_entry_mut(index).await?;
        view.clear();
        Ok(())
    }

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

    /// Obtain the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, I, W> CollectionView<C, I, W>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Clone + Debug + Sync + Send + Serialize + DeserializeOwned,
    W: View<C> + Sync,
{
    /// Execute a function on each serialized index (aka key). Keys are visited in a
    /// stable, yet unspecified order.
    async fn for_each_key<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
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
                                f(key)?;
                            }
                            update = updates.next();
                            if key == index {
                                break;
                            }
                        }
                        _ => {
                            f(index)?;
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = update {
            if let Update::Set(_) = value {
                f(key)?;
            }
            update = updates.next();
        }
        Ok(())
    }

    /// Execute a function on each index. Indices are visited in a stable, yet unspecified
    /// order.
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.for_each_key(|key| {
            let index = C::deserialize_value(key)?;
            f(index)?;
            Ok(())
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
    I: Clone + Debug + Send + Sync + Serialize + DeserializeOwned + 'static,
    W: HashableView<C> + Send + Sync + 'static,
{
    type Hasher = sha2::Sha512;

    async fn hash(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        match self.hash {
            Some(hash) => Ok(hash),
            None => {
                let mut hasher = Self::Hasher::default();
                let indices = self.indices().await?;
                hasher.update_with_bcs_bytes(&indices.len())?;
                for index in indices {
                    hasher.update_with_bcs_bytes(&index)?;
                    let view = self.load_entry_mut(index).await?;
                    let hash = view.hash().await?;
                    hasher.write_all(hash.as_ref())?;
                }
                let hash = hasher.finalize();
                self.hash = Some(hash);
                Ok(hash)
            }
        }
    }
}
