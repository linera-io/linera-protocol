// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::{Context, CustomSerialize, HasherOutput, KeyIterable, Update, MIN_VIEW_TAG},
    views::{HashableView, Hasher, View, ViewError},
};
use async_lock::Mutex;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{borrow::Borrow, collections::BTreeMap, fmt::Debug, marker::PhantomData, mem};

/// Key tags to create the sub-keys of a SetView on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the indices of the setview
    Index = MIN_VIEW_TAG,
    /// Prefix for the hash
    Hash,
}

/// A view that supports inserting and removing values indexed by a key.
#[derive(Debug)]
pub struct ByteSetView<C> {
    context: C,
    was_cleared: bool,
    updates: BTreeMap<Vec<u8>, Update<()>>,
    stored_hash: Option<HasherOutput>,
    hash: Mutex<Option<HasherOutput>>,
}

#[async_trait]
impl<C> View<C> for ByteSetView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
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
            updates: BTreeMap::new(),
            stored_hash: hash,
            hash: Mutex::new(hash),
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.updates.clear();
        *self.hash.get_mut() = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_cleared {
            self.was_cleared = false;
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(&mut self.updates) {
                if let Update::Set(_) = update {
                    let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                    batch.put_key_value_bytes(key, Vec::new());
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                match update {
                    Update::Removed => batch.delete_key(key),
                    Update::Set(_) => batch.put_key_value_bytes(key, Vec::new()),
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
        self.updates.clear();
        *self.hash.get_mut() = None;
    }
}

impl<C> ByteSetView<C>
where
    C: Context,
    ViewError: From<C::Error>,
{
    /// Set or insert a value.
    pub fn insert(&mut self, short_key: Vec<u8>) {
        *self.hash.get_mut() = None;
        self.updates.insert(short_key, Update::Set(()));
    }

    /// Remove a value.
    pub fn remove(&mut self, short_key: Vec<u8>) {
        *self.hash.get_mut() = None;
        if self.was_cleared {
            self.updates.remove(&short_key);
        } else {
            self.updates.insert(short_key, Update::Removed);
        }
    }

    /// Obtain the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C> ByteSetView<C>
where
    C: Context,
    ViewError: From<C::Error>,
{
    /// Return true if the given index exists in the set.
    pub async fn contains(&self, short_key: Vec<u8>) -> Result<bool, ViewError> {
        if let Some(update) = self.updates.get(&short_key) {
            let value = match update {
                Update::Removed => false,
                Update::Set(()) => true,
            };
            return Ok(value);
        }
        if self.was_cleared {
            return Ok(false);
        }
        let key = self.context.base_tag_index(KeyTag::Index as u8, &short_key);
        match self.context.read_key_bytes(&key).await? {
            None => Ok(false),
            Some(_) => Ok(true),
        }
    }
}

impl<C> ByteSetView<C>
where
    C: Context,
    ViewError: From<C::Error>,
{
    /// Return the list of keys in the set.
    pub async fn keys(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut keys = Vec::new();
        self.for_each_key(|key| {
            keys.push(key.to_vec());
            Ok(())
        })
        .await?;
        Ok(keys)
    }

    /// Execute a function on each serialized index (aka key). Keys are visited in a
    /// lexicographic order. If the function returns false, then it exits
    async fn for_each_key_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let mut updates = self.updates.iter();
        let mut update = updates.next();
        if !self.was_cleared {
            let base = self.context.base_tag(KeyTag::Index as u8);
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

    /// Execute a function on each serialized index (aka key). Keys are visited in a
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

    async fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        let mut hasher = sha3::Sha3_256::default();
        let mut count = 0;
        self.for_each_key(|key| {
            count += 1;
            hasher.update_with_bytes(key)?;
            Ok(())
        })
        .await?;
        hasher.update_with_bcs_bytes(&count)?;
        Ok(hasher.finalize())
    }
}

#[async_trait]
impl<C> HashableView<C> for ByteSetView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        let hash = *self.hash.get_mut();
        match hash {
            Some(hash) => Ok(hash),
            None => {
                let new_hash = self.compute_hash().await?;
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
                let new_hash = self.compute_hash().await?;
                *hash = Some(new_hash);
                Ok(new_hash)
            }
        }
    }
}

/// A ['View'] implementing the set functionality with the index I being a non-trivial type
#[derive(Debug)]
pub struct SetView<C, I> {
    set: ByteSetView<C>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I> View<C> for SetView<C, I>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize,
{
    fn context(&self) -> &C {
        self.set.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let set = ByteSetView::load(context).await?;
        Ok(Self {
            set,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.set.rollback()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.set.flush(batch)
    }

    fn delete(self, batch: &mut Batch) {
        self.set.delete(batch)
    }

    fn clear(&mut self) {
        self.set.clear()
    }
}

impl<C, I> SetView<C, I>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Serialize,
{
    /// Set or insert a value.
    pub fn insert<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.set.insert(short_key);
        Ok(())
    }

    /// Remove a value.
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.set.remove(short_key);
        Ok(())
    }

    /// Obtain the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.set.extra()
    }
}

impl<C, I> SetView<C, I>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Serialize,
{
    /// Return true if the given index exists in the set.
    pub async fn contains<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.set.contains(short_key).await
    }
}

impl<C, I> SetView<C, I>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Serialize + DeserializeOwned,
{
    /// Return the list of indices in the set.
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::<I>::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Executes a function on each index. Indices are visited in a stable, yet unspecified
    /// order determined by the serialization. If the function returns false, then it exits.
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.set
            .for_each_key_while(|key| {
                let index = C::deserialize_value(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }

    /// Executes a function on each index. Indices are visited in a stable, yet unspecified
    /// order determined by the serialization.
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.set
            .for_each_key(|key| {
                let index = C::deserialize_value(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<C, I> HashableView<C> for SetView<C, I>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash().await
    }
}

/// A ['View'] implementing the set functionality with the index I being a non-trivial type
#[derive(Debug)]
pub struct CustomSetView<C, I> {
    set: ByteSetView<C>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I> View<C> for CustomSetView<C, I>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
{
    fn context(&self) -> &C {
        self.set.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let set = ByteSetView::load(context).await?;
        Ok(Self {
            set,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.set.rollback()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.set.flush(batch)
    }

    fn delete(self, batch: &mut Batch) {
        self.set.delete(batch)
    }

    fn clear(&mut self) {
        self.set.clear()
    }
}

impl<C, I> CustomSetView<C, I>
where
    C: Context,
    ViewError: From<C::Error>,
    I: CustomSerialize,
{
    /// Set or insert a value.
    pub fn insert<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.set.insert(short_key);
        Ok(())
    }

    /// Remove a value.
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.set.remove(short_key);
        Ok(())
    }

    /// Obtain the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.set.extra()
    }
}

impl<C, I> CustomSetView<C, I>
where
    C: Context,
    ViewError: From<C::Error>,
    I: CustomSerialize,
{
    /// Return true if the given index exists in the set.
    pub async fn contains<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize + ?Sized,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.set.contains(short_key).await
    }
}

impl<C, I> CustomSetView<C, I>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + CustomSerialize,
{
    /// Return the list of indices in the set.
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::<I>::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Executes a function on each index. Indices are visited in a stable, yet unspecified
    /// order determined by the custom serialization. If the function does return false, then the iteration exits.
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.set
            .for_each_key_while(|key| {
                let index = I::from_custom_bytes::<C>(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }

    /// Executes a function on each index. Indices are visited in a stable, yet unspecified
    /// order determined by the custom serialization.
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.set
            .for_each_key(|key| {
                let index = I::from_custom_bytes::<C>(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<C, I> HashableView<C> for CustomSetView<C, I>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Clone + Send + Sync + CustomSerialize,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.set.hash().await
    }
}
