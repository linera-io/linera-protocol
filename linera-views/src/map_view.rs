// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::{
        Context, CustomSerialize, HasherOutput, KeyIterable, KeyValueIterable, Update, MIN_VIEW_TAG,
    },
    views::{HashableView, Hasher, View, ViewError},
};
use async_lock::Mutex;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{borrow::Borrow, collections::BTreeMap, fmt::Debug, marker::PhantomData, mem};

/// Key tags to create the sub-keys of a MapView on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the indices of the view
    Index = MIN_VIEW_TAG,
    /// Prefix for the hash
    Hash,
}

/// A view that supports inserting and removing values indexed by `Vec<u8>`
#[derive(Debug)]
pub struct ByteMapView<C, V> {
    context: C,
    was_cleared: bool,
    updates: BTreeMap<Vec<u8>, Update<V>>,
    stored_hash: Option<HasherOutput>,
    hash: Mutex<Option<HasherOutput>>,
}

#[async_trait]
impl<C, V> View<C> for ByteMapView<C, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    V: Send + Sync + Serialize,
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
                if let Update::Set(value) = update {
                    let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                    batch.put_key_value(key, &value)?;
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                match update {
                    Update::Removed => batch.delete_key(key),
                    Update::Set(value) => batch.put_key_value(key, &value)?,
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

impl<C, V> ByteMapView<C, V>
where
    C: Context,
    ViewError: From<C::Error>,
{
    /// Set or insert a value.
    pub fn insert(&mut self, short_key: Vec<u8>, value: V) {
        *self.hash.get_mut() = None;
        self.updates.insert(short_key, Update::Set(value));
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

impl<C, V> ByteMapView<C, V>
where
    C: Context,
    ViewError: From<C::Error>,
    V: Clone + DeserializeOwned + 'static,
{
    /// Read the value at the given position, if any.
    pub async fn get(&self, short_key: Vec<u8>) -> Result<Option<V>, ViewError> {
        if let Some(update) = self.updates.get(&short_key) {
            let value = match update {
                Update::Removed => None,
                Update::Set(value) => Some(value.clone()),
            };
            return Ok(value);
        }
        if self.was_cleared {
            return Ok(None);
        }
        let key = self.context.base_tag_index(KeyTag::Index as u8, &short_key);
        Ok(self.context.read_key(&key).await?)
    }

    /// load the value in updates if that is at all possible
    async fn load_value(&mut self, short_key: &[u8]) -> Result<(), ViewError> {
        if !self.was_cleared && !self.updates.contains_key(short_key) {
            let key = self.context.base_tag_index(KeyTag::Index as u8, short_key);
            let value = self.context.read_key(&key).await?;
            if let Some(value) = value {
                self.updates.insert(short_key.to_vec(), Update::Set(value));
            }
        }
        Ok(())
    }

    /// Obtain a mutable reference to a value at a given position if available
    pub async fn get_mut(&mut self, short_key: Vec<u8>) -> Result<Option<&mut V>, ViewError> {
        self.load_value(&short_key).await?;
        if let Some(update) = self.updates.get_mut(&short_key.clone()) {
            let value = match update {
                Update::Removed => None,
                Update::Set(value) => Some(value),
            };
            return Ok(value);
        }
        Ok(None)
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context,
    ViewError: From<C::Error>,
    V: Sync + Serialize + DeserializeOwned + 'static,
{
    /// Executes a function on each serialized index (aka key). Keys are visited
    /// in a lexicographic order. If the function returns false, then
    /// the loop exits
    pub async fn for_each_key_while<F>(&self, mut f: F) -> Result<(), ViewError>
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

    /// Executes a function on each serialized index (aka key). Keys are visited
    /// in a lexicographic order.
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

    /// Executes a function on each serialized index (aka key). Keys and values are visited
    /// in a lexicographic order. If the function returns false, then
    /// the loop exits.
    pub async fn for_each_key_value_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool, ViewError> + Send,
    {
        let mut updates = self.updates.iter();
        let mut update = updates.next();
        if !self.was_cleared {
            let base = self.context.base_tag(KeyTag::Index as u8);
            for entry in self
                .context
                .find_key_values_by_prefix(&base)
                .await?
                .iterator()
            {
                let (index, bytes) = entry?;
                loop {
                    match update {
                        Some((key, value)) if key.as_slice() <= index => {
                            if let Update::Set(value) = value {
                                let bytes = bcs::to_bytes(value)?;
                                if !f(key, &bytes)? {
                                    return Ok(());
                                }
                            }
                            update = updates.next();
                            if key == index {
                                break;
                            }
                        }
                        _ => {
                            if !f(index, bytes)? {
                                return Ok(());
                            }
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = update {
            if let Update::Set(value) = value {
                let bytes = bcs::to_bytes(value)?;
                if !f(key, &bytes)? {
                    return Ok(());
                }
            }
            update = updates.next();
        }
        Ok(())
    }

    /// Executes a function on each serialized index (aka key). Keys and values are visited
    /// in a lexicographic order.
    pub async fn for_each_key_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_key_value_while(|key, value| {
            f(key, value)?;
            Ok(true)
        })
        .await
    }

    async fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        let mut hasher = sha3::Sha3_256::default();
        let mut count = 0;
        self.for_each_key_value(|index, value| {
            count += 1;
            hasher.update_with_bytes(index)?;
            hasher.update_with_bytes(value)?;
            Ok(())
        })
        .await?;
        hasher.update_with_bcs_bytes(&count)?;
        Ok(hasher.finalize())
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context,
    ViewError: From<C::Error>,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtain a mutable reference to a value at a given position.
    /// Default value if the index is missing.
    pub async fn get_mut_or_default(&mut self, short_key: Vec<u8>) -> Result<&mut V, ViewError> {
        use std::collections::btree_map::Entry;

        let update = match self.updates.entry(short_key.clone()) {
            Entry::Vacant(e) if self.was_cleared => e.insert(Update::Set(V::default())),
            Entry::Vacant(e) => {
                let key = self.context.base_tag_index(KeyTag::Index as u8, &short_key);
                let value = self.context.read_key(&key).await?.unwrap_or_default();
                e.insert(Update::Set(value))
            }
            Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                match entry {
                    Update::Set(_) => &mut *entry,
                    Update::Removed => {
                        *entry = Update::Set(V::default());
                        &mut *entry
                    }
                }
            }
        };
        let Update::Set(value) = update else { unreachable!() };
        Ok(value)
    }
}

#[async_trait]
impl<C, V> HashableView<C> for ByteMapView<C, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    V: Send + Sync + Serialize + DeserializeOwned + 'static,
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

/// A View that actually has a type for keys. The ordering of the entries
/// is determined by the serialization of the context.
#[derive(Debug)]
pub struct MapView<C, I, V> {
    map: ByteMapView<C, V>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I, V> View<C> for MapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize,
    V: Send + Sync + Serialize,
{
    fn context(&self) -> &C {
        self.map.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let map = ByteMapView::load(context).await?;
        Ok(MapView {
            map,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.map.rollback()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.map.flush(batch)
    }

    fn delete(self, batch: &mut Batch) {
        self.map.delete(batch)
    }

    fn clear(&mut self) {
        self.map.clear()
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Serialize,
{
    /// Set or insert a value.
    pub fn insert<Q>(&mut self, index: &Q, value: V) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.insert(short_key, value);
        Ok(())
    }

    /// Remove a value.
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.remove(short_key);
        Ok(())
    }

    /// Obtain the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.map.extra()
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Serialize,
    V: Clone + DeserializeOwned + 'static,
{
    /// Read the value at the given position, if any.
    pub async fn get<Q>(&self, index: &Q) -> Result<Option<V>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.get(short_key).await
    }

    /// Obtain a mutable reference to a value at a given position if available
    pub async fn get_mut<Q>(&mut self, index: &Q) -> Result<Option<&mut V>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.get_mut(short_key).await
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Sync + Send + Serialize + DeserializeOwned,
    V: Sync + Serialize + DeserializeOwned + 'static,
{
    /// Return the list of indices in the map.
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::<I>::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Executes a function on each index. Indices are visited in a stable,
    /// yet unspecified order determined by the serialization.
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.map
            .for_each_key_while(|key| {
                let index = C::deserialize_value(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }

    /// Executes a function on each index. Indices are visited in a stable,
    /// yet unspecified order determined by the serialization.
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.map
            .for_each_key(|key| {
                let index = C::deserialize_value(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }

    /// Executes a function on each index and value in the map. Indices and values are
    /// visited in a stable, yet unspecified order determined by the serialization.
    pub async fn for_each_index_value_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<bool, ViewError> + Send,
    {
        self.map
            .for_each_key_value_while(|key, bytes| {
                let index = C::deserialize_value(key)?;
                let value = C::deserialize_value(bytes)?;
                f(index, value)
            })
            .await?;
        Ok(())
    }

    /// Execute a function on each index and value in the map. Indices and values are
    /// visited in a stable, yet unspecified order determined by the serialization.
    pub async fn for_each_index_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<(), ViewError> + Send,
    {
        self.map
            .for_each_key_value(|key, bytes| {
                let index = C::deserialize_value(key)?;
                let value = C::deserialize_value(bytes)?;
                f(index, value)
            })
            .await?;
        Ok(())
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Serialize,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtain a mutable reference to a value at a given position.
    /// Default value if the index is missing.
    pub async fn get_mut_or_default<Q>(&mut self, index: &Q) -> Result<&mut V, ViewError>
    where
        I: Borrow<Q>,
        Q: Sync + Send + Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.get_mut_or_default(short_key).await
    }
}

#[async_trait]
impl<C, I, V> HashableView<C> for MapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize + DeserializeOwned,
    V: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash().await
    }
}

/// A Custom MapView that uses the custom serialization
#[derive(Debug)]
pub struct CustomMapView<C, I, V> {
    map: ByteMapView<C, V>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I, V> View<C> for CustomMapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
    V: Send + Sync + Serialize,
{
    fn context(&self) -> &C {
        self.map.context()
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let map = ByteMapView::load(context).await?;
        Ok(CustomMapView {
            map,
            _phantom: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.map.rollback()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.map.flush(batch)
    }

    fn delete(self, batch: &mut Batch) {
        self.map.delete(batch)
    }

    fn clear(&mut self) {
        self.map.clear()
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context,
    ViewError: From<C::Error>,
    I: CustomSerialize,
{
    /// Set or insert a value.
    pub fn insert<Q>(&mut self, index: &Q, value: V) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized + CustomSerialize,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.map.insert(short_key, value);
        Ok(())
    }

    /// Remove a value.
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized + CustomSerialize,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.map.remove(short_key);
        Ok(())
    }

    /// Obtain the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.map.extra()
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context,
    ViewError: From<C::Error>,
    I: CustomSerialize,
    V: Clone + DeserializeOwned + 'static,
{
    /// Read the value at the given position, if any.
    pub async fn get<Q>(&self, index: &Q) -> Result<Option<V>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized + CustomSerialize,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.map.get(short_key).await
    }

    /// Obtain a mutable reference to a value at a given position if available
    pub async fn get_mut<Q>(&mut self, index: &Q) -> Result<Option<&mut V>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized + CustomSerialize,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.map.get_mut(short_key).await
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Sync + Send + CustomSerialize,
    V: Sync + Serialize + DeserializeOwned + 'static,
{
    /// Return the list of indices in the map.
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::<I>::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Execute a function on each index. Indices are visited in a stable, yet unspecified
    /// order determined by the custom serialization.
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        self.map
            .for_each_key_while(|key| {
                let index = I::from_custom_bytes::<C>(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }

    /// Execute a function on each index. Indices are visited in a stable, yet unspecified
    /// order determined by the custom serialization.
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.map
            .for_each_key(|key| {
                let index = I::from_custom_bytes::<C>(key)?;
                f(index)
            })
            .await?;
        Ok(())
    }

    /// Execute a function on each index and value in the map. Indices and values are
    /// visited in a stable, yet unspecified order determined by the custom serialization.
    pub async fn for_each_index_value_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<bool, ViewError> + Send,
    {
        self.map
            .for_each_key_value_while(|key, bytes| {
                let index = I::from_custom_bytes::<C>(key)?;
                let value = C::deserialize_value(bytes)?;
                f(index, value)
            })
            .await?;
        Ok(())
    }

    /// Execute a function on each index and value in the map. Indices and values are
    /// visited in a stable, yet unspecified order determined by the custom serialization.
    pub async fn for_each_index_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<(), ViewError> + Send,
    {
        self.map
            .for_each_key_value(|key, bytes| {
                let index = I::from_custom_bytes::<C>(key)?;
                let value = C::deserialize_value(bytes)?;
                f(index, value)
            })
            .await?;
        Ok(())
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context,
    ViewError: From<C::Error>,
    I: CustomSerialize,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtain a mutable reference to a value at a given position.
    /// Default value if the index is missing.
    pub async fn get_mut_or_default<Q>(&mut self, index: &Q) -> Result<&mut V, ViewError>
    where
        I: Borrow<Q>,
        Q: Sync + Send + Serialize + ?Sized + CustomSerialize,
    {
        let short_key = index.to_custom_bytes::<C>()?;
        self.map.get_mut_or_default(short_key).await
    }
}

#[async_trait]
impl<C, I, V> HashableView<C> for CustomMapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
    V: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash().await
    }
}

#[cfg(test)]
pub mod tests {
    use std::borrow::Borrow;

    fn check_str<T: Borrow<str>>(s: T) {
        let mut ser1 = Vec::new();
        bcs::serialize_into(&mut ser1, "Hello").unwrap();
        let mut ser2 = Vec::new();
        bcs::serialize_into(&mut ser2, s.borrow()).unwrap();
        assert_eq!(ser1, ser2);
    }

    fn check_array_u8<T: Borrow<[u8]>>(v: T) {
        let mut ser1 = Vec::new();
        bcs::serialize_into(&mut ser1, &vec![23_u8, 67_u8, 123_u8]).unwrap();
        let mut ser2 = Vec::new();
        bcs::serialize_into(&mut ser2, &v.borrow()).unwrap();
        assert_eq!(ser1, ser2);
    }

    #[test]
    fn test_serialization_borrow() {
        let s = "Hello".to_string();
        check_str(s);
        let s = "Hello";
        check_str(s);
        //
        let v = vec![23, 67, 123];
        check_array_u8(v);
        let v = [23, 67, 123];
        check_array_u8(v);
    }
}
