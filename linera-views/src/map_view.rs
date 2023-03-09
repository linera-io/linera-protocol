// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    common::{Batch, Context, HashOutput, KeyIterable, KeyValueIterable, Update},
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
    Index = 0,
    /// Prefix for the hash
    Hash = 1,
}

/// A view that supports inserting and removing values indexed by a key.
#[derive(Debug)]
pub struct MapView<C, I, V> {
    context: C,
    was_cleared: bool,
    updates: BTreeMap<Vec<u8>, Update<V>>,
    _phantom: PhantomData<I>,
    stored_hash: Option<HashOutput>,
    hash: Mutex<Option<HashOutput>>,
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
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let key = context.base_tag(KeyTag::Hash as u8);
        let hash = context.read_key(&key).await?;
        Ok(Self {
            context,
            was_cleared: false,
            updates: BTreeMap::new(),
            _phantom: PhantomData,
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
        *self.hash.get_mut() = None;
        let short_key = C::derive_short_key(index)?;
        self.updates.insert(short_key, Update::Set(value));
        Ok(())
    }

    /// Remove a value.
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        *self.hash.get_mut() = None;
        let short_key = C::derive_short_key(index)?;
        if self.was_cleared {
            self.updates.remove(&short_key);
        } else {
            self.updates.insert(short_key, Update::Removed);
        }
        Ok(())
    }

    /// Obtain the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
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
    pub async fn get_mut<Q>(&mut self, index: &Q) -> Result<Option<&mut V>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
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

    /// Execute a function on each serialized index (aka key). Keys are visited
    /// in a stable, yet unspecified order.
    async fn for_each_key<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
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

    /// Execute a function on each index and value in the map. Indices and values are
    /// visited in a stable, yet unspecified order.
    pub async fn for_each_index_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<(), ViewError> + Send,
    {
        self.for_each_key_value(|key, bytes| {
            let index = C::deserialize_value(key)?;
            let value = C::deserialize_value(bytes)?;
            f(index, value)?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    /// Execute a function on each serialized index (aka key). Keys and values are visited
    /// in a stable, yet unspecified order.
    async fn for_each_key_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<(), ViewError> + Send,
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
                                f(key, &bytes)?;
                            }
                            update = updates.next();
                            if key == index {
                                break;
                            }
                        }
                        _ => {
                            f(index, bytes)?;
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = update {
            if let Update::Set(value) = value {
                let bytes = bcs::to_bytes(value)?;
                f(key, &bytes)?;
            }
            update = updates.next();
        }
        Ok(())
    }

    async fn compute_hash(&self) -> Result<<sha2::Sha512 as Hasher>::Output, ViewError> {
        let mut hasher = sha2::Sha512::default();
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
impl<C, I, V> HashableView<C> for MapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize + DeserializeOwned,
    V: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Hasher = sha2::Sha512;

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
