use crate::{
    common::{Batch, Context, HashOutput},
    views::{HashView, Hasher, View, ViewError},
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::BTreeMap, fmt::Debug, marker::PhantomData, mem};

/// prefix used.
///
/// 0 : for the indices of the mapview
/// 1 : for the hash
enum KeyTag {
    Index = 0,
    Hash = 1,
}

/// A view that supports inserting and removing values indexed by a key.
#[derive(Debug)]
pub struct MapView<C, I, V> {
    context: C,
    was_cleared: bool,
    updates: BTreeMap<Vec<u8>, Option<V>>,
    _phantom: PhantomData<I>,
    hash: Option<HashOutput>,
}

#[async_trait]
impl<C, I, V> View<C> for MapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Clone + Serialize,
    V: Clone + Send + Sync + Serialize,
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
            hash,
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.updates.clear();
        self.hash = None;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_cleared {
            self.was_cleared = false;
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(&mut self.updates) {
                if let Some(value) = update {
                    let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                    batch.put_key_value(key, &value)?;
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                match update {
                    None => batch.delete_key(key),
                    Some(value) => batch.put_key_value(key, &value)?,
                }
            }
        }
        let key = self.context.base_tag(KeyTag::Hash as u8);
        match self.hash {
            None => batch.delete_key(key),
            Some(hash) => batch.put_key_value(key, &hash)?,
        }
        Ok(())
    }

    fn delete(self, batch: &mut Batch) {
        batch.delete_key_prefix(self.context.base_key());
    }

    fn clear(&mut self) {
        self.was_cleared = true;
        self.updates.clear();
        self.hash = None;
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Serialize,
{
    /// Set or insert a value.
    pub fn insert(&mut self, index: &I, value: V) -> Result<(), ViewError> {
        self.hash = None;
        let short_key = self.context.derive_short_key(index)?;
        self.updates.insert(short_key, Some(value));
        Ok(())
    }

    /// Remove a value.
    pub fn remove(&mut self, index: &I) -> Result<(), ViewError> {
        self.hash = None;
        let short_key = self.context.derive_short_key(index)?;
        if self.was_cleared {
            self.updates.remove(&short_key);
        } else {
            self.updates.insert(short_key, None);
        }
        Ok(())
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context,
    ViewError: From<C::Error>,
    I: Sync + Clone + Send + Serialize + DeserializeOwned,
    V: Clone + Sync + DeserializeOwned + 'static,
{
    /// Read the value at the given position, if any.
    pub async fn get(&self, index: &I) -> Result<Option<V>, ViewError> {
        let short_key = self.context.derive_short_key(index)?;
        if let Some(update) = self.updates.get(&short_key) {
            return Ok(update.as_ref().cloned());
        }
        if self.was_cleared {
            return Ok(None);
        }
        let key = self.context.derive_tag_key(KeyTag::Index as u8, &index)?;
        Ok(self.context.read_key(&key).await?)
    }

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

    /// Execute a function on each index serialization. The order is in which values
    /// are passed is not the one of the index but its serialization. However said
    /// order will always be the same
    pub async fn for_each_raw_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(Vec<u8>) -> Result<(), ViewError> + Send,
    {
        let mut iter = self.updates.iter();
        let mut pair = iter.next();
        if !self.was_cleared {
            let base = self.context.base_tag(KeyTag::Index as u8);
            for index in self.context.find_stripped_keys_by_prefix(&base).await? {
                loop {
                    match pair {
                        Some((key, value)) => {
                            let key = key.clone();
                            if key < index {
                                if value.is_some() {
                                    f(key)?;
                                }
                                pair = iter.next();
                            } else {
                                if key != index {
                                    f(index)?;
                                } else if value.is_some() {
                                    f(key)?;
                                    pair = iter.next();
                                }
                                break;
                            }
                        }
                        None => {
                            f(index)?;
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = pair {
            if value.is_some() {
                f(key.to_vec())?;
            }
            pair = iter.next();
        }
        Ok(())
    }

    /// Execute a function on each index. The order is in which values are passed is not
    /// the one of the index but its serialization. However said order will always be the
    /// same
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        self.for_each_raw_index(|index: Vec<u8>| {
            let index = C::deserialize_value(&index)?;
            f(index)?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    /// Execute a function on each index seralization. The order is in which values
    /// are passed is not the one of the index but its serialization. However said
    /// order will always be the same
    pub async fn for_each_raw_index_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(Vec<u8>, V) -> Result<(), ViewError> + Send,
    {
        let mut iter = self.updates.iter();
        let mut pair = iter.next();
        if !self.was_cleared {
            let base = self.context.base_tag(KeyTag::Index as u8);
            for (index, index_val) in self
                .context
                .find_stripped_key_values_by_prefix(&base)
                .await?
            {
                let index_val = C::deserialize_value(&index_val)?;
                loop {
                    match pair {
                        Some((key, value)) => {
                            let key = key.clone();
                            if key < index {
                                if let Some(value) = value {
                                    f(key, value.clone())?;
                                }
                                pair = iter.next();
                            } else {
                                if key != index {
                                    f(index, index_val)?;
                                } else if let Some(value) = value {
                                    f(key, value.clone())?;
                                    pair = iter.next();
                                }
                                break;
                            }
                        }
                        None => {
                            f(index, index_val)?;
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = pair {
            if let Some(value) = value {
                f(key.clone(), value.clone())?;
            }
            pair = iter.next();
        }
        Ok(())
    }
}

#[async_trait]
impl<C, I, V> HashView<C> for MapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Clone + Send + Sync + Serialize + DeserializeOwned,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Hasher = sha2::Sha512;

    async fn hash(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        match self.hash {
            Some(hash) => Ok(hash),
            None => {
                let mut hasher = Self::Hasher::default();
                let mut count = 0;
                self.for_each_raw_index_value(|index: Vec<u8>, value: V| {
                    count += 1;
                    hasher.update_with_bytes(&index)?;
                    hasher.update_with_bcs_bytes(&value)?;
                    Ok(())
                })
                .await?;
                hasher.update_with_bcs_bytes(&count)?;
                let hash = hasher.finalize();
                self.hash = Some(hash);
                Ok(hash)
            }
        }
    }
}
