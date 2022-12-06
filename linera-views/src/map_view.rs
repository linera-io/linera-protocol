use crate::{
    common::{Batch, Context},
    views::{HashView, Hasher, HashingContext, View, ViewError},
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::BTreeMap, fmt::Debug, marker::PhantomData, mem};

/// A view that supports inserting and removing values indexed by a key.
#[derive(Debug, Clone)]
pub struct MapView<C, I, V> {
    context: C,
    was_cleared: bool,
    updates: BTreeMap<Vec<u8>, Option<V>>,
    unit_type: PhantomData<I>,
}

#[async_trait]
impl<C, I, V> View<C> for MapView<C, I, V>
where
    C: Context + Send,
    ViewError: From<C::Error>,
    I: Send + Sync + Clone + Serialize,
    V: Clone + Send + Sync + Serialize,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Ok(Self {
            context,
            was_cleared: false,
            updates: BTreeMap::new(),
            unit_type: PhantomData,
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.updates.clear();
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_cleared {
            self.was_cleared = false;
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(&mut self.updates) {
                if let Some(value) = update {
                    batch.put_key_value(self.context.derive_key_bytes(&index), &value)?;
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                let key = self.context.derive_key_bytes(&index);
                match update {
                    None => batch.delete_key(key),
                    Some(value) => batch.put_key_value(key, &value)?,
                }
            }
        }
        Ok(())
    }

    fn delete(self, batch: &mut Batch) {
        batch.delete_key_prefix(self.context.base_key());
    }

    fn clear(&mut self) {
        self.was_cleared = true;
        self.updates.clear();
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
        let short_key = self.context.derive_short_key(index)?;
        self.updates.insert(short_key, Some(value));
        Ok(())
    }

    /// Remove a value.
    pub fn remove(&mut self, index: &I) -> Result<(), ViewError> {
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
    pub async fn get(&mut self, index: &I) -> Result<Option<V>, ViewError> {
        let short_key = self.context.derive_short_key(index)?;
        if let Some(update) = self.updates.get(&short_key) {
            return Ok(update.as_ref().cloned());
        }
        if self.was_cleared {
            return Ok(None);
        }
        let key = self.context.derive_key(index)?;
        Ok(self.context.read_key(&key).await?)
    }

    /// Return the list of indices in the map.
    pub async fn indices(&mut self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::<I>::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Execute a function on each index. The order is in which values are passed is not
    /// the one of the index but its serialization. However said order will always be the
    /// same
    pub async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        let mut iter = self.updates.iter();
        let mut pair = iter.next();
        if !self.was_cleared {
            let base = self.context.base_key();
            for index in self.context.find_keys_without_prefix(&base).await? {
                let index_i = C::deserialize_value(&index)?;
                loop {
                    match pair {
                        Some((key, value)) => {
                            let key = key.clone();
                            let key_i = C::deserialize_value(&key)?;
                            if key < index {
                                if value.is_some() {
                                    f(key_i)?;
                                }
                                pair = iter.next();
                            } else {
                                if key != index {
                                    f(index_i)?;
                                } else if value.is_some() {
                                    f(key_i)?;
                                    pair = iter.next();
                                }
                                break;
                            }
                        }
                        None => {
                            f(index_i)?;
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = pair {
            let key_i = C::deserialize_value(key)?;
            if value.is_some() {
                f(key_i)?;
            }
            pair = iter.next();
        }
        Ok(())
    }

    /// Execute a function on each index. The order is in which values are passed is not
    /// the one of the index but its serialization. However said order will always be the
    /// same
    pub async fn for_each_index_value<F>(&mut self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<(), ViewError> + Send,
    {
        let mut iter = self.updates.iter();
        let mut pair = iter.next();
        if !self.was_cleared {
            let base = self.context.base_key();
            for (index, index_val) in self.context.find_key_values_without_prefix(&base).await? {
                let index_i = C::deserialize_value(&index)?;
                let index_val = C::deserialize_value(&index_val)?;
                loop {
                    match pair {
                        Some((key, value)) => {
                            let key = key.clone();
                            let key_i = C::deserialize_value(&key)?;
                            if key < index {
                                if let Some(value) = value {
                                    f(key_i, value.clone())?;
                                }
                                pair = iter.next();
                            } else {
                                if key != index {
                                    f(index_i, index_val)?;
                                } else if let Some(value) = value {
                                    f(key_i, value.clone())?;
                                    pair = iter.next();
                                }
                                break;
                            }
                        }
                        None => {
                            f(index_i, index_val)?;
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = pair {
            let key_i = C::deserialize_value(key)?;
            if let Some(value) = value {
                f(key_i, value.clone())?;
            }
            pair = iter.next();
        }
        Ok(())
    }
}

#[async_trait]
impl<C, I, V> HashView<C> for MapView<C, I, V>
where
    C: HashingContext + Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Clone + Send + Sync + Serialize + DeserializeOwned,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, ViewError> {
        let mut hasher = C::Hasher::default();
        let mut count = 0;
        self.for_each_index_value(|index: I, value: V| {
            count += 1;
            hasher.update_with_bcs_bytes(&index)?;
            hasher.update_with_bcs_bytes(&value)?;
            Ok(())
        })
        .await?;
        hasher.update_with_bcs_bytes(&count)?;
        Ok(hasher.finalize())
    }
}
