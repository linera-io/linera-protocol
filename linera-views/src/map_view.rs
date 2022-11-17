use crate::{
    common::{Batch, Context},
    views::{HashView, Hasher, HashingContext, View, ViewError},
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{cmp::Eq, collections::BTreeMap, fmt::Debug, mem};

/// A view that supports inserting and removing values indexed by a key.
#[derive(Debug, Clone)]
pub struct MapView<C, I, V> {
    context: C,
    was_cleared: bool,
    updates: BTreeMap<I, Option<V>>,
}

/// The context operations supporting [`MapView`].
#[async_trait]
pub trait MapOperations<I, V>: Context {
    /// Obtain the value at the given index, if any.
    async fn get(&mut self, index: &I) -> Result<Option<V>, Self::Error>;

    /// Return the list of indices in the map.
    async fn indices(&mut self) -> Result<Vec<I>, Self::Error>;

    /// Execute a function on each index.
    async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), Self::Error>
    where
        F: FnMut(I) + Send;

    /// Set a value. Crash-resistant implementations should only write to `batch`.
    fn insert(&mut self, batch: &mut Batch, index: I, value: V) -> Result<(), Self::Error>;

    /// Remove the entry at the given index. Crash-resistant implementations should only write
    /// to `batch`.
    fn remove(&mut self, batch: &mut Batch, index: I) -> Result<(), Self::Error>;

    /// Delete the map and its entries from storage. Crash-resistant implementations should only
    /// write to `batch`.
    async fn delete(&mut self, batch: &mut Batch) -> Result<(), Self::Error>;
}

#[async_trait]
impl<I, V, C: Context + Send + Sync> MapOperations<I, V> for C
where
    I: Eq + Ord + Send + Sync + Serialize + DeserializeOwned + Clone + 'static,
    V: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    async fn get(&mut self, index: &I) -> Result<Option<V>, Self::Error> {
        let key = self.derive_key(index)?;
        Ok(self.read_key(&key).await?)
    }

    fn insert(&mut self, batch: &mut Batch, index: I, value: V) -> Result<(), Self::Error> {
        let key = self.derive_key(&index)?;
        batch.put_key_value(key, &value)?;
        Ok(())
    }

    fn remove(&mut self, batch: &mut Batch, index: I) -> Result<(), Self::Error> {
        let key = self.derive_key(&index)?;
        batch.delete_key(key);
        Ok(())
    }

    async fn indices(&mut self) -> Result<Vec<I>, Self::Error> {
        let base = self.base_key();
        self.get_sub_keys(&base).await
    }

    async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), Self::Error>
    where
        F: FnMut(I) + Send,
    {
        let base = self.base_key();
        for index in self.get_sub_keys(&base).await? {
            f(index);
        }
        Ok(())
    }

    async fn delete(&mut self, batch: &mut Batch) -> Result<(), Self::Error> {
        let base = self.base_key();
        for key in self.find_keys_with_prefix(&base).await? {
            batch.delete_key(key);
        }
        Ok(())
    }
}

#[async_trait]
impl<C, I, V> View<C> for MapView<C, I, V>
where
    C: MapOperations<I, V> + Send,
    ViewError: From<C::Error>,
    I: Eq + Ord + Send + Sync + Clone,
    V: Clone + Send + Sync,
{
    fn context(&self) -> &C {
        &self.context
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Ok(Self {
            context,
            was_cleared: false,
            updates: BTreeMap::new(),
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.updates.clear();
    }

    async fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_cleared {
            self.was_cleared = false;
            self.context.delete(batch).await?;
            for (index, update) in mem::take(&mut self.updates) {
                if let Some(value) = update {
                    self.context.insert(batch, index, value)?;
                }
            }
        } else {
            for (index, update) in mem::take(&mut self.updates) {
                match update {
                    None => self.context.remove(batch, index)?,
                    Some(value) => self.context.insert(batch, index, value)?,
                }
            }
        }
        Ok(())
    }

    async fn delete(mut self, batch: &mut Batch) -> Result<(), ViewError> {
        self.context.delete(batch).await?;
        Ok(())
    }

    fn clear(&mut self) {
        self.was_cleared = true;
        self.updates.clear();
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context,
    I: Eq + Ord,
{
    /// Set or insert a value.
    pub fn insert(&mut self, index: I, value: V) {
        self.updates.insert(index, Some(value));
    }

    /// Remove a value.
    pub fn remove(&mut self, index: I) {
        if self.was_cleared {
            self.updates.remove(&index);
        } else {
            self.updates.insert(index, None);
        }
    }

    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: MapOperations<I, V>,
    ViewError: From<C::Error>,
    I: Eq + Ord + Sync + Clone + Send,
    V: Clone + Sync,
{
    /// Read the value at the given position, if any.
    pub async fn get(&mut self, index: &I) -> Result<Option<V>, ViewError> {
        if let Some(update) = self.updates.get(index) {
            return Ok(update.as_ref().cloned());
        }
        if self.was_cleared {
            return Ok(None);
        }
        Ok(self.context.get(index).await?)
    }

    /// Return the list of indices in the map.
    pub async fn indices(&mut self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::new();
        if !self.was_cleared {
            self.context
                .for_each_index(|index: I| {
                    if !self.updates.contains_key(&index) {
                        indices.push(index);
                    }
                })
                .await?;
        }
        for (index, entry) in &self.updates {
            if entry.is_some() {
                indices.push(index.clone());
            }
        }
        indices.sort();
        Ok(indices)
    }

    /// Execute a function on each index. The function f must be order independent
    pub async fn for_each_index<F>(&mut self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) + Send,
    {
        if !self.was_cleared {
            self.context
                .for_each_index(|index: I| {
                    if !self.updates.contains_key(&index) {
                        f(index);
                    }
                })
                .await?;
        }
        for (index, entry) in &self.updates {
            if entry.is_some() {
                f(index.clone());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<C, I, V> HashView<C> for MapView<C, I, V>
where
    C: HashingContext + MapOperations<I, V> + Send,
    ViewError: From<C::Error>,
    I: Eq + Ord + Clone + Send + Sync + Serialize,
    V: Clone + Send + Sync + Serialize,
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, ViewError> {
        let mut hasher = C::Hasher::default();
        let indices = self.indices().await?;
        hasher.update_with_bcs_bytes(&indices.len())?;

        for index in indices {
            let value = self
                .get(&index)
                .await?
                .expect("The value for the returned index should be present");
            hasher.update_with_bcs_bytes(&index)?;
            hasher.update_with_bcs_bytes(&value)?;
        }
        Ok(hasher.finalize())
    }
}
