// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    common::{
        get_interval, get_upper_bound, Batch, Context, HashOutput, KeyIterable, KeyValueIterable,
        Update, WriteOperation,
    },
    views::{HashableView, Hasher, View, ViewError},
};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    mem,
    ops::Bound::Included,
};

#[cfg(any(test, feature = "test"))]
use {
    crate::common::{ContextFromDb, KeyValueOperations},
    crate::memory::{MemoryContext, MemoryStoreMap},
    std::sync::Arc,
    tokio::sync::{OwnedMutexGuard, RwLock},
};

/// We actually implement two types:
/// 1) The first type KeyValueStoreView that implements View and the function of KeyValueOperations
/// (though not KeyValueOperations).
///
/// 2) The second type ViewContainer encapsulates KeyValueStoreView and provides the following functionalities:
/// * The Clone trait
/// * a write_batch that takes a &self instead of a "&mut self"
/// * a write_batch that writes in the context instead of writing of the view.
/// At the present time, that second type is only used for tests.

/// Key tags to create the sub-keys of a KeyValueStoreView on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the indices of the mapview
    Index = 0,
    /// Prefix for the hash
    Hash = 1,
}

/// A view that represents the function of KeyValueOperations (though not KeyValueOperations)
///
/// Comment on the data set:
/// In order to work, the view needs to store the updates and deleted_prefixes.
/// The updates and deleted_prefixes have to be coherent. This means:
/// * If an index is deleted by one in deleted_prefixes then it should not be present
///   in updates at al.
/// * deleted prefix in deleteprefix should not dominate anyone. That is if
///   we have [0,2] then we should not have [0,2,3] since it would be dominated
///   by the preceding.
///
/// With that we have:
/// * in order to test if an index is deleted by a prefix we compute the highest deleteprefix dp
///   such that dp <= index.
///   If dp is indeed a prefix then we conclude from that.index is deleted, otherwise not.
///   The no domination is essential here.
#[derive(Debug)]
pub struct KeyValueStoreView<C> {
    context: C,
    was_cleared: bool,
    updates: BTreeMap<Vec<u8>, Update<Vec<u8>>>,
    deleted_prefixes: BTreeSet<Vec<u8>>,
    stored_hash: Option<HashOutput>,
    hash: Option<HashOutput>,
}

#[async_trait]
impl<C> View<C> for KeyValueStoreView<C>
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
            deleted_prefixes: BTreeSet::new(),
            stored_hash: hash,
            hash,
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.updates.clear();
        self.deleted_prefixes.clear();
        self.hash = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {
        if self.was_cleared {
            self.was_cleared = false;
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(&mut self.updates) {
                if let Update::Set(value) = update {
                    let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                    batch.put_key_value_bytes(key, value);
                }
            }
        } else {
            for index in mem::take(&mut self.deleted_prefixes) {
                let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                batch.delete_key_prefix(key);
            }
            for (index, update) in mem::take(&mut self.updates) {
                let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                match update {
                    Update::Removed => batch.delete_key(key),
                    Update::Set(value) => batch.put_key_value_bytes(key, value),
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
        self.updates.clear();
        self.deleted_prefixes.clear();
        self.hash = None;
    }
}

impl<'a, C> KeyValueStoreView<C>
where
    C: Send + Context,
    ViewError: From<C::Error>,
{
    pub fn new(context: C) -> Self {
        Self {
            context,
            was_cleared: false,
            updates: BTreeMap::new(),
            deleted_prefixes: BTreeSet::new(),
            stored_hash: None,
            hash: None,
        }
    }

    fn is_index_present(lower_bound: &mut NextLowerKeyIterator<'a, Vec<u8>>, index: &[u8]) -> bool {
        let lower_bound = lower_bound.get_lower_bound(index.to_vec());
        match lower_bound {
            None => true,
            Some(key_prefix) => {
                if key_prefix.len() > index.len() {
                    return true;
                }
                index[0..key_prefix.len()].to_vec() != key_prefix.to_vec()
            }
        }
    }

    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        let key_prefix = self.context.base_tag(KeyTag::Index as u8);
        let mut updates = self.updates.iter();
        let mut update = updates.next();
        let mut lower_bound = NextLowerKeyIterator::new(&self.deleted_prefixes);
        if !self.was_cleared {
            for index in self
                .context
                .find_stripped_keys_by_prefix(&key_prefix)
                .await?
                .iterate()
            {
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
                            if Self::is_index_present(&mut lower_bound, index) {
                                f(index)?;
                            }
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

    pub async fn for_each_index_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<(), ViewError> + Send,
    {
        let key_prefix = self.context.base_tag(KeyTag::Index as u8);
        let mut updates = self.updates.iter();
        let mut update = updates.next();
        let mut lower_bound = NextLowerKeyIterator::new(&self.deleted_prefixes);
        if !self.was_cleared {
            for entry in self
                .context
                .find_stripped_key_values_by_prefix(&key_prefix)
                .await?
                .iterate()
            {
                let (index, index_val) = entry?;
                loop {
                    match update {
                        Some((key, value)) if key.as_slice() <= index => {
                            if let Update::Set(value) = value {
                                f(key, value)?;
                            }
                            update = updates.next();
                            if key == index {
                                break;
                            }
                        }
                        _ => {
                            if Self::is_index_present(&mut lower_bound, index) {
                                f(index, index_val)?;
                            }
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = update {
            if let Update::Set(value) = value {
                f(key, value)?;
            }
            update = updates.next();
        }
        Ok(())
    }

    pub async fn indices(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index.to_vec());
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    pub async fn get(&self, index: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        if let Some(update) = self.updates.get(index) {
            let value = match update {
                Update::Removed => None,
                Update::Set(value) => Some(value.clone()),
            };
            return Ok(value);
        }
        if self.was_cleared {
            return Ok(None);
        }
        let key = self.context.base_tag_index(KeyTag::Index as u8, index);
        let value = self.context.read_key_bytes(&key).await?;
        Ok(value)
    }

    /// Set or insert a value.
    pub fn insert(&mut self, index: Vec<u8>, value: Vec<u8>) {
        self.updates.insert(index, Update::Set(value));
        self.hash = None;
    }

    /// Remove a value.
    pub fn remove(&mut self, index: Vec<u8>) {
        self.hash = None;
        if self.was_cleared {
            self.updates.remove(&index);
        } else {
            self.updates.insert(index, Update::Removed);
        }
    }

    /// Delete a key_prefix
    pub fn delete_prefix(&mut self, key_prefix: Vec<u8>) {
        self.hash = None;
        let key_list: Vec<Vec<u8>> = self
            .updates
            .range(get_interval(key_prefix.clone()))
            .map(|x| x.0.to_vec())
            .collect();
        for key in key_list {
            self.updates.remove(&key);
        }
        if !self.was_cleared {
            let key_prefix_list: Vec<Vec<u8>> = self
                .deleted_prefixes
                .range(get_interval(key_prefix.clone()))
                .map(|x| x.to_vec())
                .collect();
            for key in key_prefix_list {
                self.deleted_prefixes.remove(&key);
            }
            self.deleted_prefixes.insert(key_prefix);
        }
    }

    pub async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        if let Some(update) = self.updates.get(key) {
            let value = match update {
                Update::Removed => None,
                Update::Set(value) => Some(value.clone()),
            };
            return Ok(value);
        }
        if self.was_cleared {
            return Ok(None);
        }
        let key = self.context.base_tag_index(KeyTag::Index as u8, key);
        let val = self.context.read_key_bytes(&key).await?;
        Ok(val)
    }

    pub async fn find_stripped_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, ViewError> {
        let len = key_prefix.len();
        let key_prefix_full = self.context.base_tag_index(KeyTag::Index as u8, key_prefix);
        let mut keys = Vec::new();
        let key_prefix_upper = get_upper_bound(key_prefix);
        let mut updates = self
            .updates
            .range((Included(key_prefix.to_vec()), key_prefix_upper));
        let mut update = updates.next();
        let mut lower_bound = NextLowerKeyIterator::new(&self.deleted_prefixes);
        if !self.was_cleared {
            for stripped_key in self
                .context
                .find_stripped_keys_by_prefix(&key_prefix_full)
                .await?
                .iterate()
            {
                let stripped_key = stripped_key?;
                loop {
                    match update {
                        Some((key_update, value_update)) if &key_update[len..] <= stripped_key => {
                            if let Update::Set(_) = value_update {
                                keys.push(key_update[len..].to_vec());
                            }
                            update = updates.next();
                            if key_update[len..] == stripped_key[..] {
                                break;
                            }
                        }
                        _ => {
                            let mut key = key_prefix.to_vec();
                            key.extend_from_slice(stripped_key);
                            if Self::is_index_present(&mut lower_bound, &key) {
                                keys.push(stripped_key.to_vec());
                            }
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key_update, value_update)) = update {
            if let Update::Set(_) = value_update {
                let stripped_key_update = key_update[len..].to_vec();
                keys.push(stripped_key_update);
            }
            update = updates.next();
        }
        Ok(keys)
    }

    pub async fn find_stripped_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
        let len = key_prefix.len();
        let key_prefix_full = self.context.base_tag_index(KeyTag::Index as u8, key_prefix);
        let mut key_values = Vec::new();
        let key_prefix_upper = get_upper_bound(key_prefix);
        let mut updates = self
            .updates
            .range((Included(key_prefix.to_vec()), key_prefix_upper));
        let mut update = updates.next();
        let mut lower_bound = NextLowerKeyIterator::new(&self.deleted_prefixes);
        if !self.was_cleared {
            for entry in self
                .context
                .find_stripped_key_values_by_prefix(&key_prefix_full)
                .await?
                .iterate()
            {
                let (stripped_key, value) = entry?;
                loop {
                    match update {
                        Some((key_update, value_update)) if &key_update[len..] <= stripped_key => {
                            if let Update::Set(value_update) = value_update {
                                let key_value = (key_update[len..].to_vec(), value_update.to_vec());
                                key_values.push(key_value);
                            }
                            update = updates.next();
                            if key_update[len..] == stripped_key[..] {
                                break;
                            }
                        }
                        _ => {
                            let mut key = key_prefix.to_vec();
                            key.extend_from_slice(stripped_key);
                            if Self::is_index_present(&mut lower_bound, &key) {
                                key_values.push((stripped_key.to_vec(), value.to_vec()));
                            }
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key_update, value_update)) = update {
            if let Update::Set(value_update) = value_update {
                let key_value = (key_update[len..].to_vec(), value_update.to_vec());
                key_values.push(key_value);
            }
            update = updates.next();
        }
        Ok(key_values)
    }

    pub async fn write_batch(&mut self, batch: Batch) -> Result<(), ViewError> {
        self.hash = None;
        for op in batch.operations {
            match op {
                WriteOperation::Delete { key } => {
                    if self.was_cleared {
                        self.updates.remove(&key);
                    } else {
                        self.updates.insert(key, Update::Removed);
                    }
                }
                WriteOperation::Put { key, value } => {
                    self.updates.insert(key, Update::Set(value));
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    self.delete_prefix(key_prefix);
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<C> HashableView<C> for KeyValueStoreView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    type Hasher = sha2::Sha512;

    async fn hash(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        match self.hash {
            Some(hash) => Ok(hash),
            None => {
                let mut hasher = Self::Hasher::default();
                let mut count = 0;
                self.for_each_index_value(|index, value| -> Result<(), ViewError> {
                    count += 1;
                    hasher.update_with_bytes(index)?;
                    hasher.update_with_bytes(value)?;
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

#[cfg(any(test, feature = "test"))]
#[derive(Debug, Clone)]
pub struct ViewContainer<C> {
    kvsv: Arc<RwLock<KeyValueStoreView<C>>>,
}

#[cfg(any(test, feature = "test"))]
#[async_trait]
impl<C> KeyValueOperations for ViewContainer<C>
where
    C: Context + Sync + Send + Clone,
    ViewError: From<C::Error>,
{
    type Error = ViewError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        let kvsv = self.kvsv.read().await;
        kvsv.read_key_bytes(key).await
    }

    async fn find_stripped_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, ViewError> {
        let kvsv = self.kvsv.read().await;
        kvsv.find_stripped_keys_by_prefix(key_prefix).await
    }

    async fn find_stripped_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ViewError> {
        let kvsv = self.kvsv.read().await;
        kvsv.find_stripped_key_values_by_prefix(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch) -> Result<(), ViewError> {
        let mut kvsv = self.kvsv.write().await;
        kvsv.write_batch(batch).await?;
        let mut batch = Batch::default();
        kvsv.flush(&mut batch)?;
        kvsv.context().write_batch(batch).await?;
        Ok(())
    }
}

/// NextLowerKeyIterator iterates over the entries of a BTreeSet.
/// The function get_lower_bound(val) returns a Some(x) where x is the highest
/// entry such that x <= val. If none exists then None is returned.
///
/// the function calls get_lower_bound have to be called with increasing
/// values.
struct NextLowerKeyIterator<'a, T: 'static> {
    prec1: Option<T>,
    prec2: Option<T>,
    iter: std::collections::btree_set::Iter<'a, T>,
}

impl<'a, T> NextLowerKeyIterator<'a, T>
where
    T: PartialOrd + Clone,
{
    pub fn new(set: &'a BTreeSet<T>) -> Self {
        let mut iter = set.iter();
        let prec1 = None;
        let prec2 = iter.next().cloned();
        Self { prec1, prec2, iter }
    }

    pub fn get_lower_bound(&mut self, val: T) -> Option<T> {
        loop {
            match &self.prec2 {
                None => {
                    return self.prec1.clone();
                }
                Some(x) => {
                    if x.clone() > val {
                        return self.prec1.clone();
                    }
                }
            }
            let prec2 = self.iter.next().cloned();
            self.prec1 = std::mem::replace(&mut self.prec2, prec2);
        }
    }
}

#[test]
fn test_lower_bound() {
    let mut set = BTreeSet::<u8>::new();
    set.insert(4);
    set.insert(7);
    set.insert(8);
    set.insert(10);
    set.insert(24);
    set.insert(40);

    let mut lower_bound = NextLowerKeyIterator::new(&set);
    assert_eq!(lower_bound.get_lower_bound(3), None);
    assert_eq!(lower_bound.get_lower_bound(15), Some(10));
    assert_eq!(lower_bound.get_lower_bound(17), Some(10));
    assert_eq!(lower_bound.get_lower_bound(25), Some(24));
    assert_eq!(lower_bound.get_lower_bound(27), Some(24));
    assert_eq!(lower_bound.get_lower_bound(42), Some(40));
}

#[cfg(any(test, feature = "test"))]
impl<C> ViewContainer<C>
where
    C: Context + Sync + Send + Clone,
    ViewError: From<C::Error>,
{
    pub fn new(context: C) -> Self {
        let kvsv = KeyValueStoreView::new(context);
        Self {
            kvsv: Arc::new(RwLock::new(kvsv)),
        }
    }
}

/// A context that stores all values in memory.
#[cfg(any(test, feature = "test"))]
pub type KeyValueStoreMemoryContext<E> = ContextFromDb<E, ViewContainer<MemoryContext<()>>>;

#[cfg(any(test, feature = "test"))]
impl<E> KeyValueStoreMemoryContext<E> {
    pub fn new(guard: OwnedMutexGuard<MemoryStoreMap>, base_key: Vec<u8>, extra: E) -> Self {
        let context = MemoryContext::new(guard, ());
        let key_value_store_view = ViewContainer::new(context);
        Self {
            db: key_value_store_view,
            base_key,
            extra,
        }
    }
}
