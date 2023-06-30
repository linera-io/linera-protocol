// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::{Batch, WriteOperation},
    common::{
        get_interval, get_upper_bound, Context, HasherOutput, KeyIterable, KeyValueIterable,
        Update, MIN_VIEW_TAG,
    },
    views::{HashableView, Hasher, View, ViewError},
};
use async_lock::Mutex;
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    mem,
    ops::Bound::Included,
};

#[cfg(any(test, feature = "test"))]
use {
    crate::common::{ContextFromDb, KeyValueStoreClient},
    crate::memory::{MemoryContext, MemoryStoreMap},
    async_lock::{MutexGuardArc, RwLock},
    std::sync::Arc,
};

/// We implement two types:
/// 1) The first type KeyValueStoreView implements View and the function of KeyValueStoreClient
/// (though not KeyValueStoreClient).
///
/// 2) The second type ViewContainer encapsulates KeyValueStoreView and provides the following functionalities:
/// * The Clone trait
/// * a write_batch that takes a &self instead of a "&mut self"
/// * a write_batch that writes in the context instead of writing of the view.
/// Currently, that second type is only used for tests.

/// Key tags to create the sub-keys of a KeyValueStoreView on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the indices of the view.
    Index = MIN_VIEW_TAG,
    /// Prefix for the hash.
    Hash,
}

/// A view that represents the function of KeyValueStoreClient (though not KeyValueStoreClient).
///
/// Comment on the data set:
/// In order to work, the view needs to store the updates and deleted_prefixes.
/// The updates and deleted_prefixes have to be coherent. This means:
/// * If an index is deleted by one in deleted_prefixes then it should not be present
///   in updates at al.
/// * deleted_prefix in DeletePrefix should not dominate anyone. That is if
///   we have `[0,2]` then we should not have `[0,2,3]` since it would be dominated
///   by the preceding.
///
/// With that we have:
/// * in order to test if an index is deleted by a prefix we compute the highest deleted prefix dp
///   such that dp <= index.
///   If dp is indeed a prefix then we conclude from that.index is deleted, otherwise not.
///   The no domination is essential here.
#[derive(Debug)]
pub struct KeyValueStoreView<C> {
    context: C,
    was_cleared: bool,
    updates: BTreeMap<Vec<u8>, Update<Vec<u8>>>,
    deleted_prefixes: BTreeSet<Vec<u8>>,
    stored_hash: Option<HasherOutput>,
    hash: Mutex<Option<HasherOutput>>,
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
            hash: Mutex::new(hash),
        })
    }

    fn rollback(&mut self) {
        self.was_cleared = false;
        self.updates.clear();
        self.deleted_prefixes.clear();
        *self.hash.get_mut() = self.stored_hash;
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
        self.deleted_prefixes.clear();
        *self.hash.get_mut() = None;
    }
}

impl<'a, C> KeyValueStoreView<C>
where
    C: Send + Context,
    ViewError: From<C::Error>,
{
    /// Applies the function f over all indices. If the function f returns
    /// false, then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![0]);
    ///   view.insert(vec![0,2], vec![0]);
    ///   view.insert(vec![0,3], vec![0]);
    ///   let mut count = 0;
    ///   view.for_each_index_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 2)
    ///   }).await.unwrap();
    ///   assert_eq!(count, 2);
    /// # })
    /// ```
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let key_prefix = self.context.base_tag(KeyTag::Index as u8);
        let mut updates = self.updates.iter();
        let mut update = updates.next();
        let mut lower_bound = NextLowerKeyIterator::new(&self.deleted_prefixes);
        if !self.was_cleared {
            for index in self
                .context
                .find_keys_by_prefix(&key_prefix)
                .await?
                .iterator()
            {
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
                            if lower_bound.is_index_present(index) && !f(index)? {
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

    /// Applies the function f over all indices.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![0]);
    ///   view.insert(vec![0,2], vec![0]);
    ///   view.insert(vec![0,3], vec![0]);
    ///   let mut count = 0;
    ///   view.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(count, 3);
    /// # })
    /// ```
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_index_while(|key| {
            f(key)?;
            Ok(true)
        })
        .await
    }

    /// Applies the function f over all index/value pairs.
    /// If the function f returns false then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![0]);
    ///   view.insert(vec![0,2], vec![0]);
    ///   let mut values = Vec::new();
    ///   view.for_each_index_value_while(|_key, value| {
    ///     values.push(value.to_vec());
    ///     Ok(values.len() < 1)
    ///   }).await.unwrap();
    ///   assert_eq!(values, vec![vec![0]]);
    /// # })
    /// ```
    pub async fn for_each_index_value_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool, ViewError> + Send,
    {
        let key_prefix = self.context.base_tag(KeyTag::Index as u8);
        let mut updates = self.updates.iter();
        let mut update = updates.next();
        let mut lower_bound = NextLowerKeyIterator::new(&self.deleted_prefixes);
        if !self.was_cleared {
            for entry in self
                .context
                .find_key_values_by_prefix(&key_prefix)
                .await?
                .iterator()
            {
                let (index, index_val) = entry?;
                loop {
                    match update {
                        Some((key, value)) if key.as_slice() <= index => {
                            if let Update::Set(value) = value {
                                if !f(key, value)? {
                                    return Ok(());
                                }
                            }
                            update = updates.next();
                            if key == index {
                                break;
                            }
                        }
                        _ => {
                            if lower_bound.is_index_present(index) && !f(index, index_val)? {
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
                if !f(key, value)? {
                    return Ok(());
                }
            }
            update = updates.next();
        }
        Ok(())
    }

    /// Applies the function f over all index/value pairs.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![0]);
    ///   view.insert(vec![0,2], vec![0]);
    ///   let mut part_keys = Vec::new();
    ///   view.for_each_index_while(|key| {
    ///     part_keys.push(key.to_vec());
    ///     Ok(part_keys.len() < 1)
    ///   }).await.unwrap();
    ///   assert_eq!(part_keys, vec![vec![0,1]]);
    /// # })
    /// ```
    pub async fn for_each_index_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_index_value_while(|key, value| {
            f(key, value)?;
            Ok(true)
        })
        .await
    }

    /// Returns the list of indices in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![0]);
    ///   view.insert(vec![0,2], vec![0]);
    ///   let indices = view.indices().await.unwrap();
    ///   assert_eq!(indices, vec![vec![0,1],vec![0,2]]);
    /// # })
    /// ```
    pub async fn indices(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index.to_vec());
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Obtains the value at the given index, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![42]);
    ///   assert_eq!(view.get(&[0,1]).await.unwrap(), Some(vec![42]));
    ///   assert_eq!(view.get(&[0,2]).await.unwrap(), None);
    /// # })
    /// ```
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

    /// Obtains the values of a range of indices
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![42]);
    ///   assert_eq!(view.multi_get(vec![vec![0,1], vec![0,2]]).await.unwrap(), vec![Some(vec![42]), None]);
    /// # })
    /// ```
    pub async fn multi_get(
        &self,
        indices: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ViewError> {
        let mut result = Vec::with_capacity(indices.len());
        let mut missed_indices = Vec::new();
        let mut vector_query = Vec::new();
        for (i, index) in indices.into_iter().enumerate() {
            if let Some(update) = self.updates.get(&index) {
                let value = match update {
                    Update::Removed => None,
                    Update::Set(value) => Some(value.clone()),
                };
                result.push(value);
            } else {
                result.push(None);
                missed_indices.push(i);
                let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                vector_query.push(key);
            }
        }
        if !self.was_cleared {
            let values = self.context.read_multi_key_bytes(vector_query).await?;
            for (i, value) in missed_indices.into_iter().zip(values) {
                result[i] = value;
            }
        }
        Ok(result)
    }

    /// Sets or inserts a value.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![34]);
    ///   assert_eq!(view.get(&[0,1]).await.unwrap(), Some(vec![34]));
    /// # })
    /// ```
    pub fn insert(&mut self, index: Vec<u8>, value: Vec<u8>) {
        self.updates.insert(index, Update::Set(value));
        *self.hash.get_mut() = None;
    }

    /// Removes a value. If absent then the action has no effect.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![34]);
    ///   view.remove(vec![0,1]);
    ///   assert_eq!(view.get(&[0,1]).await.unwrap(), None);
    /// # })
    /// ```
    pub fn remove(&mut self, index: Vec<u8>) {
        *self.hash.get_mut() = None;
        if self.was_cleared {
            self.updates.remove(&index);
        } else {
            self.updates.insert(index, Update::Removed);
        }
    }

    /// Deletes a key_prefix.
    fn delete_prefix(&mut self, key_prefix: Vec<u8>) {
        *self.hash.get_mut() = None;
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

    /// Iterates over all the keys matching the given prefix. The prefix is not included in the returned keys.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![34]);
    ///   view.insert(vec![3,4], vec![42]);
    ///   let keys = view.find_keys_by_prefix(&[0]).await.unwrap();
    ///   assert_eq!(keys, vec![vec![1]]);
    /// # })
    /// ```
    pub async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, ViewError> {
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
            for key in self
                .context
                .find_keys_by_prefix(&key_prefix_full)
                .await?
                .iterator()
            {
                let key = key?;
                loop {
                    match update {
                        Some((update_key, update_value)) if &update_key[len..] <= key => {
                            if let Update::Set(_) = update_value {
                                keys.push(update_key[len..].to_vec());
                            }
                            update = updates.next();
                            if update_key[len..] == key[..] {
                                break;
                            }
                        }
                        _ => {
                            let mut key_with_prefix = key_prefix.to_vec();
                            key_with_prefix.extend_from_slice(key);
                            if lower_bound.is_index_present(&key_with_prefix) {
                                keys.push(key.to_vec());
                            }
                            break;
                        }
                    }
                }
            }
        }
        while let Some((update_key, update_value)) = update {
            if let Update::Set(_) = update_value {
                let update_key = update_key[len..].to_vec();
                keys.push(update_key);
            }
            update = updates.next();
        }
        Ok(keys)
    }

    /// Iterates over all the key-value pairs, for keys matching the given prefix. The
    /// prefix is not included in the returned keys.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![34]);
    ///   view.insert(vec![3,4], vec![42]);
    ///   let key_values = view.find_key_values_by_prefix(&[0]).await.unwrap();
    ///   assert_eq!(key_values, vec![(vec![1],vec![34])]);
    /// # })
    /// ```
    pub async fn find_key_values_by_prefix(
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
                .find_key_values_by_prefix(&key_prefix_full)
                .await?
                .into_iterator_owned()
            {
                let (key, value) = entry?;
                loop {
                    match update {
                        Some((update_key, update_value)) if update_key[len..] <= key[..] => {
                            if let Update::Set(update_value) = update_value {
                                let key_value = (update_key[len..].to_vec(), update_value.to_vec());
                                key_values.push(key_value);
                            }
                            update = updates.next();
                            if update_key[len..] == key[..] {
                                break;
                            }
                        }
                        _ => {
                            let mut key_with_prefix = key_prefix.to_vec();
                            key_with_prefix.extend_from_slice(&key);
                            if lower_bound.is_index_present(&key_with_prefix) {
                                key_values.push((key, value));
                            }
                            break;
                        }
                    }
                }
            }
        }
        while let Some((update_key, update_value)) = update {
            if let Update::Set(update_value) = update_value {
                let key_value = (update_key[len..].to_vec(), update_value.to_vec());
                key_values.push(key_value);
            }
            update = updates.next();
        }
        Ok(key_values)
    }

    /// Applies the given batch of `crate::common::WriteOperation`.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_test_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::batch::Batch;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![34]);
    ///   view.insert(vec![3,4], vec![42]);
    ///   let mut batch = Batch::new();
    ///   batch.delete_key_prefix(vec![0]);
    ///   view.write_batch(batch).await.unwrap();
    ///   let key_values = view.find_key_values_by_prefix(&[0]).await.unwrap();
    ///   assert_eq!(key_values, vec![]);
    /// # })
    /// ```
    pub async fn write_batch(&mut self, batch: Batch) -> Result<(), ViewError> {
        *self.hash.get_mut() = None;
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

    async fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        let mut hasher = sha3::Sha3_256::default();
        let mut count = 0;
        self.for_each_index_value(|index, value| -> Result<(), ViewError> {
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

#[async_trait]
impl<C> HashableView<C> for KeyValueStoreView<C>
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

/// A virtual DB client using a `KeyValueStoreView` as a backend (testing only).
#[cfg(any(test, feature = "test"))]
#[derive(Debug, Clone)]
pub struct ViewContainer<C> {
    view: Arc<RwLock<KeyValueStoreView<C>>>,
}

#[cfg(any(test, feature = "test"))]
#[async_trait]
impl<C> KeyValueStoreClient for ViewContainer<C>
where
    C: Context + Sync + Send + Clone,
    ViewError: From<C::Error>,
{
    const MAX_CONNECTIONS: usize = C::MAX_CONNECTIONS;
    type Error = ViewError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        let view = self.view.read().await;
        view.get(key).await
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ViewError> {
        let view = self.view.read().await;
        view.multi_get(keys).await
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, ViewError> {
        let view = self.view.read().await;
        view.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ViewError> {
        let view = self.view.read().await;
        view.find_key_values_by_prefix(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), ViewError> {
        let mut view = self.view.write().await;
        view.write_batch(batch).await?;
        let mut batch = Batch::new();
        view.flush(&mut batch)?;
        view.context().write_batch(batch).await?;
        Ok(())
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// NextLowerKeyIterator iterates over the entries of a BTreeSet.
/// The function get_lower_bound(val) returns a Some(x) where x is the highest
/// entry such that x <= val. If none exists then None is returned.
///
/// The function calls get_lower_bound have to be called with increasing
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
    fn new(set: &'a BTreeSet<T>) -> Self {
        let mut iter = set.iter();
        let prec1 = None;
        let prec2 = iter.next().cloned();
        Self { prec1, prec2, iter }
    }

    fn get_lower_bound(&mut self, val: T) -> Option<T> {
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

impl<'a> NextLowerKeyIterator<'a, Vec<u8>> {
    fn is_index_present(&mut self, index: &[u8]) -> bool {
        let lower_bound = self.get_lower_bound(index.to_vec());
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
    /// Creates a [`ViewContainer`].
    pub async fn new(context: C) -> Result<Self, ViewError> {
        let view = KeyValueStoreView::load(context).await?;
        Ok(Self {
            view: Arc::new(RwLock::new(view)),
        })
    }
}

/// A context that stores all values in memory.
#[cfg(any(test, feature = "test"))]
pub type KeyValueStoreMemoryContext<E> = ContextFromDb<E, ViewContainer<MemoryContext<()>>>;

#[cfg(any(test, feature = "test"))]
impl<E> KeyValueStoreMemoryContext<E> {
    /// Creates a [`KeyValueStoreMemoryContext`].
    pub async fn new(
        guard: MutexGuardArc<MemoryStoreMap>,
        base_key: Vec<u8>,
        extra: E,
    ) -> Result<Self, ViewError> {
        let context = MemoryContext::new(guard, ());
        let key_value_store_view = ViewContainer::new(context).await?;
        Ok(Self {
            db: key_value_store_view,
            base_key,
            extra,
        })
    }
}
