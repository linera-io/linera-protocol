// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::{Batch, WriteOperation},
    common::{
        contains_key, get_interval, get_upper_bound, insert_key_prefix, Context, HasherOutput,
        KeyIterable, KeyValueIterable, SuffixClosedSetIterator, Update, MIN_VIEW_TAG,
    },
    map_view::ByteMapView,
    views::{ClonableView, HashableView, Hasher, View, ViewError},
};
use async_lock::Mutex;
use async_trait::async_trait;
use linera_base::{data_types::ArithmeticError, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    mem,
    ops::Bound::Included,
};

#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{self, MeasureLatency},
    linera_base::sync::Lazy,
    prometheus::HistogramVec,
};

#[cfg(with_metrics)]
/// The runtime of hash computation
static KEY_VALUE_STORE_VIEW_HASH_RUNTIME: Lazy<HistogramVec> = Lazy::new(|| {
    prometheus_util::register_histogram_vec(
        "key_value_store_view_hash_runtime",
        "KeyValueStoreView hash runtime",
        &[],
        Some(vec![
            0.001, 0.003, 0.01, 0.03, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 2.0, 5.0,
        ]),
    )
    .expect("Histogram can be created")
});

#[cfg(any(test, feature = "test"))]
use {
    crate::common::{
        ContextFromStore, KeyValueStore, ReadableKeyValueStore, WritableKeyValueStore,
    },
    crate::memory::{MemoryContext, TEST_MEMORY_MAX_STREAM_QUERIES},
    async_lock::RwLock,
    std::sync::Arc,
};

/// We implement two types:
/// 1) The first type KeyValueStoreView implements View and the function of KeyValueStore
/// (though not KeyValueStore).
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
    /// The total stored size
    TotalSize,
    /// The prefix where the sizes are being stored
    Sizes,
    /// Prefix for the hash.
    Hash,
}

/// A pair containing the key and value size.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SizeData {
    /// The size of the key
    pub key: u32,
    /// The size of the value
    pub value: u32,
}

impl SizeData {
    /// Sums both terms
    pub fn sum(&mut self) -> u32 {
        self.key + self.value
    }

    /// Sums both terms
    pub fn sum_i32(&mut self) -> Result<i32, ViewError> {
        let sum = self
            .key
            .checked_add(self.value)
            .ok_or(ArithmeticError::Overflow)?;
        Ok(i32::try_from(sum).map_err(|_| ArithmeticError::Overflow)?)
    }

    /// Add a size to the existing SizeData
    pub fn add_assign(&mut self, size: SizeData) -> Result<(), ViewError> {
        self.key = self
            .key
            .checked_add(size.key)
            .ok_or(ViewError::ArithmeticError(ArithmeticError::Overflow))?;
        self.value = self
            .value
            .checked_add(size.value)
            .ok_or(ViewError::ArithmeticError(ArithmeticError::Overflow))?;
        Ok(())
    }

    /// Subtract a size to the existing SizeData
    pub fn sub_assign(&mut self, size: SizeData) {
        self.key -= size.key;
        self.value -= size.value;
    }
}

/// A view that represents the functions of KeyValueStore (though not KeyValueStore).
///
/// Comment on the data set:
/// In order to work, the view needs to store the updates and deleted_prefixes.
/// The updates and deleted_prefixes have to be coherent. This means:
/// * If an index is deleted by one in deleted_prefixes then it should not be present
///   in updates at al.
/// * [`DeletePrefix::key_prefix`][entry1] should not dominate anyone. That is if we have `[0,2]`
///   then we should not have `[0,2,3]` since it would be dominated by the preceding.
///
/// With that we have:
/// * in order to test if an index is deleted by a prefix we compute the highest deleted prefix `prefix`
///   such that prefix <= index.
///   If dp is indeed a prefix then we conclude from that.index is deleted, otherwise not.
///   The no domination is essential here.
///
/// [entry1]: crate::batch::WriteOperation::DeletePrefix
#[derive(Debug)]
pub struct KeyValueStoreView<C> {
    context: C,
    delete_storage_first: bool,
    updates: BTreeMap<Vec<u8>, Update<Vec<u8>>>,
    stored_total_size: SizeData,
    total_size: SizeData,
    sizes: ByteMapView<C, u32>,
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
        let hash = context.read_value(&key).await?;
        let key = context.base_tag(KeyTag::TotalSize as u8);
        let total_size = context.read_value(&key).await?.unwrap_or_default();
        let base_key = context.base_tag(KeyTag::Sizes as u8);
        let context_sizes = context.clone_with_base_key(base_key);
        let sizes = ByteMapView::load(context_sizes).await?;
        Ok(Self {
            context,
            delete_storage_first: false,
            updates: BTreeMap::new(),
            stored_total_size: total_size,
            total_size,
            sizes,
            deleted_prefixes: BTreeSet::new(),
            stored_hash: hash,
            hash: Mutex::new(hash),
        })
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.updates.clear();
        self.deleted_prefixes.clear();
        self.total_size = self.stored_total_size;
        self.sizes.rollback();
        *self.hash.get_mut() = self.stored_hash;
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            delete_view = true;
            self.stored_total_size = SizeData::default();
            batch.delete_key_prefix(self.context.base_key());
            for (index, update) in mem::take(&mut self.updates) {
                if let Update::Set(value) = update {
                    let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                    batch.put_key_value_bytes(key, value);
                    delete_view = false;
                }
            }
            self.stored_hash = None
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
        self.sizes.flush(batch)?;
        let hash = *self.hash.get_mut();
        if self.stored_hash != hash {
            let key = self.context.base_tag(KeyTag::Hash as u8);
            match hash {
                None => batch.delete_key(key),
                Some(hash) => batch.put_key_value(key, &hash)?,
            }
            self.stored_hash = hash;
        }
        if self.stored_total_size != self.total_size {
            let key = self.context.base_tag(KeyTag::TotalSize as u8);
            batch.put_key_value(key, &self.total_size)?;
            self.stored_total_size = self.total_size;
        }
        self.delete_storage_first = false;
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.updates.clear();
        self.deleted_prefixes.clear();
        self.total_size = SizeData::default();
        self.sizes.clear();
        *self.hash.get_mut() = None;
    }
}

impl<C> ClonableView<C> for KeyValueStoreView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(KeyValueStoreView {
            context: self.context.clone(),
            delete_storage_first: self.delete_storage_first,
            updates: self.updates.clone(),
            stored_total_size: self.stored_total_size,
            total_size: self.total_size,
            sizes: self.sizes.clone_unchecked()?,
            deleted_prefixes: self.deleted_prefixes.clone(),
            stored_hash: self.stored_hash,
            hash: Mutex::new(*self.hash.get_mut()),
        })
    }
}

impl<'a, C> KeyValueStoreView<C>
where
    C: Send + Context + Sync,
    ViewError: From<C::Error>,
{
    fn max_key_size(&self) -> usize {
        let prefix_len = self.context.base_key().len();
        C::MAX_KEY_SIZE - 1 - prefix_len
    }

    /// Getting the total sizes that will be used for keys and values when stored
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::{KeyValueStoreView, SizeData};
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   let total_size = view.total_size();
    ///   assert_eq!(total_size, SizeData::default());
    /// # })
    /// ```
    pub fn total_size(&self) -> SizeData {
        self.total_size
    }

    /// Applies the function f over all indices. If the function f returns
    /// false, then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![0]).await.unwrap();
    ///   view.insert(vec![0,2], vec![0]).await.unwrap();
    ///   view.insert(vec![0,3], vec![0]).await.unwrap();
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
        let mut suffix_closed_set = SuffixClosedSetIterator::new(0, self.deleted_prefixes.iter());
        if !self.delete_storage_first {
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
                            if !suffix_closed_set.find_key(index) && !f(index)? {
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![0]).await.unwrap();
    ///   view.insert(vec![0,2], vec![0]).await.unwrap();
    ///   view.insert(vec![0,3], vec![0]).await.unwrap();
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![0]).await.unwrap();
    ///   view.insert(vec![0,2], vec![0]).await.unwrap();
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
        let mut suffix_closed_set = SuffixClosedSetIterator::new(0, self.deleted_prefixes.iter());
        if !self.delete_storage_first {
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
                            if !suffix_closed_set.find_key(index) && !f(index, index_val)? {
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![0]).await.unwrap();
    ///   view.insert(vec![0,2], vec![0]).await.unwrap();
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![0]).await.unwrap();
    ///   view.insert(vec![0,2], vec![0]).await.unwrap();
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

    /// Returns the list of indices and values in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![0]).await.unwrap();
    ///   view.insert(vec![0,2], vec![0]).await.unwrap();
    ///   let key_values = view.indices().await.unwrap();
    ///   assert_eq!(key_values, vec![vec![0,1],vec![0,2]]);
    /// # })
    /// ```
    pub async fn index_values(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
        let mut index_values = Vec::new();
        self.for_each_index_value(|index, value| {
            index_values.push((index.to_vec(), value.to_vec()));
            Ok(())
        })
        .await?;
        Ok(index_values)
    }

    /// Returns the number of entries.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![0]).await.unwrap();
    ///   view.insert(vec![0,2], vec![0]).await.unwrap();
    ///   let count = view.count().await.unwrap();
    ///   assert_eq!(count, 2);
    /// # })
    /// ```
    pub async fn count(&self) -> Result<usize, ViewError> {
        let mut count = 0;
        self.for_each_index(|_index| {
            count += 1;
            Ok(())
        })
        .await?;
        Ok(count)
    }

    /// Obtains the value at the given index, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![42]).await.unwrap();
    ///   assert_eq!(view.get(&[0,1]).await.unwrap(), Some(vec![42]));
    ///   assert_eq!(view.get(&[0,2]).await.unwrap(), None);
    /// # })
    /// ```
    pub async fn get(&self, index: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        ensure!(index.len() <= self.max_key_size(), ViewError::KeyTooLong);
        if let Some(update) = self.updates.get(index) {
            let value = match update {
                Update::Removed => None,
                Update::Set(value) => Some(value.clone()),
            };
            return Ok(value);
        }
        if self.delete_storage_first {
            return Ok(None);
        }
        if contains_key(&self.deleted_prefixes, index) {
            return Ok(None);
        }
        let key = self.context.base_tag_index(KeyTag::Index as u8, index);
        Ok(self.context.read_value_bytes(&key).await?)
    }

    /// Test whether a view contains a specific index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![42]).await.unwrap();
    ///   assert!(view.contains_key(&[0,1]).await.unwrap());
    ///   assert!(!view.contains_key(&[0,2]).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key(&self, index: &[u8]) -> Result<bool, ViewError> {
        ensure!(index.len() <= self.max_key_size(), ViewError::KeyTooLong);
        if let Some(update) = self.updates.get(index) {
            let test = match update {
                Update::Removed => false,
                Update::Set(_value) => true,
            };
            return Ok(test);
        }
        if self.delete_storage_first {
            return Ok(false);
        }
        if contains_key(&self.deleted_prefixes, index) {
            return Ok(false);
        }
        let key = self.context.base_tag_index(KeyTag::Index as u8, index);
        Ok(self.context.contains_key(&key).await?)
    }

    /// Obtains the values of a range of indices
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![42]).await.unwrap();
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
            if index.len() > self.max_key_size() {
                return Err(ViewError::KeyTooLong);
            }
            if let Some(update) = self.updates.get(&index) {
                let value = match update {
                    Update::Removed => None,
                    Update::Set(value) => Some(value.clone()),
                };
                result.push(value);
            } else {
                result.push(None);
                if !contains_key(&self.deleted_prefixes, &index) {
                    missed_indices.push(i);
                    let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                    vector_query.push(key);
                }
            }
        }
        if !self.delete_storage_first {
            let values = self.context.read_multi_values_bytes(vector_query).await?;
            for (i, value) in missed_indices.into_iter().zip(values) {
                result[i] = value;
            }
        }
        Ok(result)
    }

    /// Applies the given batch of `crate::common::WriteOperation`.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::batch::Batch;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![34]).await.unwrap();
    ///   view.insert(vec![3,4], vec![42]).await.unwrap();
    ///   let mut batch = Batch::new();
    ///   batch.delete_key_prefix(vec![0]);
    ///   view.write_batch(batch).await.unwrap();
    ///   let key_values = view.find_key_values_by_prefix(&[0]).await.unwrap();
    ///   assert_eq!(key_values, vec![]);
    /// # })
    /// ```
    pub async fn write_batch(&mut self, batch: Batch) -> Result<(), ViewError> {
        *self.hash.get_mut() = None;
        let max_key_size = self.max_key_size();
        for operation in batch.operations {
            match operation {
                WriteOperation::Delete { key } => {
                    ensure!(key.len() <= max_key_size, ViewError::KeyTooLong);
                    if let Some(value) = self.sizes.get(&key).await? {
                        let entry_size = SizeData {
                            key: u32::try_from(key.len()).map_err(|_| ArithmeticError::Overflow)?,
                            value,
                        };
                        self.total_size.sub_assign(entry_size);
                    }
                    self.sizes.remove(key.clone());
                    if self.delete_storage_first {
                        // Optimization: No need to mark `short_key` for deletion as we are going to remove all the keys at once.
                        self.updates.remove(&key);
                    } else {
                        self.updates.insert(key, Update::Removed);
                    }
                }
                WriteOperation::Put { key, value } => {
                    ensure!(key.len() <= max_key_size, ViewError::KeyTooLong);
                    let entry_size = SizeData {
                        key: key.len() as u32,
                        value: value.len() as u32,
                    };
                    self.total_size.add_assign(entry_size)?;
                    if let Some(value) = self.sizes.get(&key).await? {
                        let entry_size = SizeData {
                            key: key.len() as u32,
                            value,
                        };
                        self.total_size.sub_assign(entry_size);
                    }
                    self.sizes.insert(key.clone(), entry_size.value);
                    self.updates.insert(key, Update::Set(value));
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    ensure!(key_prefix.len() <= max_key_size, ViewError::KeyTooLong);
                    let key_list = self
                        .updates
                        .range(get_interval(key_prefix.clone()))
                        .map(|x| x.0.to_vec())
                        .collect::<Vec<_>>();
                    for key in key_list {
                        self.updates.remove(&key);
                    }
                    let key_values = self.sizes.key_values_by_prefix(key_prefix.clone()).await?;
                    for (key, value) in key_values {
                        let entry_size = SizeData {
                            key: key.len() as u32,
                            value,
                        };
                        self.total_size.sub_assign(entry_size);
                        self.sizes.remove(key);
                    }
                    self.sizes.remove_by_prefix(key_prefix.clone());
                    if !self.delete_storage_first {
                        insert_key_prefix(&mut self.deleted_prefixes, key_prefix);
                    }
                }
            }
        }
        Ok(())
    }

    /// Sets or inserts a value.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![34]).await.unwrap();
    ///   assert_eq!(view.get(&[0,1]).await.unwrap(), Some(vec![34]));
    /// # })
    /// ```
    pub async fn insert(&mut self, index: Vec<u8>, value: Vec<u8>) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.put_key_value_bytes(index, value);
        self.write_batch(batch).await
    }

    /// Removes a value. If absent then the action has no effect.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![34]).await.unwrap();
    ///   view.remove(vec![0,1]).await.unwrap();
    ///   assert_eq!(view.get(&[0,1]).await.unwrap(), None);
    /// # })
    /// ```
    pub async fn remove(&mut self, index: Vec<u8>) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.delete_key(index);
        self.write_batch(batch).await
    }

    /// Deletes a key_prefix.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![34]).await.unwrap();
    ///   view.remove_by_prefix(vec![0]).await.unwrap();
    ///   assert_eq!(view.get(&[0,1]).await.unwrap(), None);
    /// # })
    /// ```
    pub async fn remove_by_prefix(&mut self, key_prefix: Vec<u8>) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.delete_key_prefix(key_prefix);
        self.write_batch(batch).await
    }

    /// Iterates over all the keys matching the given prefix. The prefix is not included in the returned keys.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![34]).await.unwrap();
    ///   view.insert(vec![3,4], vec![42]).await.unwrap();
    ///   let keys = view.find_keys_by_prefix(&[0]).await.unwrap();
    ///   assert_eq!(keys, vec![vec![1]]);
    /// # })
    /// ```
    pub async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, ViewError> {
        ensure!(
            key_prefix.len() <= self.max_key_size(),
            ViewError::KeyTooLong
        );
        let len = key_prefix.len();
        let key_prefix_full = self.context.base_tag_index(KeyTag::Index as u8, key_prefix);
        let mut keys = Vec::new();
        let key_prefix_upper = get_upper_bound(key_prefix);
        let mut updates = self
            .updates
            .range((Included(key_prefix.to_vec()), key_prefix_upper));
        let mut update = updates.next();
        let mut suffix_closed_set = SuffixClosedSetIterator::new(0, self.deleted_prefixes.iter());
        if !self.delete_storage_first {
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
                            if !suffix_closed_set.find_key(&key_with_prefix) {
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
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut view = KeyValueStoreView::load(context).await.unwrap();
    ///   view.insert(vec![0,1], vec![34]).await.unwrap();
    ///   view.insert(vec![3,4], vec![42]).await.unwrap();
    ///   let key_values = view.find_key_values_by_prefix(&[0]).await.unwrap();
    ///   assert_eq!(key_values, vec![(vec![1],vec![34])]);
    /// # })
    /// ```
    pub async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
        ensure!(
            key_prefix.len() <= self.max_key_size(),
            ViewError::KeyTooLong
        );
        let len = key_prefix.len();
        let key_prefix_full = self.context.base_tag_index(KeyTag::Index as u8, key_prefix);
        let mut key_values = Vec::new();
        let key_prefix_upper = get_upper_bound(key_prefix);
        let mut updates = self
            .updates
            .range((Included(key_prefix.to_vec()), key_prefix_upper));
        let mut update = updates.next();
        let mut suffix_closed_set = SuffixClosedSetIterator::new(0, self.deleted_prefixes.iter());
        if !self.delete_storage_first {
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
                            if !suffix_closed_set.find_key(&key_with_prefix) {
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

    async fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = KEY_VALUE_STORE_VIEW_HASH_RUNTIME.measure_latency();
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
impl<C> ReadableKeyValueStore<ViewError> for ViewContainer<C>
where
    C: Context + Sync + Send + Clone,
    ViewError: From<C::Error>,
{
    const MAX_KEY_SIZE: usize = C::MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        1
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        let view = self.view.read().await;
        view.get(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ViewError> {
        let view = self.view.read().await;
        view.contains_key(key).await
    }

    async fn read_multi_values_bytes(
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
}
#[cfg(any(test, feature = "test"))]
#[async_trait]
impl<C> WritableKeyValueStore<ViewError> for ViewContainer<C>
where
    C: Context + Sync + Send + Clone,
    ViewError: From<C::Error>,
{
    const MAX_VALUE_SIZE: usize = C::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), ViewError> {
        let mut view = self.view.write().await;
        view.write_batch(batch).await?;
        let mut batch = Batch::new();
        view.flush(&mut batch)?;
        view.context().write_batch(batch).await?;
        Ok(())
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), ViewError> {
        Ok(())
    }
}

#[cfg(any(test, feature = "test"))]
impl<C> KeyValueStore for ViewContainer<C>
where
    C: Context + Sync + Send + Clone,
    ViewError: From<C::Error>,
{
    type Error = ViewError;
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
        let view = Arc::new(RwLock::new(view));
        Ok(Self { view })
    }
}

/// A context that stores all values in memory.
#[cfg(any(test, feature = "test"))]
pub type KeyValueStoreMemoryContext<E> = ContextFromStore<E, ViewContainer<MemoryContext<()>>>;

#[cfg(any(test, feature = "test"))]
impl<E> KeyValueStoreMemoryContext<E> {
    /// Creates a [`KeyValueStoreMemoryContext`].
    pub async fn new(base_key: Vec<u8>, extra: E) -> Result<Self, ViewError> {
        let context = MemoryContext::new(TEST_MEMORY_MAX_STREAM_QUERIES, ());
        let store = ViewContainer::new(context).await?;
        Ok(Self {
            store,
            base_key,
            extra,
        })
    }
}
