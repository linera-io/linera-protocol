// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! We implement two types:
//! 1) The first type `KeyValueStoreView` implements View and the function of `KeyValueStore`.
//!
//! 2) The second type `ViewContainer` encapsulates `KeyValueStoreView` and provides the following functionalities:
//!    * The `Clone` trait
//!    * a `write_batch` that takes a `&self` instead of a `&mut self`
//!    * a `write_batch` that writes in the context instead of writing of the view.
//!
//! Currently, that second type is only used for tests.
//!
//! Key tags to create the sub-keys of a `KeyValueStoreView` on top of the base key.

use std::{collections::BTreeMap, fmt::Debug, mem, ops::Bound::Included};

#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{data_types::ArithmeticError, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    batch::{Batch, WriteOperation},
    common::{
        from_bytes_option, from_bytes_option_or_default, get_interval, get_upper_bound,
        DeletionSet, SuffixClosedSetIterator, Update,
    },
    context::Context,
    hashable_wrapper::WrappedHashableContainerView,
    map_view::ByteMapView,
    store::{KeyIterable, KeyValueIterable, ReadableKeyValueStore},
    views::{ClonableView, HashableView, Hasher, HasherOutput, View, ViewError, MIN_VIEW_TAG},
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    /// The latency of hash computation
    pub static KEY_VALUE_STORE_VIEW_HASH_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "key_value_store_view_hash_latency",
            "KeyValueStoreView hash latency",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });

    /// The latency of get operation
    pub static KEY_VALUE_STORE_VIEW_GET_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "key_value_store_view_get_latency",
            "KeyValueStoreView get latency",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });

    /// The latency of multi get
    pub static KEY_VALUE_STORE_VIEW_MULTI_GET_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "key_value_store_view_multi_get_latency",
                "KeyValueStoreView multi get latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of contains key
    pub static KEY_VALUE_STORE_VIEW_CONTAINS_KEY_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "key_value_store_view_contains_key_latency",
                "KeyValueStoreView contains key latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of contains keys
    pub static KEY_VALUE_STORE_VIEW_CONTAINS_KEYS_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "key_value_store_view_contains_keys_latency",
                "KeyValueStoreView contains keys latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of find keys by prefix operation
    pub static KEY_VALUE_STORE_VIEW_FIND_KEYS_BY_PREFIX_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "key_value_store_view_find_keys_by_prefix_latency",
                "KeyValueStoreView find keys by prefix latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of find key values by prefix operation
    pub static KEY_VALUE_STORE_VIEW_FIND_KEY_VALUES_BY_PREFIX_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "key_value_store_view_find_key_values_by_prefix_latency",
                "KeyValueStoreView find key values by prefix latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of write batch operation
    pub static KEY_VALUE_STORE_VIEW_WRITE_BATCH_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "key_value_store_view_write_batch_latency",
                "KeyValueStoreView write batch latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });
}

#[cfg(with_testing)]
use {
    crate::store::{KeyValueStoreError, WithError, WritableKeyValueStore},
    async_lock::RwLock,
    std::sync::Arc,
    thiserror::Error,
};

/// The number of keys being used for initialization
const NUM_INIT_KEYS: usize = 1;

/// We choose a threshold of 400K between big and small key.
const THRESHOLD_BIG_SMALL: usize = 409600;

#[repr(u8)]
enum KeyTag {
    /// The total stored size
    TotalSize = MIN_VIEW_TAG,
    /// Prefix for the indices of the view.
    Index,
    /// The prefix where the sizes are being stored
    Sizes,
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

    /// Adds a size to `self`
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

    /// Subtracts a size from `self`
    pub fn sub_assign(&mut self, size: SizeData) {
        self.key -= size.key;
        self.value -= size.value;
    }
}

/// A view that represents the functions of `KeyValueStore`.
///
/// Comment on the data set:
/// In order to work, the view needs to store the updates and deleted prefixes.
/// The updates and deleted prefixes have to be coherent. This means:
/// * If an index is deleted by one in deleted prefixes then it should not be present
///   in the updates at all.
/// * [`DeletePrefix::key_prefix`][entry1] should not dominate anyone. That is if we have `[0,2]`
///   then we should not have `[0,2,3]` since it would be dominated by the preceding.
///
/// With that we have:
/// * in order to test if an `index` is deleted by a prefix we compute the highest deleted prefix `dp`
///   such that `dp <= index`.
///   If `dp` is indeed a prefix then we conclude that `index` is deleted, otherwise not.
///   The no domination is essential here.
///
/// [entry1]: crate::batch::WriteOperation::DeletePrefix
#[derive(Debug)]
pub struct BigKeyValueStoreView<C> {
    context: C,
    deletion_set: DeletionSet,
    updates: BTreeMap<Vec<u8>, Update<Vec<u8>>>,
    stored_total_size: SizeData,
    total_size: SizeData,
    sizes: ByteMapView<C, u32>,
}

impl<C> View<C> for BigKeyValueStoreView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    const NUM_INIT_KEYS: usize = NUM_INIT_KEYS;

    fn context(&self) -> &C {
        &self.context
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        let key_total_size = context.base_key().base_tag(KeyTag::TotalSize as u8);
        assert_eq!(ByteMapView::<C, u32>::NUM_INIT_KEYS, 0);
        Ok(vec![key_total_size])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let total_size =
            from_bytes_option_or_default(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        let base_key = context.base_key().base_tag(KeyTag::Sizes as u8);
        let context_sizes = context.clone_with_base_key(base_key);
        let sizes = ByteMapView::post_load(context_sizes, &[])?;
        Ok(Self {
            context,
            deletion_set: DeletionSet::new(),
            updates: BTreeMap::new(),
            stored_total_size: total_size,
            total_size,
            sizes,
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let keys = Self::pre_load(&context)?;
        let values = context.store().read_multi_values_bytes(keys).await?;
        Self::post_load(context, &values)
    }

    fn rollback(&mut self) {
        self.deletion_set.rollback();
        self.updates.clear();
        self.total_size = self.stored_total_size;
        self.sizes.rollback();
    }

    async fn has_pending_changes(&self) -> bool {
        if self.deletion_set.has_pending_changes() {
            return true;
        }
        if !self.updates.is_empty() {
            return true;
        }
        if self.stored_total_size != self.total_size {
            return true;
        }
        self.sizes.has_pending_changes().await
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.deletion_set.delete_storage_first {
            delete_view = true;
            self.stored_total_size = SizeData::default();
            batch.delete_key_prefix(self.context.base_key().bytes.clone());
            for (index, update) in mem::take(&mut self.updates) {
                if let Update::Set(value) = update {
                    let key = self
                        .context
                        .base_key()
                        .base_tag_index(KeyTag::Index as u8, &index);
                    batch.put_key_value_bytes(key, value);
                    delete_view = false;
                }
            }
        } else {
            for index in mem::take(&mut self.deletion_set.deleted_prefixes) {
                let key = self
                    .context
                    .base_key()
                    .base_tag_index(KeyTag::Index as u8, &index);
                batch.delete_key_prefix(key);
            }
            for (index, update) in mem::take(&mut self.updates) {
                let key = self
                    .context
                    .base_key()
                    .base_tag_index(KeyTag::Index as u8, &index);
                match update {
                    Update::Removed => batch.delete_key(key),
                    Update::Set(value) => batch.put_key_value_bytes(key, value),
                }
            }
        }
        self.sizes.flush(batch)?;
        if self.stored_total_size != self.total_size {
            let key = self.context.base_key().base_tag(KeyTag::TotalSize as u8);
            batch.put_key_value(key, &self.total_size)?;
            self.stored_total_size = self.total_size;
        }
        self.deletion_set.delete_storage_first = false;
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.deletion_set.clear();
        self.updates.clear();
        self.total_size = SizeData::default();
        self.sizes.clear();
    }
}

impl<C> ClonableView<C> for BigKeyValueStoreView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(BigKeyValueStoreView {
            context: self.context.clone(),
            deletion_set: self.deletion_set.clone(),
            updates: self.updates.clone(),
            stored_total_size: self.stored_total_size,
            total_size: self.total_size,
            sizes: self.sizes.clone_unchecked()?,
        })
    }
}

impl<C> BigKeyValueStoreView<C>
where
    C: Send + Context + Sync,
    ViewError: From<C::Error>,
{
    fn max_key_size(&self) -> usize {
        let prefix_len = self.context.base_key().bytes.len();
        <C::Store as ReadableKeyValueStore>::MAX_KEY_SIZE - 1 - prefix_len
    }

    /// Getting the total sizes that will be used for keys and values when stored
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::{BigKeyValueStoreView, SizeData};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// let total_size = view.total_size();
    /// assert_eq!(total_size, SizeData::default());
    /// # })
    /// ```
    pub fn total_size(&self) -> SizeData {
        self.total_size
    }

    /// Applies the function f over all indices. If the function f returns
    /// false, then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// view.insert(vec![0, 3], vec![0]).await.unwrap();
    /// let mut count = 0;
    /// view.for_each_index_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 2)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// # })
    /// ```
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let key_prefix = self.context.base_key().base_tag(KeyTag::Index as u8);
        let mut updates = self.updates.iter();
        let mut update = updates.next();
        if !self.deletion_set.delete_storage_first {
            let mut suffix_closed_set =
                SuffixClosedSetIterator::new(0, self.deletion_set.deleted_prefixes.iter());
            for index in self
                .context
                .store()
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// view.insert(vec![0, 3], vec![0]).await.unwrap();
    /// let mut count = 0;
    /// view.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 3);
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// let mut values = Vec::new();
    /// view.for_each_index_value_while(|_key, value| {
    ///     values.push(value.to_vec());
    ///     Ok(values.len() < 1)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(values, vec![vec![0]]);
    /// # })
    /// ```
    pub async fn for_each_index_value_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool, ViewError> + Send,
    {
        let key_prefix = self.context.base_key().base_tag(KeyTag::Index as u8);
        let mut updates = self.updates.iter();
        let mut update = updates.next();
        if !self.deletion_set.delete_storage_first {
            let mut suffix_closed_set =
                SuffixClosedSetIterator::new(0, self.deletion_set.deleted_prefixes.iter());
            for entry in self
                .context
                .store()
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// let mut part_keys = Vec::new();
    /// view.for_each_index_while(|key| {
    ///     part_keys.push(key.to_vec());
    ///     Ok(part_keys.len() < 1)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(part_keys, vec![vec![0, 1]]);
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// let indices = view.indices().await.unwrap();
    /// assert_eq!(indices, vec![vec![0, 1], vec![0, 2]]);
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// let key_values = view.index_values().await.unwrap();
    /// assert_eq!(
    ///     key_values,
    ///     vec![(vec![0, 1], vec![0]), (vec![0, 2], vec![0])]
    /// );
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// let count = view.count().await.unwrap();
    /// assert_eq!(count, 2);
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).await.unwrap();
    /// assert_eq!(view.get(&[0, 1]).await.unwrap(), Some(vec![42]));
    /// assert_eq!(view.get(&[0, 2]).await.unwrap(), None);
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
        if self.deletion_set.contains_prefix_of(index) {
            return Ok(None);
        }
        let key = self
            .context
            .base_key()
            .base_tag_index(KeyTag::Index as u8, index);
        Ok(self.context.store().read_value_bytes(&key).await?)
    }

    /// Tests whether the store contains a specific index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).await.unwrap();
    /// assert!(view.contains_key(&[0, 1]).await.unwrap());
    /// assert!(!view.contains_key(&[0, 2]).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key(&self, index: &[u8]) -> Result<bool, ViewError> {
        if let Some(update) = self.updates.get(index) {
            let test = match update {
                Update::Removed => false,
                Update::Set(_value) => true,
            };
            return Ok(test);
        }
        if self.deletion_set.contains_prefix_of(index) {
            return Ok(false);
        }
        let key = self
            .context
            .base_key()
            .base_tag_index(KeyTag::Index as u8, index);
        Ok(self.context.store().contains_key(&key).await?)
    }

    /// Tests whether the view contains a range of indices
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).await.unwrap();
    /// let keys = vec![vec![0, 1], vec![0, 2]];
    /// let results = view.contains_keys(keys).await.unwrap();
    /// assert_eq!(results, vec![true, false]);
    /// # })
    /// ```
    pub async fn contains_keys(&self, indices: Vec<Vec<u8>>) -> Result<Vec<bool>, ViewError> {
        let mut results = Vec::with_capacity(indices.len());
        let mut missed_indices = Vec::new();
        let mut vector_query = Vec::new();
        for (i, index) in indices.into_iter().enumerate() {
            ensure!(index.len() <= self.max_key_size(), ViewError::KeyTooLong);
            if let Some(update) = self.updates.get(&index) {
                let value = match update {
                    Update::Removed => false,
                    Update::Set(_) => true,
                };
                results.push(value);
            } else {
                results.push(false);
                if !self.deletion_set.contains_prefix_of(&index) {
                    missed_indices.push(i);
                    let key = self
                        .context
                        .base_key()
                        .base_tag_index(KeyTag::Index as u8, &index);
                    vector_query.push(key);
                }
            }
        }
        let values = self.context.store().contains_keys(vector_query).await?;
        for (i, value) in missed_indices.into_iter().zip(values) {
            results[i] = value;
        }
        Ok(results)
    }

    /// Obtains the values of a range of indices
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).await.unwrap();
    /// assert_eq!(
    ///     view.multi_get(vec![vec![0, 1], vec![0, 2]]).await.unwrap(),
    ///     vec![Some(vec![42]), None]
    /// );
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
            ensure!(index.len() <= self.max_key_size(), ViewError::KeyTooLong);
            if let Some(update) = self.updates.get(&index) {
                let value = match update {
                    Update::Removed => None,
                    Update::Set(value) => Some(value.clone()),
                };
                result.push(value);
            } else {
                result.push(None);
                if !self.deletion_set.contains_prefix_of(&index) {
                    missed_indices.push(i);
                    let key = self
                        .context
                        .base_key()
                        .base_tag_index(KeyTag::Index as u8, &index);
                    vector_query.push(key);
                }
            }
        }
        let values = self
            .context
            .store()
            .read_multi_values_bytes(vector_query)
            .await?;
        for (i, value) in missed_indices.into_iter().zip(values) {
            result[i] = value;
        }
        Ok(result)
    }

    /// Applies the given batch of `crate::common::WriteOperation`.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::batch::Batch;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).await.unwrap();
    /// view.insert(vec![3, 4], vec![42]).await.unwrap();
    /// let mut batch = Batch::new();
    /// batch.delete_key_prefix(vec![0]);
    /// view.write_batch(batch).await.unwrap();
    /// let key_values = view.find_key_values_by_prefix(&[0]).await.unwrap();
    /// assert_eq!(key_values, vec![]);
    /// # })
    /// ```
    pub async fn write_batch(&mut self, batch: Batch) -> Result<(), ViewError> {
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
                    if self.deletion_set.contains_prefix_of(&key) {
                        // Optimization: No need to mark `short_key` for deletion as we are going to remove all the keys at once.
                        self.updates.remove(&key);
                    } else {
                        self.updates.insert(key, Update::Removed);
                    }
                }
                WriteOperation::Put { key, value } => {
                    ensure!(key.len() <= max_key_size, ViewError::KeyTooLong);
                    let entry_size = SizeData {
                        key: u32::try_from(key.len()).map_err(|_| ArithmeticError::Overflow)?,
                        value: u32::try_from(value.len()).map_err(|_| ArithmeticError::Overflow)?,
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
                            key: u32::try_from(key.len()).map_err(|_| ArithmeticError::Overflow)?,
                            value,
                        };
                        self.total_size.sub_assign(entry_size);
                        self.sizes.remove(key);
                    }
                    self.sizes.remove_by_prefix(key_prefix.clone());
                    self.deletion_set.insert_key_prefix(key_prefix);
                }
            }
        }
        Ok(())
    }

    /// Sets or inserts a value.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).await.unwrap();
    /// assert_eq!(view.get(&[0, 1]).await.unwrap(), Some(vec![34]));
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).await.unwrap();
    /// view.remove(vec![0, 1]).await.unwrap();
    /// assert_eq!(view.get(&[0, 1]).await.unwrap(), None);
    /// # })
    /// ```
    pub async fn remove(&mut self, index: Vec<u8>) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.delete_key(index);
        self.write_batch(batch).await
    }

    /// Deletes a key prefix.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).await.unwrap();
    /// view.remove_by_prefix(vec![0]).await.unwrap();
    /// assert_eq!(view.get(&[0, 1]).await.unwrap(), None);
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).await.unwrap();
    /// view.insert(vec![3, 4], vec![42]).await.unwrap();
    /// let keys = view.find_keys_by_prefix(&[0]).await.unwrap();
    /// assert_eq!(keys, vec![vec![1]]);
    /// # })
    /// ```
    pub async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, ViewError> {
        let len = key_prefix.len();
        let key_prefix_full = self
            .context
            .base_key()
            .base_tag_index(KeyTag::Index as u8, key_prefix);
        let mut keys = Vec::new();
        let key_prefix_upper = get_upper_bound(key_prefix);
        let mut updates = self
            .updates
            .range((Included(key_prefix.to_vec()), key_prefix_upper));
        let mut update = updates.next();
        if !self.deletion_set.delete_storage_first {
            let mut suffix_closed_set =
                SuffixClosedSetIterator::new(0, self.deletion_set.deleted_prefixes.iter());
            for key in self
                .context
                .store()
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::BigKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = BigKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).await.unwrap();
    /// view.insert(vec![3, 4], vec![42]).await.unwrap();
    /// let key_values = view.find_key_values_by_prefix(&[0]).await.unwrap();
    /// assert_eq!(key_values, vec![(vec![1], vec![34])]);
    /// # })
    /// ```
    pub async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
        let len = key_prefix.len();
        let key_prefix_full = self
            .context
            .base_key()
            .base_tag_index(KeyTag::Index as u8, key_prefix);
        let mut key_values = Vec::new();
        let key_prefix_upper = get_upper_bound(key_prefix);
        let mut updates = self
            .updates
            .range((Included(key_prefix.to_vec()), key_prefix_upper));
        let mut update = updates.next();
        if !self.deletion_set.delete_storage_first {
            let mut suffix_closed_set =
                SuffixClosedSetIterator::new(0, self.deletion_set.deleted_prefixes.iter());
            for entry in self
                .context
                .store()
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
        let mut hasher = sha3::Sha3_256::default();
        let mut count = 0u32;
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

impl<C> HashableView<C> for BigKeyValueStoreView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash().await
    }
}

#[derive(Serialize, Deserialize)]
struct SizeAndState {
    total_size: SizeData,
    state: Option<BTreeMap<Vec<u8>, Vec<u8>>>,
}

/// A view that represents the functions of `KeyValueStore`.
///
/// Comment on the data set:
/// In order to work, the view needs to store the updates and deleted prefixes.
/// The updates and deleted prefixes have to be coherent. This means:
/// * If an index is deleted by one in deleted prefixes then it should not be present
///   in the updates at all.
/// * [`DeletePrefix::key_prefix`][entry1] should not dominate anyone. That is if we have `[0,2]`
///   then we should not have `[0,2,3]` since it would be dominated by the preceding.
///
/// With that we have:
/// * in order to test if an `index` is deleted by a prefix we compute the highest deleted prefix `dp`
///   such that `dp <= index`.
///   If `dp` is indeed a prefix then we conclude that `index` is deleted, otherwise not.
///   The no domination is essential here.
///
/// [entry1]: crate::batch::WriteOperation::DeletePrefix
#[derive(Debug)]
pub struct SmallKeyValueStoreView<C> {
    context: C,
    delete_storage_first: bool,
    stored_state: BTreeMap<Vec<u8>, Vec<u8>>,
    state: BTreeMap<Vec<u8>, Vec<u8>>,
    stored_total_size: SizeData,
    total_size: SizeData,
}

impl<C> View<C> for SmallKeyValueStoreView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    const NUM_INIT_KEYS: usize = NUM_INIT_KEYS;

    fn context(&self) -> &C {
        &self.context
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        let key_total_size = context.base_key().base_tag(KeyTag::TotalSize as u8);
        Ok(vec![key_total_size])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let value = values.first().ok_or(ViewError::PostLoadValuesError)?;
        let size_and_state = from_bytes_option::<SizeAndState, _>(value)?;
        let (total_size, state) = match size_and_state {
            None => (SizeData::default(), BTreeMap::default()),
            Some(SizeAndState { total_size, state }) => {
                if state.is_none() {
                    return Err(ViewError::PostLoadValuesError);
                }
                (total_size, state.unwrap())
            }
        };
        Ok(Self {
            context,
            delete_storage_first: false,
            stored_state: state.clone(),
            state,
            stored_total_size: total_size,
            total_size,
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let keys = Self::pre_load(&context)?;
        let values = context.store().read_multi_values_bytes(keys).await?;
        Self::post_load(context, &values)
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.state = self.stored_state.clone();
        self.total_size = self.stored_total_size;
    }

    async fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        if self.stored_total_size != self.total_size {
            return true;
        }
        self.stored_state != self.state
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            delete_view = true;
            if !self.state.is_empty() {
                delete_view = false;
            }
        }
        self.stored_total_size = self.total_size;
        self.stored_state = self.state.clone();
        let size_and_state = SizeAndState {
            total_size: self.total_size,
            state: Some(self.state.clone()),
        };
        let key = self.context.base_key().base_tag(KeyTag::TotalSize as u8);
        if self.state.is_empty() {
            batch.delete_key(key);
        } else {
            batch.put_key_value(key, &size_and_state)?;
        }
        self.delete_storage_first = false;
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.state.clear();
        self.total_size = SizeData::default();
    }
}

impl<C> ClonableView<C> for SmallKeyValueStoreView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(SmallKeyValueStoreView {
            context: self.context.clone(),
            delete_storage_first: self.delete_storage_first,
            stored_state: self.stored_state.clone(),
            state: self.state.clone(),
            stored_total_size: self.stored_total_size,
            total_size: self.total_size,
        })
    }
}

impl<C> SmallKeyValueStoreView<C>
where
    C: Send + Context + Sync,
    ViewError: From<C::Error>,
{
    fn max_key_size(&self) -> usize {
        let prefix_len = self.context.base_key().bytes.len();
        <C::Store as ReadableKeyValueStore>::MAX_KEY_SIZE - 1 - prefix_len
    }

    /// Getting the total sizes that will be used for keys and values when stored
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::{SmallKeyValueStoreView, SizeData};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// let total_size = view.total_size();
    /// assert_eq!(total_size, SizeData::default());
    /// # })
    /// ```
    pub fn total_size(&self) -> SizeData {
        self.total_size
    }

    /// Applies the function f over all indices. If the function f returns
    /// false, then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).unwrap();
    /// view.insert(vec![0, 2], vec![0]).unwrap();
    /// view.insert(vec![0, 3], vec![0]).unwrap();
    /// let mut count = 0;
    /// view.for_each_index_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 2)
    /// })
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// # })
    /// ```
    pub fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        for key in self.state.keys() {
            if !f(key)? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Applies the function f over all indices.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).unwrap();
    /// view.insert(vec![0, 2], vec![0]).unwrap();
    /// view.insert(vec![0, 3], vec![0]).unwrap();
    /// let mut count = 0;
    /// view.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .unwrap();
    /// assert_eq!(count, 3);
    /// # })
    /// ```
    pub fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_index_while(|key| {
            f(key)?;
            Ok(true)
        })
    }

    /// Applies the function f over all index/value pairs.
    /// If the function f returns false then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).unwrap();
    /// view.insert(vec![0, 2], vec![0]).unwrap();
    /// let mut values = Vec::new();
    /// view.for_each_index_value_while(|_key, value| {
    ///     values.push(value.to_vec());
    ///     Ok(values.len() < 1)
    /// })
    /// .unwrap();
    /// assert_eq!(values, vec![vec![0]]);
    /// # })
    /// ```
    pub fn for_each_index_value_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool, ViewError> + Send,
    {
        for (key, value) in &self.state {
            if !f(key, value)? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Applies the function f over all index/value pairs.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).unwrap();
    /// view.insert(vec![0, 2], vec![0]).unwrap();
    /// let mut part_keys = Vec::new();
    /// view.for_each_index_while(|key| {
    ///     part_keys.push(key.to_vec());
    ///     Ok(part_keys.len() < 1)
    /// })
    /// .unwrap();
    /// assert_eq!(part_keys, vec![vec![0, 1]]);
    /// # })
    /// ```
    pub fn for_each_index_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_index_value_while(|key, value| {
            f(key, value)?;
            Ok(true)
        })
    }

    /// Returns the list of indices in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).unwrap();
    /// view.insert(vec![0, 2], vec![0]).unwrap();
    /// let indices = view.indices().unwrap();
    /// assert_eq!(indices, vec![vec![0, 1], vec![0, 2]]);
    /// # })
    /// ```
    pub fn indices(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut indices = Vec::new();
        self.for_each_index(|index| {
            indices.push(index.to_vec());
            Ok(())
        })?;
        Ok(indices)
    }

    /// Returns the list of indices and values in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).unwrap();
    /// view.insert(vec![0, 2], vec![0]).unwrap();
    /// let key_values = view.index_values().unwrap();
    /// assert_eq!(
    ///     key_values,
    ///     vec![(vec![0, 1], vec![0]), (vec![0, 2], vec![0])]
    /// );
    /// # })
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn index_values(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
        let mut index_values = Vec::new();
        self.for_each_index_value(|index, value| {
            index_values.push((index.to_vec(), value.to_vec()));
            Ok(())
        })?;
        Ok(index_values)
    }

    /// Returns the number of entries.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).unwrap();
    /// view.insert(vec![0, 2], vec![0]).unwrap();
    /// let count = view.count().unwrap();
    /// assert_eq!(count, 2);
    /// # })
    /// ```
    pub fn count(&self) -> Result<usize, ViewError> {
        let mut count = 0;
        self.for_each_index(|_index| {
            count += 1;
            Ok(())
        })?;
        Ok(count)
    }

    /// Obtains the value at the given index, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).unwrap();
    /// assert_eq!(view.get(&[0, 1]), Some(vec![42]));
    /// assert_eq!(view.get(&[0, 2]), None);
    /// # })
    /// ```
    pub fn get(&self, index: &[u8]) -> Option<Vec<u8>> {
        self.state.get(index).cloned()
    }

    /// Tests whether the store contains a specific index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).unwrap();
    /// assert!(view.contains_key(&[0, 1]));
    /// assert!(!view.contains_key(&[0, 2]));
    /// # })
    /// ```
    pub fn contains_key(&self, index: &[u8]) -> bool {
        self.state.contains_key(index)
    }

    /// Tests whether the view contains a range of indices
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).unwrap();
    /// let keys = vec![vec![0, 1], vec![0, 2]];
    /// let results = view.contains_keys(keys);
    /// assert_eq!(results, vec![true, false]);
    /// # })
    /// ```
    pub fn contains_keys(&self, indices: Vec<Vec<u8>>) -> Vec<bool> {
        let mut results = Vec::with_capacity(indices.len());
        for index in indices {
            results.push(self.state.contains_key(&index))
        }
        results
    }

    /// Obtains the values of a range of indices
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).unwrap();
    /// assert_eq!(
    ///     view.multi_get(vec![vec![0, 1], vec![0, 2]]),
    ///     vec![Some(vec![42]), None]
    /// );
    /// # })
    /// ```
    pub fn multi_get(&self, indices: Vec<Vec<u8>>) -> Vec<Option<Vec<u8>>> {
        let mut result = Vec::with_capacity(indices.len());
        for index in indices {
            result.push(self.state.get(&index).cloned())
        }
        result
    }

    /// Applies the given batch of `crate::common::WriteOperation`.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::batch::Batch;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).unwrap();
    /// view.insert(vec![3, 4], vec![42]).unwrap();
    /// let mut batch = Batch::new();
    /// batch.delete_key_prefix(vec![0]);
    /// view.write_batch(batch).unwrap();
    /// let key_values = view.find_key_values_by_prefix(&[0]);
    /// assert_eq!(key_values, vec![]);
    /// # })
    /// ```
    pub fn write_batch(&mut self, batch: Batch) -> Result<(), ViewError> {
        let max_key_size = self.max_key_size();
        for operation in batch.operations {
            match operation {
                WriteOperation::Delete { key } => {
                    ensure!(key.len() <= max_key_size, ViewError::KeyTooLong);
                    if let Some(value) = self.state.get(&key) {
                        let entry_size = SizeData {
                            key: u32::try_from(key.len()).map_err(|_| ArithmeticError::Overflow)?,
                            value: u32::try_from(value.len())
                                .map_err(|_| ArithmeticError::Overflow)?,
                        };
                        self.total_size.sub_assign(entry_size);
                    }
                    self.state.remove(&key);
                }
                WriteOperation::Put { key, value } => {
                    ensure!(key.len() <= max_key_size, ViewError::KeyTooLong);
                    let entry_size = SizeData {
                        key: u32::try_from(key.len()).map_err(|_| ArithmeticError::Overflow)?,
                        value: u32::try_from(value.len()).map_err(|_| ArithmeticError::Overflow)?,
                    };
                    self.total_size.add_assign(entry_size)?;
                    if let Some(value) = self.state.get(&key) {
                        let entry_size = SizeData {
                            key: u32::try_from(key.len()).map_err(|_| ArithmeticError::Overflow)?,
                            value: u32::try_from(value.len())
                                .map_err(|_| ArithmeticError::Overflow)?,
                        };
                        self.total_size.sub_assign(entry_size);
                    }
                    self.state.insert(key, value);
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    ensure!(key_prefix.len() <= max_key_size, ViewError::KeyTooLong);
                    let mut key_size = 0;
                    let mut value_size = 0;
                    let mut key_list = Vec::new();
                    for (key, value) in self.state.range(get_interval(key_prefix.clone())) {
                        key_size += key.len();
                        value_size += value.len();
                        key_list.push(key.clone());
                    }
                    for key in key_list {
                        self.state.remove(&key);
                    }
                    let entry_size = SizeData {
                        key: u32::try_from(key_size).map_err(|_| ArithmeticError::Overflow)?,
                        value: u32::try_from(value_size).map_err(|_| ArithmeticError::Overflow)?,
                    };
                    self.total_size.sub_assign(entry_size);
                }
            }
        }
        Ok(())
    }

    /// Sets or inserts a value.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).unwrap();
    /// assert_eq!(view.get(&[0, 1]), Some(vec![34]));
    /// # })
    /// ```
    pub fn insert(&mut self, index: Vec<u8>, value: Vec<u8>) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.put_key_value_bytes(index, value);
        self.write_batch(batch)
    }

    /// Removes a value. If absent then the action has no effect.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).unwrap();
    /// view.remove(vec![0, 1]).unwrap();
    /// assert_eq!(view.get(&[0, 1]), None);
    /// # })
    /// ```
    pub fn remove(&mut self, index: Vec<u8>) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.delete_key(index);
        self.write_batch(batch)
    }

    /// Deletes a key prefix.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).unwrap();
    /// view.remove_by_prefix(vec![0]).unwrap();
    /// assert_eq!(view.get(&[0, 1]), None);
    /// # })
    /// ```
    pub fn remove_by_prefix(&mut self, key_prefix: Vec<u8>) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.delete_key_prefix(key_prefix);
        self.write_batch(batch)
    }

    /// Iterates over all the keys matching the given prefix. The prefix is not included in the returned keys.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).unwrap();
    /// view.insert(vec![3, 4], vec![42]).unwrap();
    /// let keys = view.find_keys_by_prefix(&[0]);
    /// assert_eq!(keys, vec![vec![1]]);
    /// # })
    /// ```
    pub fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        let len_prefix = key_prefix.len();
        for (key, _) in self.state.range(get_interval(key_prefix.to_vec())) {
            let key = key[len_prefix..].to_vec();
            keys.push(key);
        }
        keys
    }

    /// Iterates over all the key-value pairs, for keys matching the given prefix. The
    /// prefix is not included in the returned keys.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).unwrap();
    /// view.insert(vec![3, 4], vec![42]).unwrap();
    /// let key_values = view.find_key_values_by_prefix(&[0]);
    /// assert_eq!(key_values, vec![(vec![1], vec![34])]);
    /// # })
    /// ```
    pub fn find_key_values_by_prefix(&self, key_prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut key_values = Vec::new();
        let len_prefix = key_prefix.len();
        for (key, value) in self.state.range(get_interval(key_prefix.to_vec())) {
            let key = key[len_prefix..].to_vec();
            key_values.push((key, value.clone()));
        }
        key_values
    }

    fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        let mut hasher = sha3::Sha3_256::default();
        let mut count = 0u32;
        self.for_each_index_value(|index, value| -> Result<(), ViewError> {
            count += 1;
            hasher.update_with_bytes(index)?;
            hasher.update_with_bytes(value)?;
            Ok(())
        })?;
        hasher.update_with_bcs_bytes(&count)?;
        Ok(hasher.finalize())
    }
}

impl<C> HashableView<C> for SmallKeyValueStoreView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash()
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash()
    }
}

/// A combined store.
#[derive(Debug)]
pub enum KeyValueStoreView<C> {
    /// The case of a small key value store
    Small(SmallKeyValueStoreView<C>),
    /// The case of a big key value store
    Big(BigKeyValueStoreView<C>),
}

impl<C> View<C> for KeyValueStoreView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    const NUM_INIT_KEYS: usize = NUM_INIT_KEYS;

    fn context(&self) -> &C {
        match self {
            Self::Small(small) => small.context(),
            Self::Big(big) => big.context(),
        }
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        let key_total_size = context.base_key().base_tag(KeyTag::TotalSize as u8);
        Ok(vec![key_total_size])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let value = values.first().ok_or(ViewError::PostLoadValuesError)?;
        let size_and_state = from_bytes_option::<SizeAndState, _>(value)?;
        Ok(match size_and_state {
            None => Self::Small(SmallKeyValueStoreView::post_load(context, values)?),
            Some(SizeAndState {
                total_size: _,
                state,
            }) => match state.is_some() {
                true => Self::Small(SmallKeyValueStoreView::post_load(context, values)?),
                false => Self::Big(BigKeyValueStoreView::post_load(context, values)?),
            },
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let keys = Self::pre_load(&context)?;
        let values = context.store().read_multi_values_bytes(keys).await?;
        Self::post_load(context, &values)
    }

    fn rollback(&mut self) {
        match self {
            Self::Small(small) => small.rollback(),
            Self::Big(big) => big.rollback(),
        }
    }

    async fn has_pending_changes(&self) -> bool {
        match self {
            Self::Small(small) => small.has_pending_changes().await,
            Self::Big(big) => big.has_pending_changes().await,
        }
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        match self {
            Self::Small(small) => {
                let size = small.total_size.sum() as usize;
                if size < THRESHOLD_BIG_SMALL {
                    small.flush(batch)
                } else {
                    for (key, value) in mem::take(&mut small.state) {
                        let value_size =
                            u32::try_from(value.len()).map_err(|_| ArithmeticError::Overflow)?;
                        let key_size = small
                            .context
                            .base_key()
                            .base_tag_index(KeyTag::Sizes as u8, &key);
                        batch.put_key_value(key_size, &value_size)?;
                        batch.put_key_value_bytes(key, value);
                    }
                    let size_and_state = SizeAndState {
                        total_size: small.total_size,
                        state: None,
                    };
                    let key_total_size = small.context.base_key().base_tag(KeyTag::TotalSize as u8);
                    batch.put_key_value(key_total_size, &size_and_state)?;
                    let mut big =
                        BigKeyValueStoreView::post_load(small.context().clone(), &[None])?;
                    big.stored_total_size = small.total_size;
                    big.total_size = small.total_size;
                    *self = Self::Big(big);
                    Ok(true)
                }
            }
            Self::Big(big) => big.flush(batch),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Small(small) => small.clear(),
            Self::Big(big) => big.clear(),
        }
    }
}

impl<C> ClonableView<C> for KeyValueStoreView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(match self {
            Self::Small(small) => Self::Small(small.clone_unchecked()?),
            Self::Big(big) => Self::Big(big.clone_unchecked()?),
        })
    }
}

impl<C> KeyValueStoreView<C>
where
    C: Send + Context + Sync,
    ViewError: From<C::Error>,
{
    fn max_key_size(&self) -> usize {
        let prefix_len = self.context().base_key().bytes.len();
        <C::Store as ReadableKeyValueStore>::MAX_KEY_SIZE - 1 - prefix_len
    }

    /// Getting the total sizes that will be used for keys and values when stored
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::{KeyValueStoreView, SizeData};
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// let total_size = view.total_size();
    /// assert_eq!(total_size, SizeData::default());
    /// # })
    /// ```
    pub fn total_size(&self) -> SizeData {
        match self {
            Self::Small(small) => small.total_size(),
            Self::Big(big) => big.total_size(),
        }
    }

    /// Applies the function f over all indices. If the function f returns
    /// false, then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// view.insert(vec![0, 3], vec![0]).await.unwrap();
    /// let mut count = 0;
    /// view.for_each_index_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 2)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 2);
    /// # })
    /// ```
    pub async fn for_each_index_while<F>(&self, f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        match self {
            Self::Small(small) => small.for_each_index_while(f)?,
            Self::Big(big) => big.for_each_index_while(f).await?,
        };
        Ok(())
    }

    /// Applies the function f over all indices.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// view.insert(vec![0, 3], vec![0]).await.unwrap();
    /// let mut count = 0;
    /// view.for_each_index(|_key| {
    ///     count += 1;
    ///     Ok(())
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(count, 3);
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// let mut values = Vec::new();
    /// view.for_each_index_value_while(|_key, value| {
    ///     values.push(value.to_vec());
    ///     Ok(values.len() < 1)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(values, vec![vec![0]]);
    /// # })
    /// ```
    pub async fn for_each_index_value_while<F>(&self, f: F) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool, ViewError> + Send,
    {
        match self {
            Self::Small(small) => small.for_each_index_value_while(f)?,
            Self::Big(big) => big.for_each_index_value_while(f).await?,
        };
        Ok(())
    }

    /// Applies the function f over all index/value pairs.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// let mut part_keys = Vec::new();
    /// view.for_each_index_while(|key| {
    ///     part_keys.push(key.to_vec());
    ///     Ok(part_keys.len() < 1)
    /// })
    /// .await
    /// .unwrap();
    /// assert_eq!(part_keys, vec![vec![0, 1]]);
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// let indices = view.indices().await.unwrap();
    /// assert_eq!(indices, vec![vec![0, 1], vec![0, 2]]);
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// let key_values = view.index_values().await.unwrap();
    /// assert_eq!(
    ///     key_values,
    ///     vec![(vec![0, 1], vec![0]), (vec![0, 2], vec![0])]
    /// );
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// let count = view.count().await.unwrap();
    /// assert_eq!(count, 2);
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).await.unwrap();
    /// assert_eq!(view.get(&[0, 1]).await.unwrap(), Some(vec![42]));
    /// assert_eq!(view.get(&[0, 2]).await.unwrap(), None);
    /// # })
    /// ```
    pub async fn get(&self, index: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::KEY_VALUE_STORE_VIEW_GET_LATENCY.measure_latency();
        Ok(match self {
            Self::Small(small) => small.get(index),
            Self::Big(big) => big.get(index).await?,
        })
    }

    /// Tests whether the store contains a specific index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).await.unwrap();
    /// assert!(view.contains_key(&[0, 1]).await.unwrap());
    /// assert!(!view.contains_key(&[0, 2]).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key(&self, index: &[u8]) -> Result<bool, ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::KEY_VALUE_STORE_VIEW_CONTAINS_KEY_LATENCY.measure_latency();
        Ok(match self {
            Self::Small(small) => small.contains_key(index),
            Self::Big(big) => big.contains_key(index).await?,
        })
    }

    /// Tests whether the view contains a range of indices
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).await.unwrap();
    /// let keys = vec![vec![0, 1], vec![0, 2]];
    /// let results = view.contains_keys(keys).await.unwrap();
    /// assert_eq!(results, vec![true, false]);
    /// # })
    /// ```
    pub async fn contains_keys(&self, indices: Vec<Vec<u8>>) -> Result<Vec<bool>, ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::KEY_VALUE_STORE_VIEW_CONTAINS_KEYS_LATENCY.measure_latency();
        Ok(match self {
            Self::Small(small) => small.contains_keys(indices),
            Self::Big(big) => big.contains_keys(indices).await?,
        })
    }

    /// Obtains the values of a range of indices
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).await.unwrap();
    /// assert_eq!(
    ///     view.multi_get(vec![vec![0, 1], vec![0, 2]]).await.unwrap(),
    ///     vec![Some(vec![42]), None]
    /// );
    /// # })
    /// ```
    pub async fn multi_get(
        &self,
        indices: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::KEY_VALUE_STORE_VIEW_MULTI_GET_LATENCY.measure_latency();
        Ok(match self {
            Self::Small(small) => small.multi_get(indices),
            Self::Big(big) => big.multi_get(indices).await?,
        })
    }

    /// Applies the given batch of `crate::common::WriteOperation`.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::batch::Batch;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).await.unwrap();
    /// view.insert(vec![3, 4], vec![42]).await.unwrap();
    /// let mut batch = Batch::new();
    /// batch.delete_key_prefix(vec![0]);
    /// view.write_batch(batch).await.unwrap();
    /// let key_values = view.find_key_values_by_prefix(&[0]).await.unwrap();
    /// assert_eq!(key_values, vec![]);
    /// # })
    /// ```
    pub async fn write_batch(&mut self, batch: Batch) -> Result<(), ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::KEY_VALUE_STORE_VIEW_WRITE_BATCH_LATENCY.measure_latency();
        match self {
            Self::Small(small) => small.write_batch(batch)?,
            Self::Big(big) => big.write_batch(batch).await?,
        };
        Ok(())
    }

    /// Sets or inserts a value.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).await.unwrap();
    /// assert_eq!(view.get(&[0, 1]).await.unwrap(), Some(vec![34]));
    /// # })
    /// ```
    pub async fn insert(&mut self, index: Vec<u8>, value: Vec<u8>) -> Result<(), ViewError> {
        match self {
            Self::Small(small) => small.insert(index, value)?,
            Self::Big(big) => big.insert(index, value).await?,
        };
        Ok(())
    }

    /// Removes a value. If absent then the action has no effect.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).await.unwrap();
    /// view.remove(vec![0, 1]).await.unwrap();
    /// assert_eq!(view.get(&[0, 1]).await.unwrap(), None);
    /// # })
    /// ```
    pub async fn remove(&mut self, index: Vec<u8>) -> Result<(), ViewError> {
        match self {
            Self::Small(small) => small.remove(index)?,
            Self::Big(big) => big.remove(index).await?,
        };
        Ok(())
    }

    /// Deletes a key prefix.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).await.unwrap();
    /// view.remove_by_prefix(vec![0]).await.unwrap();
    /// assert_eq!(view.get(&[0, 1]).await.unwrap(), None);
    /// # })
    /// ```
    pub async fn remove_by_prefix(&mut self, key_prefix: Vec<u8>) -> Result<(), ViewError> {
        match self {
            Self::Small(small) => small.remove_by_prefix(key_prefix)?,
            Self::Big(big) => big.remove_by_prefix(key_prefix).await?,
        };
        Ok(())
    }

    /// Iterates over all the keys matching the given prefix. The prefix is not included in the returned keys.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).await.unwrap();
    /// view.insert(vec![3, 4], vec![42]).await.unwrap();
    /// let keys = view.find_keys_by_prefix(&[0]).await.unwrap();
    /// assert_eq!(keys, vec![vec![1]]);
    /// # })
    /// ```
    pub async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::KEY_VALUE_STORE_VIEW_FIND_KEYS_BY_PREFIX_LATENCY.measure_latency();
        ensure!(
            key_prefix.len() <= self.max_key_size(),
            ViewError::KeyTooLong
        );
        Ok(match self {
            Self::Small(small) => small.find_keys_by_prefix(key_prefix),
            Self::Big(big) => big.find_keys_by_prefix(key_prefix).await?,
        })
    }

    /// Iterates over all the key-value pairs, for keys matching the given prefix. The
    /// prefix is not included in the returned keys.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::KeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = KeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).await.unwrap();
    /// view.insert(vec![3, 4], vec![42]).await.unwrap();
    /// let key_values = view.find_key_values_by_prefix(&[0]).await.unwrap();
    /// assert_eq!(key_values, vec![(vec![1], vec![34])]);
    /// # })
    /// ```
    pub async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
        #[cfg(with_metrics)]
        let _latency =
            metrics::KEY_VALUE_STORE_VIEW_FIND_KEY_VALUES_BY_PREFIX_LATENCY.measure_latency();
        ensure!(
            key_prefix.len() <= self.max_key_size(),
            ViewError::KeyTooLong
        );
        Ok(match self {
            Self::Small(small) => small.find_key_values_by_prefix(key_prefix),
            Self::Big(big) => big.find_key_values_by_prefix(key_prefix).await?,
        })
    }

    async fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = metrics::KEY_VALUE_STORE_VIEW_HASH_LATENCY.measure_latency();
        Ok(match self {
            Self::Small(small) => small.compute_hash()?,
            Self::Big(big) => big.compute_hash().await?,
        })
    }
}

impl<C> HashableView<C> for KeyValueStoreView<C>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash().await
    }
}

/// Type wrapping `BigKeyValueStoreView` while memoizing the hash.
pub type HashedKeyValueStoreView<C> =
    WrappedHashableContainerView<C, KeyValueStoreView<C>, HasherOutput>;

/// A virtual DB client using a `KeyValueStoreView` as a backend (testing only).
#[cfg(with_testing)]
#[derive(Debug, Clone)]
pub struct ViewContainer<C> {
    view: Arc<RwLock<BigKeyValueStoreView<C>>>,
}

#[cfg(with_testing)]
impl<C> WithError for ViewContainer<C> {
    type Error = ViewContainerError;
}

#[cfg(with_testing)]
/// The error type for [`ViewContainer`] operations.
#[derive(Error, Debug)]
pub enum ViewContainerError {
    /// View error.
    #[error(transparent)]
    ViewError(#[from] ViewError),

    /// BCS serialization error.
    #[error(transparent)]
    BcsError(#[from] bcs::Error),
}

#[cfg(with_testing)]
impl KeyValueStoreError for ViewContainerError {
    const BACKEND: &'static str = "view_container";
}

#[cfg(with_testing)]
impl<C> ReadableKeyValueStore for ViewContainer<C>
where
    C: Context + Sync + Send + Clone,
    ViewError: From<C::Error>,
{
    const MAX_KEY_SIZE: usize = <C::Store as ReadableKeyValueStore>::MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        1
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ViewContainerError> {
        let view = self.view.read().await;
        Ok(view.get(key).await?)
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ViewContainerError> {
        let view = self.view.read().await;
        Ok(view.contains_key(key).await?)
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, ViewContainerError> {
        let view = self.view.read().await;
        Ok(view.contains_keys(keys).await?)
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ViewContainerError> {
        let view = self.view.read().await;
        Ok(view.multi_get(keys).await?)
    }

    async fn find_keys_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::Keys, ViewContainerError> {
        let view = self.view.read().await;
        Ok(view.find_keys_by_prefix(key_prefix).await?)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ViewContainerError> {
        let view = self.view.read().await;
        Ok(view.find_key_values_by_prefix(key_prefix).await?)
    }
}

#[cfg(with_testing)]
impl<C> WritableKeyValueStore for ViewContainer<C>
where
    C: Context + Sync + Send + Clone,
    ViewError: From<C::Error>,
{
    const MAX_VALUE_SIZE: usize = <C::Store as WritableKeyValueStore>::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch) -> Result<(), ViewContainerError> {
        let mut view = self.view.write().await;
        view.write_batch(batch).await?;
        let mut batch = Batch::new();
        view.flush(&mut batch)?;
        view.context()
            .store()
            .write_batch(batch)
            .await
            .map_err(ViewError::from)?;
        Ok(())
    }

    async fn clear_journal(&self) -> Result<(), ViewContainerError> {
        Ok(())
    }
}

#[cfg(with_testing)]
impl<C> ViewContainer<C>
where
    C: Context + Sync + Send + Clone,
    ViewError: From<C::Error>,
{
    /// Creates a [`ViewContainer`].
    pub async fn new(context: C) -> Result<Self, ViewError> {
        let view = BigKeyValueStoreView::load(context).await?;
        let view = Arc::new(RwLock::new(view));
        Ok(Self { view })
    }
}
