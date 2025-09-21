// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! We implement a `SmallKeyValueStoreView` implements `View` and the functions of `KeyValueStore`.

use std::{collections::BTreeMap, fmt::Debug};

#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{data_types::ArithmeticError, ensure};

use crate::{
    batch::{Batch, WriteOperation},
    common::{get_interval, HasherOutput},
    context::Context,
    hashable_wrapper::WrappedHashableContainerView,
    key_value_store_view::SizeData,
    register_view::RegisterView,
    sha3,
    store::ReadableKeyValueStore,
    views::{ClonableView, HashableView, Hasher, ReplaceContext, View},
    ViewError,
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    /// The latency of hash computation
    pub static SMALL_KEY_VALUE_STORE_VIEW_HASH_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "small_key_value_store_view_hash_latency",
                "SmallKeyValueStoreView hash latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of get operation
    pub static SMALL_KEY_VALUE_STORE_VIEW_GET_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "small_key_value_store_view_get_latency",
                "SmallKeyValueStoreView get latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of multi get
    pub static SMALL_KEY_VALUE_STORE_VIEW_MULTI_GET_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "small_key_value_store_view_multi_get_latency",
                "SmallKeyValueStoreView multi get latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of contains key
    pub static SMALL_KEY_VALUE_STORE_VIEW_CONTAINS_KEY_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "small_key_value_store_view_contains_key_latency",
                "SmallKeyValueStoreView contains key latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of contains keys
    pub static SMALL_KEY_VALUE_STORE_VIEW_CONTAINS_KEYS_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "small_key_value_store_view_contains_keys_latency",
                "SmallKeyValueStoreView contains keys latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of find keys by prefix operation
    pub static SMALL_KEY_VALUE_STORE_VIEW_FIND_KEYS_BY_PREFIX_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "small_key_value_store_view_find_keys_by_prefix_latency",
                "SmallKeyValueStoreView find keys by prefix latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });

    /// The latency of find key values by prefix operation
    pub static SMALL_KEY_VALUE_STORE_VIEW_FIND_KEY_VALUES_BY_PREFIX_LATENCY: LazyLock<
        HistogramVec,
    > = LazyLock::new(|| {
        register_histogram_vec(
            "small_key_value_store_view_find_key_values_by_prefix_latency",
            "SmallKeyValueStoreView find key values by prefix latency",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });

    /// The latency of write batch operation
    pub static SMALL_KEY_VALUE_STORE_VIEW_WRITE_BATCH_LATENCY: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "small_key_value_store_view_write_batch_latency",
                "SmallKeyValueStoreView write batch latency",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });
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
    map: RegisterView<C, BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl<C, C2> ReplaceContext<C2> for SmallKeyValueStoreView<C>
where
    C: Context,
    C2: Context,
{
    type Target = SmallKeyValueStoreView<C2>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        let map = self.map.with_context(ctx).await;
        SmallKeyValueStoreView { map }
    }
}

impl<C> View for SmallKeyValueStoreView<C>
where
    C: Context,
{
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> &C {
        self.map.context()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        RegisterView::<C, BTreeMap<Vec<u8>, Vec<u8>>>::pre_load(context)
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let map = RegisterView::post_load(context, values)?;
        Ok(SmallKeyValueStoreView { map })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let keys = Self::pre_load(&context)?;
        let values = context.store().read_multi_values_bytes(keys).await?;
        Self::post_load(context, &values)
    }

    fn rollback(&mut self) {
        self.map.rollback()
    }

    async fn has_pending_changes(&self) -> bool {
        self.map.has_pending_changes().await
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.map.flush(batch)
    }

    fn clear(&mut self) {
        self.map.clear()
    }
}

impl<C: Context> ClonableView for SmallKeyValueStoreView<C> {
    fn clone_unchecked(&mut self) -> Self {
        SmallKeyValueStoreView {
            map: self.map.clone_unchecked(),
        }
    }
}

impl<C: Context> SmallKeyValueStoreView<C> {
    fn max_key_size(&self) -> usize {
        let prefix_len = self.context().base_key().bytes.len();
        <C::Store as ReadableKeyValueStore>::MAX_KEY_SIZE - 1 - prefix_len
    }

    /// Getting the total sizes that will be used for keys and values when stored
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::key_value_store_view::SizeData;
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// let total_size = view.total_size().unwrap();
    /// assert_eq!(total_size, SizeData::default());
    /// # })
    /// ```
    pub fn total_size(&self) -> Result<SizeData, ViewError> {
        let mut key = 0_u32;
        let mut value = 0_u32;
        for key_value in self.map.get().iter() {
            key = key
                .checked_add(key_value.0.len() as u32)
                .ok_or(ArithmeticError::Overflow)?;
            value = value
                .checked_add(key_value.1.len() as u32)
                .ok_or(ArithmeticError::Overflow)?;
        }
        Ok(SizeData { key, value })
    }

    /// Applies the function f over all indices. If the function f returns
    /// false, then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
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
        let map = self.map.get();
        for key in map.keys() {
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
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
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
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
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
        let map = self.map.get();
        for (key, value) in map {
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
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
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
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
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
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![0]).await.unwrap();
    /// view.insert(vec![0, 2], vec![0]).await.unwrap();
    /// let key_values = view.indices().await.unwrap();
    /// assert_eq!(key_values, vec![vec![0, 1], vec![0, 2]]);
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
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
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
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).await.unwrap();
    /// assert_eq!(view.get(&[0, 1]).await.unwrap(), Some(vec![42]));
    /// assert_eq!(view.get(&[0, 2]).await.unwrap(), None);
    /// # })
    /// ```
    pub async fn get(&self, index: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::SMALL_KEY_VALUE_STORE_VIEW_GET_LATENCY.measure_latency();
        ensure!(index.len() <= self.max_key_size(), ViewError::KeyTooLong);
        let map = self.map.get();
        Ok(map.get(index).cloned())
    }

    /// Tests whether the store contains a specific index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).await.unwrap();
    /// assert!(view.contains_key(&[0, 1]).await.unwrap());
    /// assert!(!view.contains_key(&[0, 2]).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key(&self, index: &[u8]) -> Result<bool, ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::SMALL_KEY_VALUE_STORE_VIEW_CONTAINS_KEY_LATENCY.measure_latency();
        ensure!(index.len() <= self.max_key_size(), ViewError::KeyTooLong);
        let map = self.map.get();
        Ok(map.contains_key(index))
    }

    /// Tests whether the view contains a range of indices
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![42]).await.unwrap();
    /// let keys = vec![vec![0, 1], vec![0, 2]];
    /// let results = view.contains_keys(keys).await.unwrap();
    /// assert_eq!(results, vec![true, false]);
    /// # })
    /// ```
    pub async fn contains_keys(&self, indices: Vec<Vec<u8>>) -> Result<Vec<bool>, ViewError> {
        #[cfg(with_metrics)]
        let _latency = metrics::SMALL_KEY_VALUE_STORE_VIEW_CONTAINS_KEYS_LATENCY.measure_latency();
        let map = self.map.get();
        let mut results = Vec::with_capacity(indices.len());
        for index in indices {
            ensure!(index.len() <= self.max_key_size(), ViewError::KeyTooLong);
            results.push(map.contains_key(&index));
        }
        Ok(results)
    }

    /// Obtains the values of a range of indices
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
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
        let _latency = metrics::SMALL_KEY_VALUE_STORE_VIEW_MULTI_GET_LATENCY.measure_latency();
        let map = self.map.get();
        let mut results = Vec::with_capacity(indices.len());
        for index in indices {
            ensure!(index.len() <= self.max_key_size(), ViewError::KeyTooLong);
            results.push(map.get(&index).cloned());
        }
        Ok(results)
    }

    /// Applies the given batch of `crate::common::WriteOperation`.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::batch::Batch;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
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
        let _latency = metrics::SMALL_KEY_VALUE_STORE_VIEW_WRITE_BATCH_LATENCY.measure_latency();
        let max_key_size = self.max_key_size();
        let map = self.map.get_mut();
        for operation in batch.operations {
            match operation {
                WriteOperation::Delete { key } => {
                    ensure!(key.len() <= max_key_size, ViewError::KeyTooLong);
                    map.remove(&key);
                }
                WriteOperation::Put { key, value } => {
                    ensure!(key.len() <= max_key_size, ViewError::KeyTooLong);
                    map.insert(key, value);
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    ensure!(key_prefix.len() <= max_key_size, ViewError::KeyTooLong);
                    let key_list = map
                        .range(get_interval(key_prefix.clone()))
                        .map(|x| x.0.to_vec())
                        .collect::<Vec<_>>();
                    for key in key_list {
                        map.remove(&key);
                    }
                }
            }
        }
        Ok(())
    }

    /// Sets or inserts a value.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
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
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
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
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
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
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
    /// view.insert(vec![0, 1], vec![34]).await.unwrap();
    /// view.insert(vec![3, 4], vec![42]).await.unwrap();
    /// let keys = view.find_keys_by_prefix(&[0]).await.unwrap();
    /// assert_eq!(keys, vec![vec![1]]);
    /// # })
    /// ```
    pub async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, ViewError> {
        #[cfg(with_metrics)]
        let _latency =
            metrics::SMALL_KEY_VALUE_STORE_VIEW_FIND_KEYS_BY_PREFIX_LATENCY.measure_latency();
        ensure!(
            key_prefix.len() <= self.max_key_size(),
            ViewError::KeyTooLong
        );
        let len = key_prefix.len();
        let map = self.map.get();
        Ok(map
            .range(get_interval(key_prefix.to_vec()))
            .map(|x| x.0[len..].to_vec())
            .collect::<Vec<_>>())
    }

    /// Iterates over all the key-value pairs, for keys matching the given prefix. The
    /// prefix is not included in the returned keys.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::small_key_value_store_view::SmallKeyValueStoreView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut view = SmallKeyValueStoreView::load(context).await.unwrap();
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
            metrics::SMALL_KEY_VALUE_STORE_VIEW_FIND_KEY_VALUES_BY_PREFIX_LATENCY.measure_latency();
        ensure!(
            key_prefix.len() <= self.max_key_size(),
            ViewError::KeyTooLong
        );
        let len = key_prefix.len();
        let map = self.map.get();
        Ok(map
            .range(get_interval(key_prefix.to_vec()))
            .map(|x| (x.0[len..].to_vec(), x.1.to_vec()))
            .collect::<Vec<_>>())
    }

    async fn compute_hash(&self) -> Result<<sha3::Sha3_256 as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = metrics::SMALL_KEY_VALUE_STORE_VIEW_HASH_LATENCY.measure_latency();
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

impl<C: Context> HashableView for SmallKeyValueStoreView<C> {
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.compute_hash().await
    }
}

/// A key-value-store view that uses a single `BTreeMap` for storing the data.
pub type HashedSmallKeyValueStoreView<C> =
    WrappedHashableContainerView<C, SmallKeyValueStoreView<C>, HasherOutput>;
