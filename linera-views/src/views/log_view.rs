// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    ops::{Bound, Range, RangeBounds},
};

use allocative::Allocative;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::{from_bytes_option_or_default, HasherOutput},
    context::Context,
    hashable_wrapper::WrappedHashableContainerView,
    historical_hash_wrapper::HistoricallyHashableView,
    store::ReadableKeyValueStore as _,
    views::{ClonableView, HashableView, Hasher, View, ViewError, MIN_VIEW_TAG},
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    /// The runtime of hash computation
    pub static LOG_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "log_view_hash_runtime",
            "LogView hash runtime",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });
}

/// Key tags to create the sub-keys of a `LogView` on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the storing of the variable `stored_count`.
    Count = MIN_VIEW_TAG,
    /// Prefix for the indices of the log.
    Index,
}

/// A view that supports logging values of type `T`.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative")]
pub struct LogView<C, T> {
    /// The view context.
    #[allocative(skip)]
    context: C,
    /// Whether to clear storage before applying updates.
    delete_storage_first: bool,
    /// The number of entries persisted in storage.
    stored_count: u32,
    /// New values not yet persisted to storage.
    new_values: Vec<T>,
}

impl<C, T> View for LogView<C, T>
where
    C: Context,
    T: Send + Sync + Serialize,
{
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> C {
        self.context.clone()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![context.base_key().base_tag(KeyTag::Count as u8)])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let stored_count =
            from_bytes_option_or_default(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        Ok(Self {
            context,
            delete_storage_first: false,
            stored_count,
            new_values: Vec::new(),
        })
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.new_values.clear();
    }

    async fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        !self.new_values.is_empty()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            batch.delete_key_prefix(self.context.base_key().bytes.clone());
            delete_view = true;
        }
        if !self.new_values.is_empty() {
            delete_view = false;
            let mut count = self.stored_count as usize;
            for value in &self.new_values {
                let key = self
                    .context
                    .base_key()
                    .derive_tag_key(KeyTag::Index as u8, &count)?;
                batch.put_key_value(key, value)?;
                count += 1;
            }
            let count_u32 = self.stored_count + self.new_values.len() as u32;
            let key = self.context.base_key().base_tag(KeyTag::Count as u8);
            batch.put_key_value(key, &count_u32)?;
        }
        Ok(delete_view)
    }

    fn post_save(&mut self) {
        if self.delete_storage_first {
            self.stored_count = 0;
        }
        self.stored_count += self.new_values.len() as u32;
        self.new_values.clear();
        self.delete_storage_first = false;
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.new_values.clear();
    }
}

impl<C, T> ClonableView for LogView<C, T>
where
    C: Context,
    T: Clone + Send + Sync + Serialize,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(LogView {
            context: self.context.clone(),
            delete_storage_first: self.delete_storage_first,
            stored_count: self.stored_count,
            new_values: self.new_values.clone(),
        })
    }
}

impl<C, T> LogView<C, T>
where
    C: Context,
{
    /// Pushes a value to the end of the log.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::log_view::LogView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut log = LogView::load(context).await.unwrap();
    /// log.push(34);
    /// # })
    /// ```
    pub fn push(&mut self, value: T) {
        self.new_values.push(value);
    }

    /// Reads the size of the log.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::log_view::LogView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut log = LogView::load(context).await.unwrap();
    /// log.push(34);
    /// log.push(42);
    /// assert_eq!(log.count(), 2);
    /// # })
    /// ```
    pub fn count(&self) -> usize {
        if self.delete_storage_first {
            self.new_values.len()
        } else {
            self.stored_count as usize + self.new_values.len()
        }
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, T> LogView<C, T>
where
    C: Context,
    T: Clone + DeserializeOwned + Serialize + Send + Sync,
{
    /// Reads the logged value with the given index (including staged ones).
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::log_view::LogView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut log = LogView::load(context).await.unwrap();
    /// log.push(34);
    /// assert_eq!(log.get(0).await.unwrap(), Some(34));
    /// # })
    /// ```
    pub async fn get(&self, index: usize) -> Result<Option<T>, ViewError> {
        let value = if self.delete_storage_first {
            self.new_values.get(index).cloned()
        } else if index < self.stored_count as usize {
            let key = self
                .context
                .base_key()
                .derive_tag_key(KeyTag::Index as u8, &index)?;
            self.context.store().read_value(&key).await?
        } else {
            self.new_values
                .get(index - self.stored_count as usize)
                .cloned()
        };
        Ok(value)
    }

    /// Reads several logged keys (including staged ones)
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::log_view::LogView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut log = LogView::load(context).await.unwrap();
    /// log.push(34);
    /// log.push(42);
    /// assert_eq!(
    ///     log.multi_get(vec![0, 1]).await.unwrap(),
    ///     vec![Some(34), Some(42)]
    /// );
    /// # })
    /// ```
    pub async fn multi_get(&self, indices: Vec<usize>) -> Result<Vec<Option<T>>, ViewError> {
        let mut result = Vec::new();
        if self.delete_storage_first {
            for index in indices {
                result.push(self.new_values.get(index).cloned());
            }
        } else {
            let mut index_to_positions = BTreeMap::<usize, Vec<usize>>::new();
            for (pos, index) in indices.into_iter().enumerate() {
                if index < self.stored_count as usize {
                    index_to_positions.entry(index).or_default().push(pos);
                    result.push(None);
                } else {
                    result.push(
                        self.new_values
                            .get(index - self.stored_count as usize)
                            .cloned(),
                    );
                }
            }
            let mut keys = Vec::new();
            let mut vec_positions = Vec::new();
            for (index, positions) in index_to_positions {
                let key = self
                    .context
                    .base_key()
                    .derive_tag_key(KeyTag::Index as u8, &index)?;
                keys.push(key);
                vec_positions.push(positions);
            }
            let values = self.context.store().read_multi_values(&keys).await?;
            for (positions, value) in vec_positions.into_iter().zip(values) {
                if let Some((&last, rest)) = positions.split_last() {
                    for &position in rest {
                        *result.get_mut(position).unwrap() = value.clone();
                    }
                    *result.get_mut(last).unwrap() = value;
                }
            }
        }
        Ok(result)
    }

    /// Reads the index-value pairs at the given positions.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::log_view::LogView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut log = LogView::load(context).await.unwrap();
    /// log.push(34);
    /// log.push(42);
    /// assert_eq!(
    ///     log.multi_get_pairs(vec![0, 1, 5]).await.unwrap(),
    ///     vec![(0, Some(34)), (1, Some(42)), (5, None)]
    /// );
    /// # })
    /// ```
    pub async fn multi_get_pairs(
        &self,
        indices: Vec<usize>,
    ) -> Result<Vec<(usize, Option<T>)>, ViewError> {
        let values = self.multi_get(indices.clone()).await?;
        Ok(indices.into_iter().zip(values).collect())
    }

    async fn read_context(&self, range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let count = range.len();
        let mut keys = Vec::with_capacity(count);
        for index in range {
            let key = self
                .context
                .base_key()
                .derive_tag_key(KeyTag::Index as u8, &index)?;
            keys.push(key);
        }
        let mut values = Vec::with_capacity(count);
        for entry in self.context.store().read_multi_values(&keys).await? {
            match entry {
                None => {
                    return Err(ViewError::MissingEntries("LogView".into()));
                }
                Some(value) => values.push(value),
            }
        }
        Ok(values)
    }

    /// Reads the logged values in the given range (including staged ones).
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::log_view::LogView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut log = LogView::load(context).await.unwrap();
    /// log.push(34);
    /// log.push(42);
    /// log.push(56);
    /// assert_eq!(log.read(0..2).await.unwrap(), vec![34, 42]);
    /// # })
    /// ```
    pub async fn read<R>(&self, range: R) -> Result<Vec<T>, ViewError>
    where
        R: RangeBounds<usize>,
    {
        let effective_stored_count = if self.delete_storage_first {
            0
        } else {
            self.stored_count as usize
        };
        let end = match range.end_bound() {
            Bound::Included(end) => *end + 1,
            Bound::Excluded(end) => *end,
            Bound::Unbounded => self.count(),
        }
        .min(self.count());
        let start = match range.start_bound() {
            Bound::Included(start) => *start,
            Bound::Excluded(start) => *start + 1,
            Bound::Unbounded => 0,
        };
        if start >= end {
            return Ok(Vec::new());
        }
        if start < effective_stored_count {
            if end <= effective_stored_count {
                self.read_context(start..end).await
            } else {
                let mut values = self.read_context(start..effective_stored_count).await?;
                values.extend(
                    self.new_values[0..(end - effective_stored_count)]
                        .iter()
                        .cloned(),
                );
                Ok(values)
            }
        } else {
            Ok(
                self.new_values[(start - effective_stored_count)..(end - effective_stored_count)]
                    .to_vec(),
            )
        }
    }
}

impl<C, T> HashableView for LogView<C, T>
where
    C: Context,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.hash().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = metrics::LOG_VIEW_HASH_RUNTIME.measure_latency();
        let elements = self.read(..).await?;
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bcs_bytes(&elements)?;
        Ok(hasher.finalize())
    }
}

/// Type wrapping `LogView` while memoizing the hash.
pub type HashedLogView<C, T> = WrappedHashableContainerView<C, LogView<C, T>, HasherOutput>;

/// Wrapper around `LogView` to compute hashes based on the history of changes.
pub type HistoricallyHashedLogView<C, T> = HistoricallyHashableView<C, LogView<C, T>>;

#[cfg(not(web))]
mod graphql {
    use std::borrow::Cow;

    use super::LogView;
    use crate::{
        context::Context,
        graphql::{hash_name, mangle},
    };

    impl<C: Send + Sync, T: async_graphql::OutputType> async_graphql::TypeName for LogView<C, T> {
        fn type_name() -> Cow<'static, str> {
            format!(
                "LogView_{}_{:08x}",
                mangle(T::type_name()),
                hash_name::<T>()
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C: Context, T: async_graphql::OutputType> LogView<C, T>
    where
        T: serde::ser::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync,
    {
        #[graphql(derived(name = "count"))]
        async fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count() as u32)
        }

        async fn entries(
            &self,
            start: Option<usize>,
            end: Option<usize>,
        ) -> async_graphql::Result<Vec<T>> {
            Ok(self
                .read(start.unwrap_or_default()..end.unwrap_or_else(|| self.count()))
                .await?)
        }
    }
}
