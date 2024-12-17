// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::{Bound, Range, RangeBounds};
#[cfg(with_metrics)]
use std::sync::LazyLock;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{bucket_latencies, register_histogram_vec, MeasureLatency},
    prometheus::HistogramVec,
};

use crate::{
    batch::Batch,
    common::{from_bytes_option_or_default, HasherOutput},
    context::Context,
    hashable_wrapper::WrappedHashableContainerView,
    views::{ClonableView, HashableView, Hasher, View, ViewError, MIN_VIEW_TAG},
};

#[cfg(with_metrics)]
/// The runtime of hash computation
static LOG_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "log_view_hash_runtime",
        "LogView hash runtime",
        &[],
        bucket_latencies(5.0),
    )
});

/// Key tags to create the sub-keys of a LogView on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the storing of the variable stored_count.
    Count = MIN_VIEW_TAG,
    /// Prefix for the indices of the log.
    Index,
}

/// A view that supports logging values of type `T`.
#[derive(Debug)]
pub struct LogView<C, T> {
    context: C,
    delete_storage_first: bool,
    stored_count: usize,
    new_values: Vec<T>,
}

#[async_trait]
impl<C, T> View<C> for LogView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Serialize,
{
    const NUM_INIT_KEYS: usize = 1;

    fn context(&self) -> &C {
        &self.context
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![context.base_tag(KeyTag::Count as u8)])
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

    async fn load(context: C) -> Result<Self, ViewError> {
        let keys = Self::pre_load(&context)?;
        let values = context.read_multi_values_bytes(keys).await?;
        Self::post_load(context, &values)
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

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            batch.delete_key_prefix(self.context.base_key());
            self.stored_count = 0;
            delete_view = true;
        }
        if !self.new_values.is_empty() {
            delete_view = false;
            for value in &self.new_values {
                let key = self
                    .context
                    .derive_tag_key(KeyTag::Index as u8, &self.stored_count)?;
                batch.put_key_value(key, value)?;
                self.stored_count += 1;
            }
            let key = self.context.base_tag(KeyTag::Count as u8);
            batch.put_key_value(key, &self.stored_count)?;
            self.new_values.clear();
        }
        self.delete_storage_first = false;
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.new_values.clear();
    }
}

impl<C, T> ClonableView<C> for LogView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
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
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::log_view::LogView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
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
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::log_view::LogView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
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
            self.stored_count + self.new_values.len()
        }
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }
}

impl<C, T> LogView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Clone + DeserializeOwned + Serialize + Send,
{
    /// Reads the logged value with the given index (including staged ones).
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::log_view::LogView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut log = LogView::load(context).await.unwrap();
    /// log.push(34);
    /// assert_eq!(log.get(0).await.unwrap(), Some(34));
    /// # })
    /// ```
    pub async fn get(&self, index: usize) -> Result<Option<T>, ViewError> {
        let value = if self.delete_storage_first {
            self.new_values.get(index).cloned()
        } else if index < self.stored_count {
            let key = self.context.derive_tag_key(KeyTag::Index as u8, &index)?;
            self.context.read_value(&key).await?
        } else {
            self.new_values.get(index - self.stored_count).cloned()
        };
        Ok(value)
    }

    /// Reads several logged keys (including staged ones)
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::log_view::LogView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
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
            let mut keys = Vec::new();
            let mut positions = Vec::new();
            for (pos, index) in indices.into_iter().enumerate() {
                if index < self.stored_count {
                    let key = self.context.derive_tag_key(KeyTag::Index as u8, &index)?;
                    keys.push(key);
                    positions.push(pos);
                    result.push(None);
                } else {
                    result.push(self.new_values.get(index - self.stored_count).cloned());
                }
            }
            let values = self.context.read_multi_values(keys).await?;
            for (pos, value) in positions.into_iter().zip(values) {
                *result.get_mut(pos).unwrap() = value;
            }
        }
        Ok(result)
    }

    async fn read_context(&self, range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let count = range.len();
        let mut keys = Vec::with_capacity(count);
        for index in range {
            let key = self.context.derive_tag_key(KeyTag::Index as u8, &index)?;
            keys.push(key);
        }
        let mut values = Vec::with_capacity(count);
        for entry in self.context.read_multi_values(keys).await? {
            match entry {
                None => {
                    return Err(ViewError::MissingEntries);
                }
                Some(value) => values.push(value),
            }
        }
        Ok(values)
    }

    /// Reads the logged values in the given range (including staged ones).
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::log_view::LogView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
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
            self.stored_count
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

#[async_trait]
impl<C, T> HashableView<C> for LogView<C, T>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.hash().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = LOG_VIEW_HASH_RUNTIME.measure_latency();
        let elements = self.read(..).await?;
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bcs_bytes(&elements)?;
        Ok(hasher.finalize())
    }
}

/// Type wrapping `LogView` while memoizing the hash.
pub type HashedLogView<C, T> = WrappedHashableContainerView<C, LogView<C, T>, HasherOutput>;

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
        C: Send + Sync,
        T: serde::ser::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync,
    {
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
