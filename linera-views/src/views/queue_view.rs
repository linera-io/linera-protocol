// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{vec_deque::IterMut, VecDeque},
    ops::Range,
};

use allocative::Allocative;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::visit_allocative_simple;
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
    pub static QUEUE_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "queue_view_hash_runtime",
            "QueueView hash runtime",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });
}

/// Key tags to create the sub-keys of a `QueueView` on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the storing of the variable `stored_indices`.
    Store = MIN_VIEW_TAG,
    /// Prefix for the indices of the log.
    Index,
}

/// A view that supports a FIFO queue for values of type `T`.
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative")]
pub struct QueueView<C, T> {
    /// The view context.
    #[allocative(skip)]
    context: C,
    /// The range of indices for entries persisted in storage.
    #[allocative(visit = visit_allocative_simple)]
    stored_indices: Range<u32>,
    /// The number of entries to delete from the front.
    front_delete_count: u32,
    /// Whether to clear storage before applying updates.
    delete_storage_first: bool,
    /// New values added to the back, not yet persisted to storage.
    new_back_values: VecDeque<T>,
}

impl<C, T> View for QueueView<C, T>
where
    C: Context,
    T: Serialize + Send + Sync,
{
    const NUM_INIT_KEYS: usize = 1;

    type Context = C;

    fn context(&self) -> C {
        self.context.clone()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![context.base_key().base_tag(KeyTag::Store as u8)])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let stored_indices =
            from_bytes_option_or_default(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        Ok(Self {
            context,
            stored_indices,
            front_delete_count: 0,
            delete_storage_first: false,
            new_back_values: VecDeque::new(),
        })
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.front_delete_count = 0;
        self.new_back_values.clear();
    }

    async fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        if self.front_delete_count > 0 {
            return true;
        }
        !self.new_back_values.is_empty()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            batch.delete_key_prefix(self.context.base_key().bytes.clone());
            delete_view = true;
        }
        let mut new_stored_indices = self.stored_indices.clone();
        if self.stored_count() == 0 {
            let key_prefix = self.context.base_key().base_tag(KeyTag::Index as u8);
            batch.delete_key_prefix(key_prefix);
            new_stored_indices = Range::default();
        } else if self.front_delete_count > 0 {
            let deletion_range = self
                .stored_indices
                .clone()
                .take(self.front_delete_count as usize);
            new_stored_indices.start += self.front_delete_count;
            for index in deletion_range {
                let key = self
                    .context
                    .base_key()
                    .derive_tag_key(KeyTag::Index as u8, &index)?;
                batch.delete_key(key);
            }
        }
        if !self.new_back_values.is_empty() {
            delete_view = false;
            for value in &self.new_back_values {
                let key = self
                    .context
                    .base_key()
                    .derive_tag_key(KeyTag::Index as u8, &new_stored_indices.end)?;
                batch.put_key_value(key, value)?;
                new_stored_indices.end += 1;
            }
        }
        if !self.delete_storage_first || !new_stored_indices.is_empty() {
            let key = self.context.base_key().base_tag(KeyTag::Store as u8);
            batch.put_key_value(key, &new_stored_indices)?;
        }
        Ok(delete_view)
    }

    fn post_save(&mut self) {
        if self.stored_count() == 0 {
            self.stored_indices = Range::default();
        } else if self.front_delete_count > 0 {
            self.stored_indices.start += self.front_delete_count;
        }
        if !self.new_back_values.is_empty() {
            self.stored_indices.end += self.new_back_values.len() as u32;
            self.new_back_values.clear();
        }
        self.front_delete_count = 0;
        self.delete_storage_first = false;
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.new_back_values.clear();
    }
}

impl<C, T> ClonableView for QueueView<C, T>
where
    C: Context,
    T: Clone + Send + Sync + Serialize,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(QueueView {
            context: self.context.clone(),
            stored_indices: self.stored_indices.clone(),
            front_delete_count: self.front_delete_count,
            delete_storage_first: self.delete_storage_first,
            new_back_values: self.new_back_values.clone(),
        })
    }
}

impl<C, T> QueueView<C, T> {
    fn stored_count(&self) -> u32 {
        if self.delete_storage_first {
            0
        } else {
            (self.stored_indices.end - self.stored_indices.start) - self.front_delete_count
        }
    }
}

impl<'a, C, T> QueueView<C, T>
where
    C: Context,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    async fn get(&self, index: u32) -> Result<Option<T>, ViewError> {
        let key = self
            .context
            .base_key()
            .derive_tag_key(KeyTag::Index as u8, &index)?;
        Ok(self.context.store().read_value(&key).await?)
    }

    /// Reads the front value, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::queue_view::QueueView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = QueueView::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(42);
    /// assert_eq!(queue.front().await.unwrap(), Some(34));
    /// # })
    /// ```
    pub async fn front(&self) -> Result<Option<T>, ViewError> {
        let stored_remainder = self.stored_count();
        let value = if stored_remainder > 0 {
            self.get(self.stored_indices.end - stored_remainder).await?
        } else {
            self.new_back_values.front().cloned()
        };
        Ok(value)
    }

    /// Reads the back value, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::queue_view::QueueView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = QueueView::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(42);
    /// assert_eq!(queue.back().await.unwrap(), Some(42));
    /// # })
    /// ```
    pub async fn back(&self) -> Result<Option<T>, ViewError> {
        Ok(match self.new_back_values.back() {
            Some(value) => Some(value.clone()),
            None if self.stored_count() > 0 => self.get(self.stored_indices.end - 1).await?,
            _ => None,
        })
    }

    /// Deletes the front value, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::queue_view::QueueView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = QueueView::load(context).await.unwrap();
    /// queue.push_back(34 as u128);
    /// queue.delete_front();
    /// assert_eq!(queue.elements().await.unwrap(), Vec::<u128>::new());
    /// # })
    /// ```
    pub fn delete_front(&mut self) {
        if self.stored_count() > 0 {
            self.front_delete_count += 1;
        } else {
            self.new_back_values.pop_front();
        }
    }

    /// Pushes a value to the end of the queue.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::queue_view::QueueView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = QueueView::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(37);
    /// assert_eq!(queue.elements().await.unwrap(), vec![34, 37]);
    /// # })
    /// ```
    pub fn push_back(&mut self, value: T) {
        self.new_back_values.push_back(value);
    }

    /// Reads the size of the queue.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::queue_view::QueueView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = QueueView::load(context).await.unwrap();
    /// queue.push_back(34);
    /// assert_eq!(queue.count(), 1);
    /// # })
    /// ```
    pub fn count(&self) -> usize {
        self.stored_count() as usize + self.new_back_values.len()
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }

    async fn read_context(&self, range: Range<u32>) -> Result<Vec<T>, ViewError> {
        let count = (range.end - range.start) as usize;
        let mut keys = Vec::with_capacity(count);
        for index in range {
            let key = self
                .context
                .base_key()
                .derive_tag_key(KeyTag::Index as u8, &index)?;
            keys.push(key)
        }
        let mut values = Vec::with_capacity(count);
        for entry in self.context.store().read_multi_values(&keys).await? {
            match entry {
                None => {
                    return Err(ViewError::MissingEntries("QueueView".into()));
                }
                Some(value) => values.push(value),
            }
        }
        Ok(values)
    }

    /// Reads the `count` next values in the queue (including staged ones).
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::queue_view::QueueView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = QueueView::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(42);
    /// assert_eq!(queue.read_front(1).await.unwrap(), vec![34]);
    /// # })
    /// ```
    pub async fn read_front(&self, mut count: usize) -> Result<Vec<T>, ViewError> {
        if count > self.count() {
            count = self.count();
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut values = Vec::with_capacity(count);
        if !self.delete_storage_first {
            let stored_remainder = self.stored_count();
            let start = self.stored_indices.end - stored_remainder;
            if count as u32 <= stored_remainder {
                values.extend(self.read_context(start..(start + count as u32)).await?);
            } else {
                values.extend(self.read_context(start..self.stored_indices.end).await?);
                values.extend(
                    self.new_back_values
                        .range(0..(count - stored_remainder as usize))
                        .cloned(),
                );
            }
        } else {
            values.extend(self.new_back_values.range(0..count).cloned());
        }
        Ok(values)
    }

    /// Reads the `count` last values in the queue (including staged ones).
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::queue_view::QueueView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = QueueView::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(42);
    /// assert_eq!(queue.read_back(1).await.unwrap(), vec![42]);
    /// # })
    /// ```
    pub async fn read_back(&self, mut count: usize) -> Result<Vec<T>, ViewError> {
        if count > self.count() {
            count = self.count();
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut values = Vec::with_capacity(count);
        let new_back_len = self.new_back_values.len();
        if count <= new_back_len || self.delete_storage_first {
            values.extend(
                self.new_back_values
                    .range((new_back_len - count)..new_back_len)
                    .cloned(),
            );
        } else {
            let start = self.stored_indices.end + new_back_len as u32 - count as u32;
            values.extend(self.read_context(start..self.stored_indices.end).await?);
            values.extend(self.new_back_values.iter().cloned());
        }
        Ok(values)
    }

    /// Reads all the elements
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::queue_view::QueueView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = QueueView::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(37);
    /// assert_eq!(queue.elements().await.unwrap(), vec![34, 37]);
    /// # })
    /// ```
    pub async fn elements(&self) -> Result<Vec<T>, ViewError> {
        let count = self.count();
        self.read_front(count).await
    }

    async fn load_all(&mut self) -> Result<(), ViewError> {
        if !self.delete_storage_first {
            let stored_remainder = self.stored_count();
            let start = self.stored_indices.end - stored_remainder;
            let elements = self.read_context(start..self.stored_indices.end).await?;
            let shift = (self.stored_indices.end - start) as usize;
            for elt in elements {
                self.new_back_values.push_back(elt);
            }
            self.new_back_values.rotate_right(shift);
            // All indices are being deleted at the next flush. This is because they are deleted either:
            // * Because a self.front_delete_count forces them to be removed
            // * Or because loading them means that their value can be changed which invalidates
            //   the entries on storage
            self.delete_storage_first = true;
        }
        Ok(())
    }

    /// Gets a mutable iterator on the entries of the queue
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::queue_view::QueueView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = QueueView::load(context).await.unwrap();
    /// queue.push_back(34);
    /// let mut iter = queue.iter_mut().await.unwrap();
    /// let value = iter.next().unwrap();
    /// *value = 42;
    /// assert_eq!(queue.elements().await.unwrap(), vec![42]);
    /// # })
    /// ```
    pub async fn iter_mut(&'a mut self) -> Result<IterMut<'a, T>, ViewError> {
        self.load_all().await?;
        Ok(self.new_back_values.iter_mut())
    }
}

impl<C, T> HashableView for QueueView<C, T>
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
        let _hash_latency = metrics::QUEUE_VIEW_HASH_RUNTIME.measure_latency();
        let elements = self.elements().await?;
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bcs_bytes(&elements)?;
        Ok(hasher.finalize())
    }
}

/// Type wrapping `QueueView` while memoizing the hash.
pub type HashedQueueView<C, T> = WrappedHashableContainerView<C, QueueView<C, T>, HasherOutput>;

/// Wrapper around `QueueView` to compute hashes based on the history of changes.
pub type HistoricallyHashedQueueView<C, T> = HistoricallyHashableView<C, QueueView<C, T>>;

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::QueueView;
    use crate::{
        context::Context,
        graphql::{hash_name, mangle},
    };

    impl<C: Send + Sync, T: async_graphql::OutputType> async_graphql::TypeName for QueueView<C, T> {
        fn type_name() -> Cow<'static, str> {
            format!(
                "QueueView_{}_{:08x}",
                mangle(T::type_name()),
                hash_name::<T>()
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C: Context, T: async_graphql::OutputType> QueueView<C, T>
    where
        T: serde::ser::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync,
    {
        #[graphql(derived(name = "count"))]
        async fn count_(&self) -> Result<u32, async_graphql::Error> {
            Ok(self.count() as u32)
        }

        async fn entries(&self, count: Option<usize>) -> async_graphql::Result<Vec<T>> {
            Ok(self
                .read_front(count.unwrap_or_else(|| self.count()))
                .await?)
        }
    }
}
