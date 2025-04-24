// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_metrics)]
use std::sync::LazyLock;
use std::{
    collections::{BTreeSet, HashMap},
    ops::{Bound, Range, RangeBounds},
};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{
        exponential_bucket_latencies, register_histogram_vec, MeasureLatency,
    },
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
static BUCKET_LOG_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "bucket_log_view_hash_runtime",
        "BucketLogView hash runtime",
        &[],
        exponential_bucket_latencies(5.0),
    )
});

/// Key tags to create the sub-keys of a `LogView` on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the storing of the variable `stored_count`.
    StoredSizes = MIN_VIEW_TAG,
    /// Prefix for the indices of the log.
    Index,
}

/// The `StoredIndices` contains the description of the stored buckets.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct StoredSizes {
    /// The stored buckets sizes.
    sizes: Vec<usize>,
}

#[derive(Clone, Debug)]
enum Bucket<T> {
    Loaded { data: Vec<T> },
    NotLoaded { length: usize },
}

impl<T> Bucket<T> {
    fn len(&self) -> usize {
        match self {
            Bucket::Loaded { data } => data.len(),
            Bucket::NotLoaded { length } => *length,
        }
    }
}

/// A view that supports logging values of type `T`.
#[derive(Debug)]
pub struct BucketLogView<C, T, const N: usize> {
    context: C,
    delete_storage_first: bool,
    stored_data: Vec<Bucket<T>>,
    stored_count: usize,
    new_values: Vec<T>,
}

#[async_trait]
impl<C, T, const N: usize> View<C> for BucketLogView<C, T, N>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Clone + Send + Sync + Serialize,
{
    const NUM_INIT_KEYS: usize = 1;

    fn context(&self) -> &C {
        &self.context
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![context.base_tag(KeyTag::StoredSizes as u8)])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let value = values.first().ok_or(ViewError::PostLoadValuesError)?;
        let stored_sizes = from_bytes_option_or_default::<StoredSizes, _>(value)?;
        let stored_count = stored_sizes.sizes.iter().sum();
        let stored_data = stored_sizes
            .sizes
            .into_iter()
            .map(|length| Bucket::NotLoaded { length })
            .collect::<Vec<_>>();
        Ok(Self {
            context,
            delete_storage_first: false,
            stored_data,
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
            self.stored_data.clear();
            self.stored_count = 0;
            delete_view = true;
        }
        if !self.new_values.is_empty() {
            let mut i_block = self.stored_data.len();
            self.stored_count += self.new_values.len();
            let new_values = std::mem::take(&mut self.new_values);
            for value_chunk in new_values.chunks(N) {
                let key = self.context.derive_tag_key(KeyTag::Index as u8, &i_block)?;
                batch.put_key_value(key, &value_chunk)?;
                self.stored_data.push(Bucket::Loaded {
                    data: value_chunk.to_vec(),
                });
                i_block += 1;
            }
            delete_view = false;
            self.new_values.clear();
        }
        if !self.delete_storage_first || !self.stored_data.is_empty() {
            let stored_sizes = self
                .stored_data
                .iter()
                .map(|bucket| bucket.len())
                .collect::<Vec<_>>();
            let key = self.context.base_tag(KeyTag::StoredSizes as u8);
            batch.put_key_value(key, &stored_sizes)?;
        }
        self.delete_storage_first = false;
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.new_values.clear();
    }
}

impl<C, T, const N: usize> ClonableView<C> for BucketLogView<C, T, N>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Clone + Send + Sync + Serialize,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(BucketLogView {
            context: self.context.clone(),
            delete_storage_first: self.delete_storage_first,
            stored_data: self.stored_data.clone(),
            stored_count: self.stored_count,
            new_values: self.new_values.clone(),
        })
    }
}

impl<C, T, const N: usize> BucketLogView<C, T, N>
where
    C: Context,
{
    /// Pushes a value to the end of the log.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_log_view::BucketLogView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut log = BucketLogView::<_, u8, 5>::load(context).await.unwrap();
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
    /// # use linera_views::bucket_log_view::BucketLogView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut log = BucketLogView::<_, u8, 5>::load(context).await.unwrap();
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

impl<C, T, const N: usize> BucketLogView<C, T, N>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Clone + DeserializeOwned + Serialize + Send,
{
    /// Returns the corresponding bucket and the corresponding position
    fn get_position(&self, mut index: usize) -> Option<(usize, usize)> {
        for i_bucket in 0..self.stored_data.len() {
            let size = self.stored_data[i_bucket].len();
            if index < size {
                return Some((i_bucket, index));
            }
            index -= size;
        }
        None
    }

    /// Reads the logged value with the given index (including staged ones).
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_log_view::BucketLogView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut log = BucketLogView::<_, u8, 5>::load(context).await.unwrap();
    /// log.push(34);
    /// assert_eq!(log.get(0).await.unwrap(), Some(34));
    /// # })
    /// ```
    pub async fn get(&self, index: usize) -> Result<Option<T>, ViewError> {
        let value = if self.delete_storage_first {
            self.new_values.get(index).cloned()
        } else if index < self.stored_count {
            let (i_bucket, position) = self.get_position(index).unwrap();
            if let Bucket::Loaded { data } = self.stored_data.get(i_bucket).unwrap() {
                return Ok(Some(data[position].clone()));
            }
            let key = self
                .context
                .derive_tag_key(KeyTag::Index as u8, &i_bucket)?;
            let bucket = self.context.read_value::<Vec<T>>(&key).await?.unwrap();
            Some(bucket[position].clone())
        } else {
            self.new_values.get(index - self.stored_count).cloned()
        };
        Ok(value)
    }

    /// Reads several logged keys (including staged ones)
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_log_view::BucketLogView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut log = BucketLogView::<_, u8, 5>::load(context).await.unwrap();
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
            let mut infos = Vec::new();
            let mut set_bucket = BTreeSet::new();
            for (pos, index) in indices.into_iter().enumerate() {
                if index < self.stored_count {
                    let (i_bucket, position) = self.get_position(index).unwrap();
                    match self.stored_data.get(i_bucket).unwrap() {
                        Bucket::Loaded { data } => {
                            result.push(Some(data[position].clone()));
                        }
                        Bucket::NotLoaded { length: _ } => {
                            set_bucket.insert(i_bucket);
                            infos.push((pos, i_bucket, position));
                            result.push(None);
                        }
                    }
                } else {
                    result.push(self.new_values.get(index - self.stored_count).cloned());
                }
            }
            let mut keys = Vec::new();
            let mut map_bucket = HashMap::new();
            for (pos_bucket, i_bucket) in set_bucket.into_iter().enumerate() {
                keys.push(
                    self.context
                        .derive_tag_key(KeyTag::Index as u8, &i_bucket)?,
                );
                map_bucket.insert(i_bucket, pos_bucket);
            }
            let values = self.context.read_multi_values::<Vec<T>>(keys).await?;
            for (pos, i_bucket, position) in infos {
                let pos_bucket = map_bucket.get(&i_bucket).unwrap();
                let bucket = values[*pos_bucket].as_ref().unwrap();
                *result.get_mut(pos).unwrap() = Some(bucket[position].clone());
            }
        }
        Ok(result)
    }

    async fn read_context(&self, range: Range<usize>) -> Result<Vec<T>, ViewError> {
        let (i_bucket_start, position_start) = self.get_position(range.start).unwrap();
        let (i_bucket_end, position_end) = self.get_position(range.end).unwrap();

        let mut keys = Vec::new();
        let mut symbols = Vec::new();
        let mut i_key = 0;
        for i_bucket in i_bucket_start..=i_bucket_end {
            let start = if i_bucket == i_bucket_start {
                position_start
            } else {
                0
            };
            let end = if i_bucket == i_bucket_end {
                position_end
            } else {
                self.stored_data[i_bucket].len()
            };
            let range = Range { start, end };
            let access = match self.stored_data[i_bucket] {
                Bucket::Loaded { data: _ } => None,
                Bucket::NotLoaded { length: _ } => {
                    let key = self.context
                        .derive_tag_key(KeyTag::Index as u8, &i_bucket)?;
                    keys.push(key);
                    i_key += 1;
                    Some(i_key - 1)
                },
            };
            symbols.push((i_bucket, range, access));
        }
        let values = self.context.read_multi_values::<Vec<T>>(keys).await?;
        let mut returned_values: Vec<T> = Vec::new();
        for (i_bucket, range, access) in symbols {
            let vec: Vec<T> = match access {
                None => {
                    let Bucket::Loaded { data } = self.stored_data.get(i_bucket).unwrap() else {
                        unreachable!("The data should be loaded");
                    };
                    data[range].to_vec()
                },
                Some(i_key) => {
                    values.get(i_key).unwrap().clone().unwrap()[range].to_vec()
                },
            };
            returned_values.extend(vec);
        }
        Ok(returned_values)
    }

    /// Reads the logged values in the given range (including staged ones).
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_log_view::BucketLogView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut log = BucketLogView::<_, u8, 5>::load(context).await.unwrap();
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

    /// Reads all the elements of the bucket log view
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_log_view::BucketLogView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut log = BucketLogView::<_, u8, 5>::load(context).await.unwrap();
    /// log.push(34);
    /// log.push(42);
    /// log.push(56);
    /// assert_eq!(log.elements().await.unwrap(), vec![34, 42, 56]);
    /// # })
    /// ```
    pub async fn elements(&self) -> Result<Vec<T>, ViewError> {
        self.read(..).await
    }
}

#[async_trait]
impl<C, T, const N: usize> HashableView<C> for BucketLogView<C, T, N>
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
        let _hash_latency = BUCKET_LOG_VIEW_HASH_RUNTIME.measure_latency();
        let elements = self.read(..).await?;
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bcs_bytes(&elements)?;
        Ok(hasher.finalize())
    }
}

/// Type wrapping `LogView` while memoizing the hash.
pub type HashedBucketLogView<C, T, const N: usize> =
    WrappedHashableContainerView<C, BucketLogView<C, T, N>, HasherOutput>;

mod graphql {
    use std::borrow::Cow;

    use super::BucketLogView;
    use crate::{
        context::Context,
        graphql::{hash_name, mangle},
    };

    impl<C: Send + Sync, T: async_graphql::OutputType, const N: usize> async_graphql::TypeName
        for BucketLogView<C, T, N>
    {
        fn type_name() -> Cow<'static, str> {
            format!(
                "BucketLogView_{}_{:08x}",
                mangle(T::type_name()),
                hash_name::<T>()
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C: Context, T: async_graphql::OutputType, const N: usize> BucketLogView<C, T, N>
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
