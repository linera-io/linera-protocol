// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_metrics)]
use std::sync::LazyLock;
use std::{
    collections::{vec_deque::IterMut, VecDeque},
    fmt::Debug,
};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{self, MeasureLatency},
    prometheus::HistogramVec,
};

use crate::{
    batch::Batch,
    common::{from_bytes_option, from_bytes_option_or_default, HasherOutput, MIN_VIEW_TAG},
    context::Context,
    hashable_wrapper::WrappedHashableContainerView,
    views::{ClonableView, HashableView, Hasher, View, ViewError},
};

#[cfg(with_metrics)]
/// The runtime of hash computation
static BUCKET_QUEUE_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "bucket_queue_view_hash_runtime",
        "BucketQueueView hash runtime",
        &[],
        Some(vec![
            0.001, 0.003, 0.01, 0.03, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 2.0, 5.0,
        ]),
    )
    .expect("Histogram can be created")
});

/// Key tags to create the sub-keys of a [`BucketQueueView`] on top of the base key.
/// * The Front is special and directly accessible so that the
/// * The Store is where the structure of the buckets is stored.
/// * The Index is for storing the specific buckets.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the front of the view
    Front = MIN_VIEW_TAG,
    /// Prefix for [`StoredIndices`]
    Store,
    /// Prefix for the indices of the log.
    Index,
}

/// The `StoredIndices` contains the description of the stored buckets.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct StoredIndices {
    /// The stored buckets with the first index being the size at most N and the
    /// second one is the index. If the index is 0 then it correspond to the Front,
    /// otherwise to the Index.
    indices: Vec<(usize, usize)>,
    /// The position of the front in the first index.
    position: usize,
}

impl StoredIndices {
    fn len(&self) -> usize {
        self.indices.len()
    }
}

/// The `Cursor` is the current position of the front in the queue.
/// If position is None, then all the stored entries have been deleted
/// and the new_back_values are the ones accessed by the front operation.
/// If position is not trivial, then the first index is the relevant
/// bucket for the front and the second index is the position in the index.
#[derive(Debug, Clone)]
struct Cursor {
    position: Option<(usize, usize)>,
}

impl Cursor {
    pub fn new(number_bucket: usize, position: usize) -> Self {
        if number_bucket == 0 {
            Cursor { position: None }
        } else {
            Cursor {
                position: Some((0, position)),
            }
        }
    }

    pub fn is_incrementable(&self) -> bool {
        self.position.is_some()
    }
}

#[derive(Clone)]
enum Bucket<T> {
    Loaded {
        data: Vec<T>,
    },
    NotLoaded {
        length: usize,
    },
}

impl<T> Bucket<T> {
    fn len(&self) -> usize {
        match self {
            Bucket::Loaded { data } => data.len(),
            Bucket::NotLoaded { length } => *length
        }
    }

    fn is_loaded(&self) -> bool {
        match self {
            Bucket::Loaded { .. } => true,
            Bucket::NotLoaded { .. } => false,
        }
    }
}


fn stored_indices<T>(stored_data: &[(usize, Bucket<T>)], position: usize) -> StoredIndices {
    let indices = stored_data.iter().map(|(index, bucket)| (bucket.len(), *index)).collect::<Vec<_>>();
    StoredIndices { indices, position }
}


/// A view that supports a FIFO queue for values of type `T`.
pub struct BucketQueueView<C, T, const N: usize> {
    context: C,
    /// The buckets of stored data. If missing, then it has not been loaded. The first index is always loaded.
    stored_data: Vec<(usize,Bucket<T>)>,
    /// The newly inserted back values.
    new_back_values: VecDeque<T>,
    /// The stored position for the data
    stored_position: usize,
    /// The current position in the stored_data.
    cursor: Cursor,
    /// Whether the storage is deleted or not.
    delete_storage_first: bool,
}

#[async_trait]
impl<C, T, const N: usize> View<C> for BucketQueueView<C, T, N>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = 2;

    fn context(&self) -> &C {
        &self.context
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        let key1 = context.base_tag(KeyTag::Front as u8);
        let key2 = context.base_tag(KeyTag::Store as u8);
        Ok(vec![key1, key2])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let value1 = values.first().ok_or(ViewError::PostLoadValuesError)?;
        let value2 = values.get(1).ok_or(ViewError::PostLoadValuesError)?;
        let front = from_bytes_option::<Vec<T>, _>(value1)?;
        let mut stored_data = match front {
            Some(front) => {
                vec![(0, Bucket::Loaded { data: front })]
            }
            None => {
                vec![]
            }
        };
        let stored_indices = from_bytes_option_or_default::<StoredIndices, _>(value2)?;
        for i in 1..stored_indices.len() {
            let length = stored_indices.indices[i].0;
            let index = stored_indices.indices[i].1;
            stored_data.push((index, Bucket::NotLoaded { length }));
        }
        let cursor = Cursor::new(stored_indices.len(), stored_indices.position);
        Ok(Self {
            context,
            stored_data,
            stored_position: stored_indices.position,
            new_back_values: VecDeque::new(),
            cursor,
            delete_storage_first: false,
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let keys = Self::pre_load(&context)?;
        let values = context.read_multi_values_bytes(keys).await?;
        Self::post_load(context, &values)
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.cursor = Cursor::new(self.stored_data.len(), self.stored_position);
        self.new_back_values.clear();
    }

    async fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        if !self.stored_data.is_empty() {
            let Some((i_block, position)) = self.cursor.position else {
                return true;
            };
            if i_block != 0 || position != self.stored_position {
                return true;
            }
        }
        !self.new_back_values.is_empty()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            let key_prefix = self.context.base_key();
            batch.delete_key_prefix(key_prefix);
            delete_view = true;
        }
        if self.stored_count() == 0 {
            let key_prefix = self.context.base_key();
            batch.delete_key_prefix(key_prefix);
            self.stored_data.clear();
            self.stored_position = 0;
        } else if let Some((i_block, position)) = self.cursor.position {
            for block in 0..i_block {
                let index = self.stored_data[block].0;
                let key = self.get_index_key(index)?;
                batch.delete_key(key);
            }
            self.stored_data.drain(0..i_block);
            self.cursor = Cursor {
                position: Some((0, position)),
            };
            // We need to ensure that the first index is in the front.
            let first_index = self.stored_data[0].0;
            if first_index != 0 {
                self.stored_data[0].0 = 0;
                let key = self.get_index_key(first_index)?;
                batch.delete_key(key);
                let key = self.get_index_key(0)?;
                let (_, data0) = self.stored_data.first().unwrap();
                let Bucket::Loaded { data } = data0 else {
                    unreachable!();
                };
                batch.put_key_value(key, &data)?;
            }
        }
        if !self.new_back_values.is_empty() {
            delete_view = false;
            let mut unused_index = match self.stored_data.last() {
                Some((index, _)) => index + 1,
                None => 0,
            };
            let new_back_values: VecDeque<T> = std::mem::take(&mut self.new_back_values);
            let new_back_values: Vec<T> = new_back_values.into_iter().collect::<Vec<_>>();
            for value_chunk in new_back_values.chunks(N) {
                let key = self.get_index_key(unused_index)?;
                batch.put_key_value(key, &value_chunk)?;
                self.stored_data.push((unused_index, Bucket::Loaded { data: value_chunk.to_vec() }));
                unused_index += 1;
            }
            if !self.cursor.is_incrementable() {
                self.cursor = Cursor {
                    position: Some((0, 0)),
                }
            }
        }
        if !self.delete_storage_first || !self.stored_data.is_empty() {
            let stored_indices = stored_indices(&self.stored_data, self.stored_position);
            let key = self.context.base_tag(KeyTag::Store as u8);
            batch.put_key_value(key, &stored_indices)?;
        }
        self.delete_storage_first = false;
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.new_back_values.clear();
        self.cursor.position = None;
    }
}

impl<C, T, const N: usize> ClonableView<C> for BucketQueueView<C, T, N>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(BucketQueueView {
            context: self.context.clone(),
            stored_data: self.stored_data.clone(),
            new_back_values: self.new_back_values.clone(),
            stored_position: self.stored_position,
            cursor: self.cursor.clone(),
            delete_storage_first: self.delete_storage_first,
        })
    }
}

impl<C, T, const N: usize> BucketQueueView<C, T, N>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    /// Gets the key corresponding to the index
    fn get_index_key(&self, index: usize) -> Result<Vec<u8>, ViewError> {
        Ok(if index == 0 {
            self.context.base_tag(KeyTag::Front as u8)
        } else {
            self.context.derive_tag_key(KeyTag::Index as u8, &index)?
        })
    }

    /// Gets the number of entries in the container that are stored
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut queue = BucketQueueView::<_, u8, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// assert_eq!(queue.stored_count(), 0);
    /// # })
    /// ```
    pub fn stored_count(&self) -> usize {
        if self.delete_storage_first {
            0
        } else {
            let Some((i_block, position)) = self.cursor.position else {
                return 0;
            };
            let mut stored_count = 0;
            for block in i_block..self.stored_data.len() {
                stored_count += self.stored_data[block].1.len();
            }
            stored_count -= position;
            stored_count
        }
    }

    /// The total number of entries of the container
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut queue = BucketQueueView::<_, u8, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// assert_eq!(queue.count(), 1);
    /// # })
    /// ```
    pub fn count(&self) -> usize {
        self.stored_count() + self.new_back_values.len()
    }
}

impl<'a, C, T, const N: usize> BucketQueueView<C, T, N>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    /// Reads the front value, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut queue = BucketQueueView::<_, u8, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(42);
    /// assert_eq!(queue.front().cloned(), Some(34));
    /// # })
    /// ```
    pub fn front(&self) -> Option<&T> {
        match self.cursor.position {
            Some((i_block, position)) => {
                let block = &self.stored_data[i_block].1;
                let Bucket::Loaded { data } = block else {
                    unreachable!();
                };
                Some(&data[position])
            }
            None => self.new_back_values.front(),
        }
    }

    /// Deletes the front value, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut queue = BucketQueueView::<_, u128, 5>::load(context).await.unwrap();
    /// queue.push_back(34 as u128);
    /// queue.delete_front().await.unwrap();
    /// assert_eq!(queue.elements().await.unwrap(), Vec::<u128>::new());
    /// # })
    /// ```
    pub async fn delete_front(&mut self) -> Result<(), ViewError> {
        match self.cursor.position {
            Some((mut i_block, mut position)) => {
                position += 1;
                if self.stored_data[i_block].1.len() == position {
                    i_block += 1;
                    position = 0;
                }
                if i_block == self.stored_data.len() {
                    self.cursor = Cursor { position: None };
                } else {
                    self.cursor = Cursor {
                        position: Some((i_block, position)),
                    };
                    let (index, bucket) = self.stored_data.get_mut(i_block).unwrap();
                    let index = index.clone();
                    if !bucket.is_loaded() {
                        let key = self.get_index_key(index)?;
                        let value = self.context.read_value_bytes(&key).await?;
                        let value = value.ok_or(ViewError::MissingEntries)?;
                        let data = bcs::from_bytes(&value)?;
                        self.stored_data[i_block].1 = Bucket::Loaded { data };
                    }
                }
            }
            None => {
                self.new_back_values.pop_front();
            }
        }
        Ok(())
    }

    /// Pushes a value to the end of the queue.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut queue = BucketQueueView::<_, u128, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// assert_eq!(queue.elements().await.unwrap(), vec![34]);
    /// # })
    /// ```
    pub fn push_back(&mut self, value: T) {
        self.new_back_values.push_back(value);
    }

    /// Returns the list of elements in the queue.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut queue = BucketQueueView::<_, u128, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(37);
    /// assert_eq!(queue.elements().await.unwrap(), vec![34, 37]);
    /// # })
    /// ```
    pub async fn elements(&self) -> Result<Vec<T>, ViewError> {
        let count = self.count();
        self.read_context(self.cursor.position, count).await
    }

    /// Returns the last element of a bucket queue view
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut queue = BucketQueueView::<_, u128, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(37);
    /// assert_eq!(queue.back().await.unwrap(), Some(37));
    /// # })
    /// ```
    pub async fn back(&self) -> Result<Option<T>, ViewError> {
        if let Some(value) = self.new_back_values.back() {
            return Ok(Some(value.clone()));
        }
        if self.cursor.position.is_none() {
            return Ok(None);
        }
        let Some((index, bucket)) = self.stored_data.last() else {
            return Ok(None);
        };
        let len = bucket.len();
        if let Bucket::Loaded { data } = bucket {
            return Ok(Some(data.last().unwrap().clone()));
        };
        let key = self.get_index_key(*index)?;
        let value = self.context.read_value_bytes(&key).await?;
        let value = value.as_ref().ok_or(ViewError::MissingEntries)?;
        let value = bcs::from_bytes::<Vec<T>>(value)?;
        debug_assert_eq!(value.len(), len);
        Ok(Some(value[len - 1].clone()))
    }

    async fn read_context(
        &self,
        position: Option<(usize, usize)>,
        count: usize,
    ) -> Result<Vec<T>, ViewError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut elements = Vec::<T>::new();
        let mut count_remain = count;
        if let Some(pair) = position {
            let mut keys = Vec::new();
            let (i_block, mut position) = pair;
            for block in i_block..self.stored_data.len() {
                let (index, bucket) = &self.stored_data[block];
                let size = bucket.len() - position;
                if !bucket.is_loaded() {
                    let key = self.get_index_key(*index)?;
                    keys.push(key);
                };
                if size >= count_remain {
                    break;
                }
                count_remain -= size;
                position = 0;
            }
            let values = self.context.read_multi_values_bytes(keys).await?;
            position = pair.1;
            let mut pos = 0;
            count_remain = count;
            for block in i_block..self.stored_data.len() {
                let bucket = &self.stored_data[block].1;
                let size = bucket.len() - position;
                let vec = match bucket {
                    Bucket::Loaded { data } => data,
                    Bucket::NotLoaded { .. } => {
                        let value = values[pos].as_ref().ok_or(ViewError::MissingEntries)?;
                        pos += 1;
                        &bcs::from_bytes::<Vec<T>>(value)?
                    }
                };
                let end = if count_remain <= size {
                    position + count_remain
                } else {
                    position + size
                };
                for element in &vec[position..end] {
                    elements.push(element.clone());
                }
                if size >= count_remain {
                    count_remain = 0;
                    break;
                }
                count_remain -= size;
                position = 0;
            }
        }
        if count_remain > 0 {
            let count_read = std::cmp::min(count_remain, self.new_back_values.len());
            elements.extend(self.new_back_values.range(0..count_read).cloned());
        }
        Ok(elements)
    }

    /// Returns the first elements of a bucket queue view
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut queue = BucketQueueView::<_, u128, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(37);
    /// queue.push_back(47);
    /// assert_eq!(queue.read_front(2).await.unwrap(), vec![34, 37]);
    /// # })
    /// ```
    pub async fn read_front(&self, count: usize) -> Result<Vec<T>, ViewError> {
        self.read_context(self.cursor.position, count).await
    }

    /// Returns the last element of a bucket queue view
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut queue = BucketQueueView::<_, u128, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(37);
    /// queue.push_back(47);
    /// assert_eq!(queue.read_back(2).await.unwrap(), vec![37, 47]);
    /// # })
    /// ```
    pub async fn read_back(&self, count: usize) -> Result<Vec<T>, ViewError> {
        if count <= self.new_back_values.len() {
            let start = self.new_back_values.len() - count;
            Ok(self
                .new_back_values
                .range(start..)
                .cloned()
                .collect::<Vec<_>>())
        } else {
            let mut increment = self.count() - count;
            let Some((i_block, mut position)) = self.cursor.position else {
                unreachable!();
            };
            for block in i_block..self.stored_data.len() {
                let size = self.stored_data[block].1.len() - position;
                if increment < size {
                    return self
                        .read_context(Some((block, position + increment)), count)
                        .await;
                }
                increment -= size;
                position = 0;
            }
            unreachable!();
        }
    }

    async fn load_all(&mut self) -> Result<(), ViewError> {
        if !self.delete_storage_first {
            let elements = self.elements().await?;
            self.new_back_values.clear();
            for elt in elements {
                self.new_back_values.push_back(elt);
            }
            self.cursor = Cursor { position: None };
            self.delete_storage_first = true;
        }
        Ok(())
    }

    /// Gets a mutable iterator on the entries of the queue
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::create_test_memory_context;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use linera_views::views::View;
    /// # let context = create_test_memory_context();
    /// let mut queue = BucketQueueView::<_, u8, 5>::load(context).await.unwrap();
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

#[async_trait]
impl<C, T, const N: usize> HashableView<C> for BucketQueueView<C, T, N>
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
        let _hash_latency = BUCKET_QUEUE_VIEW_HASH_RUNTIME.measure_latency();
        let elements = self.elements().await?;
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bcs_bytes(&elements)?;
        Ok(hasher.finalize())
    }
}

/// Type wrapping `QueueView` while memoizing the hash.
pub type HashedBucketQueueView<C, T, const N: usize> =
    WrappedHashableContainerView<C, BucketQueueView<C, T, N>, HasherOutput>;

mod graphql {
    use std::borrow::Cow;

    use super::BucketQueueView;
    use crate::{
        context::Context,
        graphql::{hash_name, mangle},
    };

    impl<C: Send + Sync, T: async_graphql::OutputType, const N: usize> async_graphql::TypeName
        for BucketQueueView<C, T, N>
    {
        fn type_name() -> Cow<'static, str> {
            format!(
                "BucketQueueView_{}_{:08x}",
                mangle(T::type_name()),
                hash_name::<T>()
            )
            .into()
        }
    }

    #[async_graphql::Object(cache_control(no_cache), name_type)]
    impl<C: Context, T: async_graphql::OutputType, const N: usize> BucketQueueView<C, T, N>
    where
        C: Send + Sync,
        T: serde::ser::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync,
    {
        async fn entries(&self, count: Option<usize>) -> async_graphql::Result<Vec<T>> {
            Ok(self
                .read_front(count.unwrap_or_else(|| self.count()))
                .await?)
        }
    }
}
