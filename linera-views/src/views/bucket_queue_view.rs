// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{vec_deque::IterMut, VecDeque};

use allocative::Allocative;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    batch::Batch,
    common::{from_bytes_option, from_bytes_option_or_default, HasherOutput},
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
    pub static BUCKET_QUEUE_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "bucket_queue_view_hash_runtime",
            "BucketQueueView hash runtime",
            &[],
            exponential_bucket_latencies(5.0),
        )
    });
}

/// Key tags to create the sub-keys of a [`BucketQueueView`] on top of the base key.
/// * The Front is special and downloaded at the view loading.
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

/// The description of the stored buckets.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct StoredIndices {
    /// The description of each stored bucket.
    indices: Vec<StoredIndex>,
    /// The position of the front value in the first stored bucket.
    front_position: usize,
}

/// The description of a stored bucket.
#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize)]
struct StoredIndex {
    /// The length of the bucket (at most N).
    length: usize,
    /// The index of the bucket in storage. If the index is 0 then it corresponds
    /// to the first key (entry `KeyTag::Front`), otherwise to the keys with
    /// prefix `KeyTag::Index`.
    index: usize,
}

impl StoredIndices {
    fn new<T>(stored_data: &VecDeque<(usize, Bucket<T>)>, position: usize) -> Self {
        let indices = stored_data
            .iter()
            .map(|(index, bucket)| StoredIndex {
                length: bucket.len(),
                index: *index,
            })
            .collect::<Vec<_>>();
        Self {
            indices,
            front_position: position,
        }
    }

    fn len(&self) -> usize {
        self.indices.len()
    }
}

/// The current position of the front in the queue. If position is `None`, then all the
/// stored entries have been deleted and the `new_back_values` are the ones accessed by
/// the front operation. If position is not trivial, then the first index is the relevant
/// bucket for the front and the second index is the position in the index.
#[derive(Copy, Clone, Debug, Allocative)]
struct Cursor {
    /// The front bucket.
    i_block: usize,
    /// The position in the index.
    position: usize,
}

#[derive(Clone, Debug, Allocative)]
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

    fn is_loaded(&self) -> bool {
        match self {
            Bucket::Loaded { .. } => true,
            Bucket::NotLoaded { .. } => false,
        }
    }
}

/// A view that supports a FIFO queue for values of type `T`.
/// The size `N` has to be chosen by taking into account the size of the type `T`
/// and the basic size of a block. For example a total size of 100 bytes to 10 KB
/// seems adequate.
//#[allocative(bound = "T: Allocative")]
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative, const N: usize")]
pub struct BucketQueueView<C, T, const N: usize> {
    /// The view context.
    #[allocative(skip)]
    context: C,
    /// The stored buckets. Some buckets may not be loaded. The first one is always loaded.
    stored_data: VecDeque<(usize, Bucket<T>)>,
    /// The newly inserted back values.
    new_back_values: VecDeque<T>,
    /// The position for the stored front value in the first stored bucket.
    stored_front_position: usize,
    /// The current position of the front value if it is in the stored buckets, and `None`
    /// otherwise.
    cursor: Option<Cursor>,
    /// Whether the storage is to be deleted or not.
    delete_storage_first: bool,
}

impl<C, T, const N: usize> View for BucketQueueView<C, T, N>
where
    C: Context,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = 2;

    type Context = C;

    fn context(&self) -> &C {
        &self.context
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        let key1 = context.base_key().base_tag(KeyTag::Front as u8);
        let key2 = context.base_key().base_tag(KeyTag::Store as u8);
        Ok(vec![key1, key2])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let value1 = values.first().ok_or(ViewError::PostLoadValuesError)?;
        let value2 = values.get(1).ok_or(ViewError::PostLoadValuesError)?;
        let front = from_bytes_option::<Vec<T>>(value1)?;
        let mut stored_data = VecDeque::from(match front {
            Some(front) => {
                vec![(0, Bucket::Loaded { data: front })]
            }
            None => {
                vec![]
            }
        });
        let stored_indices = from_bytes_option_or_default::<StoredIndices>(value2)?;
        for i in 1..stored_indices.len() {
            let length = stored_indices.indices[i].length;
            let index = stored_indices.indices[i].index;
            stored_data.push_back((index, Bucket::NotLoaded { length }));
        }
        let cursor = if stored_indices.indices.is_empty() {
            None
        } else {
            Some(Cursor {
                i_block: 0,
                position: stored_indices.front_position,
            })
        };
        Ok(Self {
            context,
            stored_data,
            stored_front_position: stored_indices.front_position,
            new_back_values: VecDeque::new(),
            cursor,
            delete_storage_first: false,
        })
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.cursor = if self.stored_data.is_empty() {
            None
        } else {
            Some(Cursor {
                i_block: 0,
                position: self.stored_front_position,
            })
        };
        self.new_back_values.clear();
    }

    async fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        if !self.stored_data.is_empty() {
            let Some(Cursor { i_block, position }) = self.cursor else {
                return true;
            };
            if i_block != 0 || position != self.stored_front_position {
                return true;
            }
        }
        !self.new_back_values.is_empty()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        if self.delete_storage_first {
            let key_prefix = self.context.base_key().bytes.clone();
            batch.delete_key_prefix(key_prefix);
            delete_view = true;
        }
        let mut temp_stored_data = self.stored_data.clone();
        let mut temp_stored_position = self.stored_front_position;
        if self.stored_count() == 0 {
            let key_prefix = self.context.base_key().bytes.clone();
            batch.delete_key_prefix(key_prefix);
            temp_stored_data.clear();
            temp_stored_position = 0;
        } else if let Some(Cursor { i_block, position }) = self.cursor {
            for _ in 0..i_block {
                let block = temp_stored_data.pop_front().unwrap();
                let index = block.0;
                let key = self.get_index_key(index)?;
                batch.delete_key(key);
            }
            temp_stored_position = position;
            // We need to ensure that the first index is in the front.
            let first_index = temp_stored_data[0].0;
            if first_index != 0 {
                temp_stored_data[0].0 = 0;
                let key = self.get_index_key(first_index)?;
                batch.delete_key(key);
                let key = self.get_index_key(0)?;
                let (_, data0) = temp_stored_data.front().unwrap();
                let Bucket::Loaded { data } = data0 else {
                    unreachable!();
                };
                batch.put_key_value(key, &data)?;
            }
        }
        if !self.new_back_values.is_empty() {
            delete_view = false;
            let mut unused_index = match temp_stored_data.back() {
                Some((index, _)) => index + 1,
                None => 0,
            };
            let new_back_values = self.new_back_values.iter().cloned().collect::<Vec<_>>();
            for value_chunk in new_back_values.chunks(N) {
                let key = self.get_index_key(unused_index)?;
                batch.put_key_value(key, &value_chunk)?;
                temp_stored_data.push_back((
                    unused_index,
                    Bucket::Loaded {
                        data: value_chunk.to_vec(),
                    },
                ));
                unused_index += 1;
            }
        }
        if !self.delete_storage_first || !temp_stored_data.is_empty() {
            let stored_indices = StoredIndices::new(&temp_stored_data, temp_stored_position);
            let key = self.context.base_key().base_tag(KeyTag::Store as u8);
            batch.put_key_value(key, &stored_indices)?;
        }
        Ok(delete_view)
    }

    fn post_save(&mut self) {
        if self.stored_count() == 0 {
            self.stored_data.clear();
            self.stored_front_position = 0;
        } else if let Some(Cursor { i_block, position }) = self.cursor {
            for _ in 0..i_block {
                self.stored_data.pop_front();
            }
            self.cursor = Some(Cursor {
                i_block: 0,
                position,
            });
            self.stored_front_position = position;
            // We need to ensure that the first index is in the front.
            let first_index = self.stored_data[0].0;
            if first_index != 0 {
                self.stored_data[0].0 = 0;
            }
        }
        if !self.new_back_values.is_empty() {
            let mut unused_index = match self.stored_data.back() {
                Some((index, _)) => index + 1,
                None => 0,
            };
            let new_back_values = std::mem::take(&mut self.new_back_values);
            let new_back_values = new_back_values.into_iter().collect::<Vec<_>>();
            for value_chunk in new_back_values.chunks(N) {
                self.stored_data.push_back((
                    unused_index,
                    Bucket::Loaded {
                        data: value_chunk.to_vec(),
                    },
                ));
                unused_index += 1;
            }
            if self.cursor.is_none() {
                self.cursor = Some(Cursor {
                    i_block: 0,
                    position: 0,
                });
            }
        }
        self.delete_storage_first = false;
    }

    fn clear(&mut self) {
        self.delete_storage_first = true;
        self.new_back_values.clear();
        self.cursor = None;
    }
}

impl<C: Clone, T: Clone, const N: usize> ClonableView for BucketQueueView<C, T, N>
where
    Self: View,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(BucketQueueView {
            context: self.context.clone(),
            stored_data: self.stored_data.clone(),
            new_back_values: self.new_back_values.clone(),
            stored_front_position: self.stored_front_position,
            cursor: self.cursor,
            delete_storage_first: self.delete_storage_first,
        })
    }
}

impl<C: Context, T, const N: usize> BucketQueueView<C, T, N> {
    /// Gets the key corresponding to the index
    fn get_index_key(&self, index: usize) -> Result<Vec<u8>, ViewError> {
        Ok(if index == 0 {
            self.context.base_key().base_tag(KeyTag::Front as u8)
        } else {
            self.context
                .base_key()
                .derive_tag_key(KeyTag::Index as u8, &index)?
        })
    }

    /// Gets the number of entries in the container that are stored
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = BucketQueueView::<_, u8, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// assert_eq!(queue.stored_count(), 0);
    /// # })
    /// ```
    pub fn stored_count(&self) -> usize {
        if self.delete_storage_first {
            0
        } else {
            let Some(Cursor { i_block, position }) = self.cursor else {
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = BucketQueueView::<_, u8, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// assert_eq!(queue.count(), 1);
    /// # })
    /// ```
    pub fn count(&self) -> usize {
        self.stored_count() + self.new_back_values.len()
    }
}

impl<C: Context, T: DeserializeOwned + Clone, const N: usize> BucketQueueView<C, T, N> {
    /// Gets a reference on the front value if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = BucketQueueView::<_, u8, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(42);
    /// assert_eq!(queue.front().cloned(), Some(34));
    /// # })
    /// ```
    pub fn front(&self) -> Option<&T> {
        match self.cursor {
            Some(Cursor { i_block, position }) => {
                let block = &self.stored_data[i_block].1;
                let Bucket::Loaded { data } = block else {
                    unreachable!();
                };
                Some(&data[position])
            }
            None => self.new_back_values.front(),
        }
    }

    /// Reads the front value, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = BucketQueueView::<_, u8, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(42);
    /// let front = queue.front_mut().unwrap();
    /// *front = 43;
    /// assert_eq!(queue.front().cloned(), Some(43));
    /// # })
    /// ```
    pub fn front_mut(&mut self) -> Option<&mut T> {
        match self.cursor {
            Some(Cursor { i_block, position }) => {
                let block = &mut self.stored_data.get_mut(i_block).unwrap().1;
                let Bucket::Loaded { data } = block else {
                    unreachable!();
                };
                Some(data.get_mut(position).unwrap())
            }
            None => self.new_back_values.front_mut(),
        }
    }

    /// Deletes the front value, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = BucketQueueView::<_, u128, 5>::load(context).await.unwrap();
    /// queue.push_back(34 as u128);
    /// queue.delete_front().await.unwrap();
    /// assert_eq!(queue.elements().await.unwrap(), Vec::<u128>::new());
    /// # })
    /// ```
    pub async fn delete_front(&mut self) -> Result<(), ViewError> {
        match self.cursor {
            Some(Cursor {
                mut i_block,
                mut position,
            }) => {
                position += 1;
                if self.stored_data[i_block].1.len() == position {
                    i_block += 1;
                    position = 0;
                }
                if i_block == self.stored_data.len() {
                    self.cursor = None;
                } else {
                    self.cursor = Some(Cursor { i_block, position });
                    let (index, bucket) = self.stored_data.get_mut(i_block).unwrap();
                    let index = *index;
                    if !bucket.is_loaded() {
                        let key = self.get_index_key(index)?;
                        let data = self.context.store().read_value(&key).await?;
                        let data = match data {
                            Some(value) => value,
                            None => {
                                let root_key = self.context.store().root_key()?;
                                return Err(ViewError::MissingEntries(root_key));
                            }
                        };
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
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
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = BucketQueueView::<_, u128, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(37);
    /// assert_eq!(queue.elements().await.unwrap(), vec![34, 37]);
    /// # })
    /// ```
    pub async fn elements(&self) -> Result<Vec<T>, ViewError> {
        let count = self.count();
        self.read_context(self.cursor, count).await
    }

    /// Returns the last element of a bucket queue view
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = BucketQueueView::<_, u128, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(37);
    /// assert_eq!(queue.back().await.unwrap(), Some(37));
    /// # })
    /// ```
    pub async fn back(&mut self) -> Result<Option<T>, ViewError>
    where
        T: Clone,
    {
        if let Some(value) = self.new_back_values.back() {
            return Ok(Some(value.clone()));
        }
        if self.cursor.is_none() {
            return Ok(None);
        }
        let Some((index, bucket)) = self.stored_data.back() else {
            return Ok(None);
        };
        if !bucket.is_loaded() {
            let key = self.get_index_key(*index)?;
            let data = self.context.store().read_value(&key).await?;
            let data = match data {
                Some(data) => data,
                None => {
                    let root_key = self.context.store().root_key()?;
                    return Err(ViewError::MissingEntries(root_key));
                }
            };
            self.stored_data.back_mut().unwrap().1 = Bucket::Loaded { data };
        }
        let bucket = &self.stored_data.back_mut().unwrap().1;
        let Bucket::Loaded { data } = bucket else {
            unreachable!();
        };
        Ok(Some(data.last().unwrap().clone()))
    }

    async fn read_context(
        &self,
        cursor: Option<Cursor>,
        count: usize,
    ) -> Result<Vec<T>, ViewError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut elements = Vec::<T>::new();
        let mut count_remain = count;
        if let Some(Cursor { i_block, position }) = cursor {
            let mut keys = Vec::new();
            let mut next_position = position;
            for block in i_block..self.stored_data.len() {
                let (index, bucket) = &self.stored_data[block];
                let size = bucket.len() - next_position;
                if !bucket.is_loaded() {
                    let key = self.get_index_key(*index)?;
                    keys.push(key);
                };
                if size >= count_remain {
                    break;
                }
                count_remain -= size;
                next_position = 0;
            }
            let values = self.context.store().read_multi_values_bytes(&keys).await?;
            let mut value_pos = 0;
            count_remain = count;
            let mut next_position = position;
            for block in i_block..self.stored_data.len() {
                let bucket = &self.stored_data[block].1;
                let size = bucket.len() - position;
                let vec = match bucket {
                    Bucket::Loaded { data } => data,
                    Bucket::NotLoaded { .. } => {
                        let value = match &values[value_pos] {
                            Some(value) => value,
                            None => {
                                let root_key = self.context.store().root_key()?;
                                return Err(ViewError::MissingEntries(root_key));
                            }
                        };
                        value_pos += 1;
                        &bcs::from_bytes::<Vec<T>>(value)?
                    }
                };
                elements.extend(vec[next_position..].iter().take(count_remain).cloned());
                if size >= count_remain {
                    return Ok(elements);
                }
                count_remain -= size;
                next_position = 0;
            }
        }
        let count_read = std::cmp::min(count_remain, self.new_back_values.len());
        elements.extend(self.new_back_values.range(0..count_read).cloned());
        Ok(elements)
    }

    /// Returns the first elements of a bucket queue view
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = BucketQueueView::<_, u128, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(37);
    /// queue.push_back(47);
    /// assert_eq!(queue.read_front(2).await.unwrap(), vec![34, 37]);
    /// # })
    /// ```
    pub async fn read_front(&self, count: usize) -> Result<Vec<T>, ViewError> {
        let count = std::cmp::min(count, self.count());
        self.read_context(self.cursor, count).await
    }

    /// Returns the last element of a bucket queue view
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use crate::linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = BucketQueueView::<_, u128, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// queue.push_back(37);
    /// queue.push_back(47);
    /// assert_eq!(queue.read_back(2).await.unwrap(), vec![37, 47]);
    /// # })
    /// ```
    pub async fn read_back(&self, count: usize) -> Result<Vec<T>, ViewError> {
        let count = std::cmp::min(count, self.count());
        if count <= self.new_back_values.len() {
            let start = self.new_back_values.len() - count;
            Ok(self
                .new_back_values
                .range(start..)
                .cloned()
                .collect::<Vec<_>>())
        } else {
            let mut increment = self.count() - count;
            let Some(Cursor {
                i_block,
                mut position,
            }) = self.cursor
            else {
                unreachable!();
            };
            for block in i_block..self.stored_data.len() {
                let size = self.stored_data[block].1.len() - position;
                if increment < size {
                    return self
                        .read_context(
                            Some(Cursor {
                                i_block: block,
                                position: position + increment,
                            }),
                            count,
                        )
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
            self.cursor = None;
            self.delete_storage_first = true;
        }
        Ok(())
    }

    /// Gets a mutable iterator on the entries of the queue
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::context::MemoryContext;
    /// # use linera_views::bucket_queue_view::BucketQueueView;
    /// # use linera_views::views::View;
    /// # let context = MemoryContext::new_for_testing(());
    /// let mut queue = BucketQueueView::<_, u8, 5>::load(context).await.unwrap();
    /// queue.push_back(34);
    /// let mut iter = queue.iter_mut().await.unwrap();
    /// let value = iter.next().unwrap();
    /// *value = 42;
    /// assert_eq!(queue.elements().await.unwrap(), vec![42]);
    /// # })
    /// ```
    pub async fn iter_mut(&mut self) -> Result<IterMut<'_, T>, ViewError> {
        self.load_all().await?;
        Ok(self.new_back_values.iter_mut())
    }
}

impl<C: Context, T: Serialize + DeserializeOwned + Send + Sync + Clone, const N: usize> HashableView
    for BucketQueueView<C, T, N>
where
    Self: View,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.hash().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = metrics::BUCKET_QUEUE_VIEW_HASH_RUNTIME.measure_latency();
        let elements = self.elements().await?;
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bcs_bytes(&elements)?;
        Ok(hasher.finalize())
    }
}

/// Type wrapping `QueueView` while memoizing the hash.
pub type HashedBucketQueueView<C, T, const N: usize> =
    WrappedHashableContainerView<C, BucketQueueView<C, T, N>, HasherOutput>;

/// Wrapper around `BucketQueueView` to compute hashes based on the history of changes.
pub type HistoricallyHashedBucketQueueView<C, T, const N: usize> =
    HistoricallyHashableView<C, BucketQueueView<C, T, N>>;

#[cfg(with_graphql)]
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
