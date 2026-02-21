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
#[repr(u8)]
enum KeyTag {
    /// Key tag for the front bucket (index 0).
    Front = MIN_VIEW_TAG,
    /// Key tag for the `BucketStore`.
    Store,
    /// Key tag for the content of non-front buckets (index > 0).
    Index,
}

/// The metadata of the view in storage.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct BucketStore {
    /// The descriptions of all stored buckets. The first description is expected to start
    /// with index 0 (front bucket) and will be ignored.
    descriptions: Vec<BucketDescription>,
    /// The position of the front value in the front bucket.
    front_position: u32,
}

/// The description of a bucket in storage.
#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize)]
struct BucketDescription {
    /// The length of the bucket (at most N).
    length: u32,
    /// The index of the bucket in storage.
    index: u32,
}

impl BucketStore {
    fn len(&self) -> usize {
        self.descriptions.len()
    }
}

/// The position of a value in the stored buckket.
#[derive(Copy, Clone, Debug, Allocative)]
struct Cursor {
    /// The offset of the bucket in the vector of stored buckets.
    offset: u32,
    /// The position of the value in the stored bucket.
    position: u32,
}

/// The state of a stored bucket in memory.
#[derive(Clone, Debug, Allocative)]
enum State<T> {
    Loaded { data: Vec<T> },
    NotLoaded { length: u32 },
}

impl<T> Bucket<T> {
    fn len(&self) -> u32 {
        match &self.state {
            State::Loaded { data } => data.len() as u32,
            State::NotLoaded { length } => *length,
        }
    }

    fn is_loaded(&self) -> bool {
        match self.state {
            State::Loaded { .. } => true,
            State::NotLoaded { .. } => false,
        }
    }

    fn to_description(&self) -> BucketDescription {
        BucketDescription {
            length: self.len(),
            index: self.index,
        }
    }
}

/// A stored bucket.
#[derive(Clone, Debug, Allocative)]
struct Bucket<T> {
    /// The index in storage.
    index: u32,
    /// The state of the bucket.
    state: State<T>,
}

/// A view that supports a FIFO queue for values of type `T`.
/// The size `N` has to be chosen by taking into account the size of the type `T`
/// and the basic size of a block. For example a total size of 100 bytes to 10 KB
/// seems adequate.
//#[allocative(bound = "T: Allocative")]
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative, const N: u32")]
pub struct BucketQueueView<C, T, const N: u32> {
    /// The view context.
    #[allocative(skip)]
    context: C,
    /// The stored buckets. Some buckets may not be loaded. The first one is always loaded.
    stored_buckets: VecDeque<Bucket<T>>,
    /// The newly inserted back values.
    new_back_values: VecDeque<T>,
    /// The position for the stored front value in the first stored bucket.
    stored_front_position: u32,
    /// The current position of the front value if it is in the stored buckets, and `None`
    /// otherwise.
    cursor: Option<Cursor>,
    /// Whether the storage is to be deleted or not.
    delete_storage_first: bool,
}

impl<C, T, const N: u32> View for BucketQueueView<C, T, N>
where
    C: Context,
    T: Send + Sync + Clone + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = 2;

    type Context = C;

    fn context(&self) -> C {
        self.context.clone()
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
        let mut stored_buckets = VecDeque::from(match front {
            Some(data) => {
                let bucket = Bucket {
                    index: 0,
                    state: State::Loaded { data },
                };
                vec![bucket]
            }
            None => {
                vec![]
            }
        });
        let bucket_store = from_bytes_option_or_default::<BucketStore>(value2)?;
        // Ignoring `bucket_store.descriptions[0]`.
        // TODO(#4969): Remove redundant BucketDescription in BucketQueueView.
        for i in 1..bucket_store.len() {
            let length = bucket_store.descriptions[i].length;
            let index = bucket_store.descriptions[i].index;
            stored_buckets.push_back(Bucket {
                index,
                state: State::NotLoaded { length },
            });
        }
        let cursor = if bucket_store.descriptions.is_empty() {
            None
        } else {
            Some(Cursor {
                offset: 0,
                position: bucket_store.front_position,
            })
        };
        Ok(Self {
            context,
            stored_buckets,
            stored_front_position: bucket_store.front_position,
            new_back_values: VecDeque::new(),
            cursor,
            delete_storage_first: false,
        })
    }

    fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.cursor = if self.stored_buckets.is_empty() {
            None
        } else {
            Some(Cursor {
                offset: 0,
                position: self.stored_front_position,
            })
        };
        self.new_back_values.clear();
    }

    async fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        if !self.stored_buckets.is_empty() {
            let Some(cursor) = self.cursor else {
                return true;
            };
            if cursor.offset != 0 || cursor.position != self.stored_front_position {
                return true;
            }
        }
        !self.new_back_values.is_empty()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        let mut descriptions = Vec::new();
        let mut stored_front_position = self.stored_front_position;
        if self.stored_count() == 0 {
            let key_prefix = self.context.base_key().bytes.clone();
            batch.delete_key_prefix(key_prefix);
            delete_view = true;
            stored_front_position = 0;
        } else if let Some(cursor) = self.cursor {
            // Delete buckets that are before the cursor
            for i in 0..cursor.offset as usize {
                let bucket = &self.stored_buckets[i];
                let index = bucket.index;
                let key = self.get_bucket_key(index)?;
                batch.delete_key(key);
            }
            stored_front_position = cursor.position;
            // Build descriptions for remaining buckets
            let first_index = self.stored_buckets[cursor.offset as usize].index;
            let start_offset = if first_index != 0 {
                // Need to move the first remaining bucket to index 0
                let key = self.get_bucket_key(first_index)?;
                batch.delete_key(key);
                let key = self.get_bucket_key(0)?;
                let bucket = &self.stored_buckets[cursor.offset as usize];
                let State::Loaded { data } = &bucket.state else {
                    unreachable!("The front bucket is always loaded.");
                };
                batch.put_key_value(key, data)?;
                descriptions.push(BucketDescription {
                    length: bucket.len(),
                    index: 0,
                });
                cursor.offset + 1
            } else {
                cursor.offset
            };
            for bucket in self.stored_buckets.range(start_offset as usize..) {
                descriptions.push(bucket.to_description());
            }
        }
        if !self.new_back_values.is_empty() {
            delete_view = false;
            // Calculate the starting index for new buckets
            // If stored_count() == 0, all stored buckets are being removed, so start at 0
            // Otherwise, start after the last remaining bucket
            let mut index = if self.stored_count() == 0 {
                0
            } else if let Some(last_description) = descriptions.last() {
                last_description.index + 1
            } else {
                // This shouldn't happen if stored_count() > 0
                0
            };
            let mut start = 0_u32;
            while start < self.new_back_values.len() as u32 {
                let end = std::cmp::min(start + N, self.new_back_values.len() as u32);
                let value_chunk: Vec<_> = self
                    .new_back_values
                    .range(start as usize..end as usize)
                    .collect();
                let key = self.get_bucket_key(index)?;
                batch.put_key_value(key, &value_chunk)?;
                descriptions.push(BucketDescription {
                    index,
                    length: end - start,
                });
                index += 1;
                start = end;
            }
        }
        if !delete_view {
            let bucket_store = BucketStore {
                descriptions,
                front_position: stored_front_position,
            };
            let key = self.context.base_key().base_tag(KeyTag::Store as u8);
            batch.put_key_value(key, &bucket_store)?;
        }
        Ok(delete_view)
    }

    fn post_save(&mut self) {
        if self.stored_count() == 0 {
            self.stored_buckets.clear();
            self.stored_front_position = 0;
            self.cursor = None;
        } else if let Some(cursor) = self.cursor {
            for _ in 0..(cursor.offset as usize) {
                self.stored_buckets.pop_front();
            }
            self.cursor = Some(Cursor {
                offset: 0,
                position: cursor.position,
            });
            self.stored_front_position = cursor.position;
            // We need to ensure that the first index is in the front.
            self.stored_buckets[0].index = 0;
        }
        if !self.new_back_values.is_empty() {
            let mut index = match self.stored_buckets.back() {
                Some(bucket) => bucket.index + 1,
                None => 0,
            };
            let new_back_values = std::mem::take(&mut self.new_back_values);
            let new_back_values = new_back_values.into_iter().collect::<Vec<_>>();
            for value_chunk in new_back_values.chunks(N as usize) {
                self.stored_buckets.push_back(Bucket {
                    index,
                    state: State::Loaded {
                        data: value_chunk.to_vec(),
                    },
                });
                index += 1;
            }
            if self.cursor.is_none() {
                self.cursor = Some(Cursor {
                    offset: 0,
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

impl<C: Clone, T: Clone, const N: u32> ClonableView for BucketQueueView<C, T, N>
where
    Self: View,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(BucketQueueView {
            context: self.context.clone(),
            stored_buckets: self.stored_buckets.clone(),
            new_back_values: self.new_back_values.clone(),
            stored_front_position: self.stored_front_position,
            cursor: self.cursor,
            delete_storage_first: self.delete_storage_first,
        })
    }
}

impl<C: Context, T, const N: u32> BucketQueueView<C, T, N> {
    /// Gets the key corresponding to this bucket index.
    fn get_bucket_key(&self, index: u32) -> Result<Vec<u8>, ViewError> {
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
    pub fn stored_count(&self) -> u32 {
        if self.delete_storage_first {
            0
        } else {
            let Some(cursor) = self.cursor else {
                return 0;
            };
            let mut stored_count = 0u32;
            let offset_start = cursor.offset as usize;
            for offset in offset_start..self.stored_buckets.len() {
                stored_count += self.stored_buckets[offset].len();
            }
            stored_count -= cursor.position;
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
        self.stored_count() as usize + self.new_back_values.len()
    }
}

impl<C: Context, T: DeserializeOwned + Clone, const N: u32> BucketQueueView<C, T, N> {
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
            Some(Cursor { offset, position }) => {
                let bucket = &self.stored_buckets[offset as usize];
                let State::Loaded { data } = &bucket.state else {
                    unreachable!();
                };
                Some(&data[position as usize])
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
            Some(Cursor { offset, position }) => {
                let bucket = self.stored_buckets.get_mut(offset as usize).unwrap();
                let State::Loaded { data } = &mut bucket.state else {
                    unreachable!();
                };
                Some(data.get_mut(position as usize).unwrap())
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
            Some(cursor) => {
                let mut offset = cursor.offset;
                let mut position = cursor.position + 1;
                let mut offset_usize = offset as usize;
                if self.stored_buckets[offset_usize].len() == position {
                    offset += 1;
                    position = 0;
                    offset_usize += 1;
                }
                if offset_usize == self.stored_buckets.len() {
                    self.cursor = None;
                } else {
                    self.cursor = Some(Cursor { offset, position });
                    let bucket = self.stored_buckets.get_mut(offset_usize).unwrap();
                    let index = bucket.index;
                    if !bucket.is_loaded() {
                        let key = self.get_bucket_key(index)?;
                        let data = self.context.store().read_value(&key).await?;
                        let data = match data {
                            Some(value) => value,
                            None => {
                                return Err(ViewError::MissingEntries(
                                    "BucketQueueView::delete_front".into(),
                                ));
                            }
                        };
                        self.stored_buckets[offset_usize].state = State::Loaded { data };
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
        let Some(bucket) = self.stored_buckets.back() else {
            return Ok(None);
        };
        if !bucket.is_loaded() {
            let key = self.get_bucket_key(bucket.index)?;
            let data = self.context.store().read_value(&key).await?;
            let data = match data {
                Some(data) => data,
                None => {
                    return Err(ViewError::MissingEntries("BucketQueueView::back".into()));
                }
            };
            self.stored_buckets.back_mut().unwrap().state = State::Loaded { data };
        }
        let state = &self.stored_buckets.back_mut().unwrap().state;
        let State::Loaded { data } = state else {
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
        if let Some(cursor) = cursor {
            let mut keys = Vec::new();
            let mut position = cursor.position;
            let offset_start = cursor.offset as usize;
            for offset in offset_start..self.stored_buckets.len() {
                let bucket = &self.stored_buckets[offset];
                let size = bucket.len() - position;
                if !bucket.is_loaded() {
                    let key = self.get_bucket_key(bucket.index)?;
                    keys.push(key);
                };
                if size >= count_remain as u32 {
                    break;
                }
                count_remain -= size as usize;
                position = 0;
            }
            let values = self.context.store().read_multi_values_bytes(&keys).await?;
            let mut value_pos = 0;
            count_remain = count;
            let mut position = cursor.position;
            for offset in offset_start..self.stored_buckets.len() {
                let bucket = &self.stored_buckets[offset];
                let size = bucket.len() - position;
                let data = match &bucket.state {
                    State::Loaded { data } => data,
                    State::NotLoaded { .. } => {
                        let value = match &values[value_pos] {
                            Some(value) => value,
                            None => {
                                return Err(ViewError::MissingEntries(
                                    "BucketQueueView::read_context".into(),
                                ));
                            }
                        };
                        value_pos += 1;
                        &bcs::from_bytes::<Vec<T>>(value)?
                    }
                };
                let position_usize = position as usize;
                elements.extend(data[position_usize..].iter().take(count_remain).cloned());
                if size >= count_remain as u32 {
                    return Ok(elements);
                }
                count_remain -= size as usize;
                position = 0;
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
            let mut increment = (self.count() - count) as u32;
            let Some(cursor) = self.cursor else {
                unreachable!();
            };
            let mut position = cursor.position;
            let offset_start = cursor.offset as usize;
            for offset in offset_start..self.stored_buckets.len() {
                let size = self.stored_buckets[offset].len() - position;
                if increment < size {
                    return self
                        .read_context(
                            Some(Cursor {
                                offset: offset as u32,
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

impl<C: Context, T: Serialize + DeserializeOwned + Send + Sync + Clone, const N: u32> HashableView
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
pub type HashedBucketQueueView<C, T, const N: u32> =
    WrappedHashableContainerView<C, BucketQueueView<C, T, N>, HasherOutput>;

/// Wrapper around `BucketQueueView` to compute hashes based on the history of changes.
pub type HistoricallyHashedBucketQueueView<C, T, const N: u32> =
    HistoricallyHashableView<C, BucketQueueView<C, T, N>>;

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::BucketQueueView;
    use crate::{
        context::Context,
        graphql::{hash_name, mangle},
    };

    impl<C: Send + Sync, T: async_graphql::OutputType, const N: u32> async_graphql::TypeName
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
    impl<C: Context, T: async_graphql::OutputType, const N: u32> BucketQueueView<C, T, N>
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
