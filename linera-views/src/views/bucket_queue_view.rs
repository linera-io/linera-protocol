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
    /// Key tag for the front bucket.
    Front = MIN_VIEW_TAG,
    /// Key tag for the `BucketStore`.
    Store,
    /// Key tag for the content of middle buckets.
    Index,
    /// Key tag for the back bucket.
    Back,
}

/// The metadata of the view in storage.
///
/// This is O(1) in size regardless of the number of buckets. The invariant is that
/// all middle buckets (i.e. neither front nor back) have exactly N elements. Only
/// the front bucket (tracked by `front_position`) and back bucket can be partial.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct BucketStore {
    /// The position of the front value in the front bucket.
    front_position: usize,
    /// The total number of stored buckets.
    num_buckets: usize,
    /// The logical index of the front bucket. Middle bucket at position `p` (1-indexed
    /// from front) has storage key `KeyTag::Index + (first_index + p)`.
    first_index: usize,
}

/// The position of a value in the stored buckket.
#[derive(Copy, Clone, Debug, Allocative)]
struct Cursor {
    /// The offset of the bucket in the vector of stored buckets.
    offset: usize,
    /// The position of the value in the stored bucket.
    position: usize,
}

/// The state of a stored bucket in memory.
#[derive(Clone, Debug, Allocative)]
enum State<T> {
    Loaded { data: Vec<T> },
    NotLoaded { length: usize },
}

impl<T> Bucket<T> {
    fn len(&self) -> usize {
        match &self.state {
            State::Loaded { data } => data.len(),
            State::NotLoaded { length } => *length,
        }
    }

    fn is_loaded(&self) -> bool {
        match self.state {
            State::Loaded { .. } => true,
            State::NotLoaded { .. } => false,
        }
    }
}

/// A stored bucket.
#[derive(Clone, Debug, Allocative)]
struct Bucket<T> {
    /// The index in storage.
    index: usize,
    /// The state of the bucket.
    state: State<T>,
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
    stored_buckets: VecDeque<Bucket<T>>,
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
    const NUM_INIT_KEYS: usize = 3;

    type Context = C;

    fn context(&self) -> C {
        self.context.clone()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        let key1 = context.base_key().base_tag(KeyTag::Front as u8);
        let key2 = context.base_key().base_tag(KeyTag::Store as u8);
        let key3 = context.base_key().base_tag(KeyTag::Back as u8);
        Ok(vec![key1, key2, key3])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let value_front = values.first().ok_or(ViewError::PostLoadValuesError)?;
        let value_store = values.get(1).ok_or(ViewError::PostLoadValuesError)?;
        let value_back = values.get(2).ok_or(ViewError::PostLoadValuesError)?;
        let front = from_bytes_option::<Vec<T>>(value_front)?;
        let back = from_bytes_option::<Vec<T>>(value_back)?;
        let bucket_store = from_bytes_option_or_default::<BucketStore>(value_store)?;
        let mut stored_buckets = VecDeque::new();
        if let Some(front_data) = front {
            // Front bucket (always loaded).
            stored_buckets.push_back(Bucket {
                index: bucket_store.first_index,
                state: State::Loaded { data: front_data },
            });
            // Middle buckets (NotLoaded, all have exactly N elements).
            let num_middles = bucket_store.num_buckets.saturating_sub(2);
            for p in 1..=num_middles {
                stored_buckets.push_back(Bucket {
                    index: bucket_store.first_index + p,
                    state: State::NotLoaded { length: N },
                });
            }
            // Back bucket (always loaded, separate from front if num_buckets >= 2).
            if let Some(back_data) = back {
                stored_buckets.push_back(Bucket {
                    index: bucket_store.first_index + bucket_store.num_buckets - 1,
                    state: State::Loaded { data: back_data },
                });
            }
        }
        let cursor = if bucket_store.num_buckets == 0 {
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
        let remaining_start = match (self.delete_storage_first, self.cursor) {
            (true, _) => self.stored_buckets.len(),
            (false, Some(c)) => c.offset,
            (false, None) => self.stored_buckets.len(),
        };
        let remaining_count = self.stored_buckets.len() - remaining_start;
        let cursor_position = match self.cursor {
            Some(c) => c.position,
            None => 0,
        };

        // Case 1: Nothing remains and no new values -> empty queue.
        if remaining_count == 0 && self.new_back_values.is_empty() {
            if !self.stored_buckets.is_empty() || self.delete_storage_first {
                batch.delete_key_prefix(self.context.base_key().bytes.clone());
            }
            return Ok(true);
        }

        // Case 2: Exactly 1 stored bucket remains, front didn't move, no new values.
        // Just update the metadata (front_position).
        if remaining_count == 1 && remaining_start == 0 && self.new_back_values.is_empty() {
            let store = BucketStore {
                front_position: cursor_position,
                num_buckets: 1,
                first_index: self.stored_buckets[0].index,
            };
            let store_key = self.context.base_key().base_tag(KeyTag::Store as u8);
            batch.put_key_value(store_key, &store)?;
            return Ok(false);
        }

        // Case 3: At most 1 stored bucket remains with structural changes.
        // Delete everything and rewrite from scratch.
        if remaining_count <= 1 {
            if !self.stored_buckets.is_empty() || self.delete_storage_first {
                batch.delete_key_prefix(self.context.base_key().bytes.clone());
            }

            // Collect all valid data.
            let mut all_data: Vec<T> = Vec::new();
            if remaining_count == 1 {
                let bucket = &self.stored_buckets[remaining_start];
                let State::Loaded { data } = &bucket.state else {
                    unreachable!("front bucket is always loaded");
                };
                all_data.extend(data[cursor_position..].iter().cloned());
            }
            all_data.extend(self.new_back_values.iter().cloned());

            if all_data.is_empty() {
                return Ok(true);
            }

            // Chunk into buckets and write.
            let chunks: Vec<&[T]> = all_data.chunks(N).collect();
            let num_buckets = chunks.len();
            let first_index = 0usize;

            // Front bucket.
            let front_key = self.context.base_key().base_tag(KeyTag::Front as u8);
            batch.put_key_value(front_key, &chunks[0].to_vec())?;

            // Middle buckets (all exactly N elements).
            for (i, chunk) in chunks
                .iter()
                .enumerate()
                .skip(1)
                .take(num_buckets.saturating_sub(2))
            {
                let key = self.get_middle_key(first_index + i)?;
                batch.put_key_value(key, &chunk.to_vec())?;
            }

            // Back bucket (if more than 1 chunk).
            if num_buckets >= 2 {
                let back_key = self.context.base_key().base_tag(KeyTag::Back as u8);
                batch.put_key_value(back_key, &chunks[num_buckets - 1].to_vec())?;
            }

            let store = BucketStore {
                front_position: 0,
                num_buckets,
                first_index,
            };
            let store_key = self.context.base_key().base_tag(KeyTag::Store as u8);
            batch.put_key_value(store_key, &store)?;
            return Ok(false);
        }

        // Case 3: remaining_count >= 2 (middle buckets exist, can't delete everything).
        let new_first_index = self.stored_buckets[remaining_start].index;
        let new_front_position = cursor_position;

        // Delete consumed buckets: positions 0..remaining_start.
        // Position 0 was at KeyTag::Front (will be overwritten if front moved).
        // Positions 1..old_len-2 were at KeyTag::Index.
        // Position old_len-1 was at KeyTag::Back (not consumed since remaining_count >= 2).
        for i in 1..remaining_start {
            let key = self.get_middle_key(self.stored_buckets[i].index)?;
            batch.delete_key(key);
        }

        // Move new front to KeyTag::Front if it changed.
        if remaining_start > 0 {
            let front_bucket = &self.stored_buckets[remaining_start];
            let State::Loaded { data } = &front_bucket.state else {
                unreachable!("front bucket is always loaded");
            };
            let front_key = self.context.base_key().base_tag(KeyTag::Front as u8);
            batch.put_key_value(front_key, data)?;
            // Delete the old key for this bucket (it was a middle).
            let old_key = self.get_middle_key(front_bucket.index)?;
            batch.delete_key(old_key);
        }

        if self.new_back_values.is_empty() {
            // No new values: back stays at KeyTag::Back. Just update metadata.
            let store = BucketStore {
                front_position: new_front_position,
                num_buckets: remaining_count,
                first_index: new_first_index,
            };
            let store_key = self.context.base_key().base_tag(KeyTag::Store as u8);
            batch.put_key_value(store_key, &store)?;
        } else {
            // Merge back bucket data with new values, then re-chunk.
            let back_bucket = self.stored_buckets.back().unwrap();
            let State::Loaded { data: back_data } = &back_bucket.state else {
                unreachable!("back bucket is always loaded");
            };

            let mut merged: Vec<T> = back_data.clone();
            merged.extend(self.new_back_values.iter().cloned());

            let chunks: Vec<&[T]> = merged.chunks(N).collect();
            let num_new_chunks = chunks.len();

            // New middle bucket indices start at the old back's logical position.
            let new_middle_start_index = new_first_index + remaining_count - 1;

            // Write new middle buckets (all chunks except the last).
            for (i, chunk) in chunks.iter().enumerate().take(num_new_chunks - 1) {
                let index = new_middle_start_index + i;
                let key = self.get_middle_key(index)?;
                batch.put_key_value(key, &chunk.to_vec())?;
            }

            // Write new back bucket (last chunk, overwrites old KeyTag::Back).
            let back_key = self.context.base_key().base_tag(KeyTag::Back as u8);
            batch.put_key_value(back_key, &chunks[num_new_chunks - 1].to_vec())?;

            // Total buckets: (remaining - 1 for the old back) + num_new_chunks.
            let new_num_buckets = (remaining_count - 1) + num_new_chunks;

            let store = BucketStore {
                front_position: new_front_position,
                num_buckets: new_num_buckets,
                first_index: new_first_index,
            };
            let store_key = self.context.base_key().base_tag(KeyTag::Store as u8);
            batch.put_key_value(store_key, &store)?;
        }

        Ok(false)
    }

    fn post_save(&mut self) {
        let remaining_start = match (self.delete_storage_first, self.cursor) {
            (true, _) => self.stored_buckets.len(),
            (false, Some(c)) => c.offset,
            (false, None) => self.stored_buckets.len(),
        };
        let remaining_count = self.stored_buckets.len() - remaining_start;
        let cursor_position = match self.cursor {
            Some(c) => c.position,
            None => 0,
        };

        if remaining_count == 0 && self.new_back_values.is_empty() {
            // Empty queue.
            self.stored_buckets.clear();
            self.cursor = None;
            self.stored_front_position = 0;
            self.delete_storage_first = false;
            return;
        }

        // Single bucket, front didn't move, no new values: just update position.
        if remaining_count == 1 && remaining_start == 0 && self.new_back_values.is_empty() {
            self.cursor = Some(Cursor {
                offset: 0,
                position: cursor_position,
            });
            self.stored_front_position = cursor_position;
            self.delete_storage_first = false;
            return;
        }

        if remaining_count <= 1 {
            // Rebuilt from scratch in pre_save.
            let mut all_data: Vec<T> = Vec::new();
            if remaining_count == 1 {
                let bucket = &self.stored_buckets[remaining_start];
                let State::Loaded { data } = &bucket.state else {
                    unreachable!();
                };
                all_data.extend(data[cursor_position..].iter().cloned());
            }
            all_data.extend(std::mem::take(&mut self.new_back_values));

            self.stored_buckets.clear();
            for (i, chunk) in all_data.chunks(N).enumerate() {
                self.stored_buckets.push_back(Bucket {
                    index: i,
                    state: State::Loaded {
                        data: chunk.to_vec(),
                    },
                });
            }
            self.cursor = if self.stored_buckets.is_empty() {
                None
            } else {
                Some(Cursor {
                    offset: 0,
                    position: 0,
                })
            };
            self.stored_front_position = 0;
            self.delete_storage_first = false;
            return;
        }

        // remaining_count >= 2: remove consumed buckets from front.
        for _ in 0..remaining_start {
            self.stored_buckets.pop_front();
        }

        if self.new_back_values.is_empty() {
            // No new values. Just update cursor.
            self.cursor = Some(Cursor {
                offset: 0,
                position: cursor_position,
            });
            self.stored_front_position = cursor_position;
        } else {
            // Merge back with new values.
            let back = self.stored_buckets.pop_back().unwrap();
            let State::Loaded { data: back_data } = back.state else {
                unreachable!();
            };
            let first_index = self.stored_buckets[0].index;
            let old_remaining_without_back = self.stored_buckets.len();

            let mut merged: Vec<T> = back_data;
            merged.extend(std::mem::take(&mut self.new_back_values));

            let new_start_index = first_index + old_remaining_without_back;
            for (i, chunk) in merged.chunks(N).enumerate() {
                self.stored_buckets.push_back(Bucket {
                    index: new_start_index + i,
                    state: State::Loaded {
                        data: chunk.to_vec(),
                    },
                });
            }
            self.cursor = Some(Cursor {
                offset: 0,
                position: cursor_position,
            });
            self.stored_front_position = cursor_position;
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
            stored_buckets: self.stored_buckets.clone(),
            new_back_values: self.new_back_values.clone(),
            stored_front_position: self.stored_front_position,
            cursor: self.cursor,
            delete_storage_first: self.delete_storage_first,
        })
    }
}

impl<C: Context, T, const N: usize> BucketQueueView<C, T, N> {
    /// Gets the key for a middle bucket with the given storage index.
    fn get_middle_key(&self, index: usize) -> Result<Vec<u8>, ViewError> {
        Ok(self
            .context
            .base_key()
            .derive_tag_key(KeyTag::Index as u8, &index)?)
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
            return 0;
        }
        let Some(cursor) = self.cursor else {
            return 0;
        };
        let remaining = self.stored_buckets.len() - cursor.offset;
        if remaining == 0 {
            return 0;
        }
        // Front bucket: count valid elements after cursor position.
        let front = &self.stored_buckets[cursor.offset];
        let front_count = front.len() - cursor.position;
        if remaining == 1 {
            return front_count;
        }
        // Back bucket (always loaded, so len() is cheap).
        let back_count = self.stored_buckets.back().unwrap().len();
        // Middle buckets: all have exactly N elements (invariant).
        let num_middles = remaining - 2;
        front_count + num_middles * N + back_count
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
            Some(Cursor { offset, position }) => {
                let bucket = &self.stored_buckets[offset];
                let State::Loaded { data } = &bucket.state else {
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
            Some(Cursor { offset, position }) => {
                let bucket = self.stored_buckets.get_mut(offset).unwrap();
                let State::Loaded { data } = &mut bucket.state else {
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
            Some(cursor) => {
                let mut offset = cursor.offset;
                let mut position = cursor.position + 1;
                if self.stored_buckets[offset].len() == position {
                    offset += 1;
                    position = 0;
                }
                if offset == self.stored_buckets.len() {
                    self.cursor = None;
                } else {
                    self.cursor = Some(Cursor { offset, position });
                    let bucket = self.stored_buckets.get_mut(offset).unwrap();
                    let index = bucket.index;
                    if !bucket.is_loaded() {
                        let key = self.get_middle_key(index)?;
                        let data = self.context.store().read_value(&key).await?;
                        let data = match data {
                            Some(value) => value,
                            None => {
                                return Err(ViewError::MissingEntries(
                                    "BucketQueueView::delete_front".into(),
                                ));
                            }
                        };
                        self.stored_buckets[offset].state = State::Loaded { data };
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
        // Back bucket is always loaded (invariant).
        let State::Loaded { data } = &bucket.state else {
            unreachable!("back bucket should always be loaded");
        };
        Ok(data.last().cloned())
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
            for offset in cursor.offset..self.stored_buckets.len() {
                let bucket = &self.stored_buckets[offset];
                let size = bucket.len() - position;
                if !bucket.is_loaded() {
                    let key = self.get_middle_key(bucket.index)?;
                    keys.push(key);
                };
                if size >= count_remain {
                    break;
                }
                count_remain -= size;
                position = 0;
            }
            let values = self.context.store().read_multi_values_bytes(&keys).await?;
            let mut value_pos = 0;
            count_remain = count;
            let mut position = cursor.position;
            for offset in cursor.offset..self.stored_buckets.len() {
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
                elements.extend(data[position..].iter().take(count_remain).cloned());
                if size >= count_remain {
                    return Ok(elements);
                }
                count_remain -= size;
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
            let mut increment = self.count() - count;
            let Some(cursor) = self.cursor else {
                unreachable!();
            };
            let mut position = cursor.position;
            for offset in cursor.offset..self.stored_buckets.len() {
                let size = self.stored_buckets[offset].len() - position;
                if increment < size {
                    return self
                        .read_context(
                            Some(Cursor {
                                offset,
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
    /// let mut iter = queue.try_iter_mut().await.unwrap();
    /// let value = iter.next().unwrap();
    /// *value = 42;
    /// assert_eq!(queue.elements().await.unwrap(), vec![42]);
    /// # })
    /// ```
    pub async fn try_iter_mut(&mut self) -> Result<IterMut<'_, T>, ViewError> {
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
