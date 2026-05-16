// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{vec_deque::IterMut, VecDeque};

use allocative::Allocative;
use linera_base::data_types::ArithmeticError;
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
    /// Key tag for the `BucketLayout`.
    Layout,
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
struct BucketLayout {
    /// The position of the front value in the front bucket.
    front_position: u32,
    /// The total number of stored buckets.
    num_buckets: u32,
    /// The logical index of the front bucket. Middle bucket at position `p` (1-indexed
    /// from front) has storage key `KeyTag::Index + (first_index + p)`.
    first_index: u32,
}

/// The position of a value in the stored bucket.
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
    stored_front_position: u32,
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
        let key2 = context.base_key().base_tag(KeyTag::Layout as u8);
        let key3 = context.base_key().base_tag(KeyTag::Back as u8);
        Ok(vec![key1, key2, key3])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let value_front = values.first().ok_or(ViewError::PostLoadValuesError)?;
        let value_layout = values.get(1).ok_or(ViewError::PostLoadValuesError)?;
        let value_back = values.get(2).ok_or(ViewError::PostLoadValuesError)?;
        let front = from_bytes_option::<Vec<T>>(value_front)?;
        let back = from_bytes_option::<Vec<T>>(value_back)?;
        let layout = from_bytes_option_or_default::<BucketLayout>(value_layout)?;
        let mut stored_buckets = VecDeque::new();
        if let Some(front_data) = front {
            // Front bucket (always loaded).
            stored_buckets.push_back(Bucket {
                index: layout.first_index,
                state: State::Loaded { data: front_data },
            });
            // Middle buckets (NotLoaded, all have exactly N elements).
            let num_middles = layout.num_buckets.saturating_sub(2);
            for p in 1..=num_middles {
                stored_buckets.push_back(Bucket {
                    index: layout.first_index + p,
                    state: State::NotLoaded { length: N },
                });
            }
            // Back bucket (always loaded, separate from front if num_buckets >= 2).
            if let Some(back_data) = back {
                stored_buckets.push_back(Bucket {
                    index: layout.first_index + layout.num_buckets - 1,
                    state: State::Loaded { data: back_data },
                });
            }
        }
        let cursor = if layout.num_buckets == 0 {
            None
        } else {
            Some(Cursor {
                offset: 0,
                position: layout.front_position as usize,
            })
        };
        Ok(Self {
            context,
            stored_buckets,
            stored_front_position: layout.front_position,
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
                position: self.stored_front_position as usize,
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
            if cursor.offset != 0 || cursor.position != self.stored_front_position as usize {
                return true;
            }
        }
        !self.new_back_values.is_empty()
    }

    fn pre_save(&self, batch: &mut Batch) -> Result<bool, ViewError> {
        let plan = self.save_plan()?;
        match plan.case {
            SaveCase::Empty => {
                if plan.has_storage {
                    batch.delete_key_prefix(self.context.base_key().bytes.clone());
                }
                Ok(true)
            }
            SaveCase::MetadataOnly => {
                batch.put_key_value(
                    self.layout_key(),
                    &BucketLayout {
                        front_position: plan.cursor_position_u32,
                        num_buckets: 1,
                        first_index: self.stored_buckets[0].index,
                    },
                )?;
                Ok(false)
            }
            SaveCase::Rewrite => {
                if plan.has_storage {
                    batch.delete_key_prefix(self.context.base_key().bytes.clone());
                }
                let all_data = self.gather_for_rewrite(&plan);
                if all_data.is_empty() {
                    return Ok(true);
                }
                let first_index = 0;
                let num_buckets = self.write_chunks(batch, &all_data, first_index)?;
                batch.put_key_value(
                    self.layout_key(),
                    &BucketLayout {
                        front_position: 0,
                        num_buckets,
                        first_index,
                    },
                )?;
                Ok(false)
            }
            SaveCase::Patch {
                new_first_index,
                remaining_count,
            } => {
                // Delete consumed middle keys (offsets 1..remaining_start).
                // Offset 0 was at KeyTag::Front (will be overwritten if front moved).
                // The last offset is the back bucket (preserved since remaining_count >= 2).
                for i in 1..plan.remaining_start {
                    let key = self.get_middle_key(self.stored_buckets[i].index)?;
                    batch.delete_key(key);
                }
                // Promote the new front bucket if the cursor crossed buckets.
                if plan.remaining_start > 0 {
                    let bucket = &self.stored_buckets[plan.remaining_start];
                    let State::Loaded { data } = &bucket.state else {
                        unreachable!("front bucket is always loaded");
                    };
                    batch.put_key_value(self.front_key(), data)?;
                    batch.delete_key(self.get_middle_key(bucket.index)?);
                }

                let num_buckets = if self.new_back_values.is_empty() {
                    remaining_count
                } else {
                    // Merge old back + new values, re-chunk into full-N middles + new back.
                    let State::Loaded { data: back_data } =
                        &self.stored_buckets.back().unwrap().state
                    else {
                        unreachable!("back bucket is always loaded");
                    };
                    let mut merged: Vec<T> = back_data.clone();
                    merged.extend(self.new_back_values.iter().cloned());
                    let chunks: Vec<&[T]> = merged.chunks(N).collect();
                    let num_new_chunks =
                        u32::try_from(chunks.len()).map_err(|_| ArithmeticError::Overflow)?;
                    let new_middle_start = new_first_index + remaining_count - 1;
                    for (i, chunk) in chunks.iter().enumerate().take(chunks.len() - 1) {
                        let i = u32::try_from(i).map_err(|_| ArithmeticError::Overflow)?;
                        let key = self.get_middle_key(new_middle_start + i)?;
                        batch.put_key_value(key, &chunk.to_vec())?;
                    }
                    batch.put_key_value(self.back_key(), &chunks.last().unwrap().to_vec())?;
                    (remaining_count - 1) + num_new_chunks
                };

                batch.put_key_value(
                    self.layout_key(),
                    &BucketLayout {
                        front_position: plan.cursor_position_u32,
                        num_buckets,
                        first_index: new_first_index,
                    },
                )?;
                Ok(false)
            }
        }
    }

    fn post_save(&mut self) {
        let plan = self.save_plan().expect("verified in pre_save");
        self.delete_storage_first = false;
        match plan.case {
            SaveCase::Empty => {
                self.stored_buckets.clear();
                self.cursor = None;
                self.stored_front_position = 0;
            }
            SaveCase::MetadataOnly => {
                self.cursor = Some(Cursor {
                    offset: 0,
                    position: plan.cursor_position,
                });
                self.stored_front_position = plan.cursor_position_u32;
            }
            SaveCase::Rewrite => {
                let mut all_data: Vec<T> = Vec::new();
                if let Some(bucket) = self.stored_buckets.get(plan.remaining_start) {
                    let State::Loaded { data } = &bucket.state else {
                        unreachable!("front bucket is always loaded");
                    };
                    all_data.extend(data[plan.cursor_position..].iter().cloned());
                }
                all_data.extend(std::mem::take(&mut self.new_back_values));
                self.stored_buckets.clear();
                for (i, chunk) in all_data.chunks(N).enumerate() {
                    let i = u32::try_from(i).expect("verified in pre_save");
                    self.stored_buckets.push_back(Bucket {
                        index: i,
                        state: State::Loaded {
                            data: chunk.to_vec(),
                        },
                    });
                }
                self.cursor = (!self.stored_buckets.is_empty()).then_some(Cursor {
                    offset: 0,
                    position: 0,
                });
                self.stored_front_position = 0;
            }
            SaveCase::Patch { .. } => {
                for _ in 0..plan.remaining_start {
                    self.stored_buckets.pop_front();
                }
                if !self.new_back_values.is_empty() {
                    let back = self.stored_buckets.pop_back().unwrap();
                    let State::Loaded { data: back_data } = back.state else {
                        unreachable!("back bucket is always loaded");
                    };
                    let first_index = self.stored_buckets[0].index;
                    let old_remaining_without_back =
                        u32::try_from(self.stored_buckets.len()).expect("verified in pre_save");
                    let mut merged: Vec<T> = back_data;
                    merged.extend(std::mem::take(&mut self.new_back_values));
                    let new_start = first_index + old_remaining_without_back;
                    for (i, chunk) in merged.chunks(N).enumerate() {
                        let i = u32::try_from(i).expect("verified in pre_save");
                        self.stored_buckets.push_back(Bucket {
                            index: new_start + i,
                            state: State::Loaded {
                                data: chunk.to_vec(),
                            },
                        });
                    }
                }
                self.cursor = Some(Cursor {
                    offset: 0,
                    position: plan.cursor_position,
                });
                self.stored_front_position = plan.cursor_position_u32;
            }
        }
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

/// Pattern describing how a save will affect storage and in-memory state.
/// Derived from `&self` in [`BucketQueueView::save_plan`] and consumed by both
/// `pre_save` (storage batch) and `post_save` (in-memory state).
#[derive(Debug)]
enum SaveCase {
    /// Drop everything — leaves the queue empty.
    Empty,
    /// The single stored bucket survives without structural change; only the
    /// cursor advanced inside the front bucket. Just refresh `front_position`.
    MetadataOnly,
    /// At most one stored bucket survives (after consumption) and there may be
    /// new values to append. Drop all keys and rewrite from scratch.
    Rewrite,
    /// Two or more stored buckets survive. Delete consumed middles, promote a
    /// surviving middle to front if the cursor crossed buckets, and — if there
    /// are new values — merge them into the back and re-chunk.
    Patch {
        new_first_index: u32,
        remaining_count: u32,
    },
}

#[derive(Debug)]
struct SavePlan {
    case: SaveCase,
    /// Offset within `stored_buckets` of the first surviving bucket.
    remaining_start: usize,
    /// Position within the front bucket of the cursor.
    cursor_position: usize,
    /// `cursor_position` pre-validated as `u32`.
    cursor_position_u32: u32,
    /// True if there is any existing storage to clear (for `Empty`/`Rewrite`).
    has_storage: bool,
}

impl<C: Context, T, const N: usize> BucketQueueView<C, T, N> {
    fn front_key(&self) -> Vec<u8> {
        self.context.base_key().base_tag(KeyTag::Front as u8)
    }

    fn back_key(&self) -> Vec<u8> {
        self.context.base_key().base_tag(KeyTag::Back as u8)
    }

    fn layout_key(&self) -> Vec<u8> {
        self.context.base_key().base_tag(KeyTag::Layout as u8)
    }

    /// Gets the key for a middle bucket with the given storage index.
    fn get_middle_key(&self, index: u32) -> Result<Vec<u8>, ViewError> {
        Ok(self
            .context
            .base_key()
            .derive_tag_key(KeyTag::Index as u8, &index)?)
    }

    /// Classifies the pending save based on the current view state.
    /// Called once by `pre_save` and once by `post_save`; since `&self` is
    /// unchanged between the two it returns the same plan both times.
    fn save_plan(&self) -> Result<SavePlan, ViewError> {
        let remaining_start = match (self.delete_storage_first, self.cursor) {
            (true, _) => self.stored_buckets.len(),
            (false, Some(c)) => c.offset,
            (false, None) => self.stored_buckets.len(),
        };
        let remaining_count = self.stored_buckets.len() - remaining_start;
        let cursor_position = self.cursor.map_or(0, |c| c.position);
        let cursor_position_u32 =
            u32::try_from(cursor_position).map_err(|_| ArithmeticError::Overflow)?;
        let has_storage = !self.stored_buckets.is_empty() || self.delete_storage_first;
        let new_back_empty = self.new_back_values.is_empty();

        let case = if remaining_count == 0 && new_back_empty {
            SaveCase::Empty
        } else if remaining_count == 1 && remaining_start == 0 && new_back_empty {
            SaveCase::MetadataOnly
        } else if remaining_count <= 1 {
            SaveCase::Rewrite
        } else {
            SaveCase::Patch {
                new_first_index: self.stored_buckets[remaining_start].index,
                remaining_count: u32::try_from(remaining_count)
                    .map_err(|_| ArithmeticError::Overflow)?,
            }
        };
        Ok(SavePlan {
            case,
            remaining_start,
            cursor_position,
            cursor_position_u32,
            has_storage,
        })
    }

    /// Collects the remaining front data (after `cursor_position`) followed by
    /// `new_back_values`. Used by the `Rewrite` case in `pre_save`.
    fn gather_for_rewrite(&self, plan: &SavePlan) -> Vec<T>
    where
        T: Clone,
    {
        let mut data = Vec::new();
        if let Some(bucket) = self.stored_buckets.get(plan.remaining_start) {
            let State::Loaded { data: front } = &bucket.state else {
                unreachable!("front bucket is always loaded");
            };
            data.extend(front[plan.cursor_position..].iter().cloned());
        }
        data.extend(self.new_back_values.iter().cloned());
        data
    }

    /// Splits `data` into N-sized chunks and writes them as front (KeyTag::Front),
    /// middles (KeyTag::Index, starting at `first_index`+1), and back (KeyTag::Back).
    /// Returns the total number of buckets written. The caller is responsible for
    /// writing the matching `BucketLayout` entry. Used by `Rewrite`.
    fn write_chunks(&self, batch: &mut Batch, data: &[T], first_index: u32) -> Result<u32, ViewError>
    where
        T: Serialize + Clone,
    {
        let chunks: Vec<&[T]> = data.chunks(N).collect();
        let num_buckets = u32::try_from(chunks.len()).map_err(|_| ArithmeticError::Overflow)?;
        batch.put_key_value(self.front_key(), &chunks[0].to_vec())?;
        for (i, chunk) in chunks
            .iter()
            .enumerate()
            .skip(1)
            .take(chunks.len().saturating_sub(2))
        {
            let i = u32::try_from(i).map_err(|_| ArithmeticError::Overflow)?;
            let key = self.get_middle_key(first_index + i)?;
            batch.put_key_value(key, &chunk.to_vec())?;
        }
        if num_buckets >= 2 {
            batch.put_key_value(self.back_key(), &chunks.last().unwrap().to_vec())?;
        }
        Ok(num_buckets)
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
                    unreachable!("The front bucket should always be loaded");
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
                let bucket = self
                    .stored_buckets
                    .get_mut(offset)
                    .expect("cursor.offset must be a valid index into stored_buckets");
                let State::Loaded { data } = &mut bucket.state else {
                    unreachable!("The front bucket should always be loaded");
                };
                Some(
                    data.get_mut(position)
                        .expect("cursor.position must be a valid index within the front bucket"),
                )
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
                    // Ensure the bucket at the new cursor is loaded BEFORE advancing the
                    // cursor: a failed load must leave the view's invariant intact (the
                    // bucket at `cursor.offset` is always `Loaded`).
                    let bucket = &self.stored_buckets[offset];
                    if !bucket.is_loaded() {
                        let key = self.get_middle_key(bucket.index)?;
                        let data =
                            self.context
                                .store()
                                .read_value(&key)
                                .await?
                                .ok_or_else(|| {
                                    ViewError::MissingEntries(
                                        "BucketQueueView::delete_front".into(),
                                    )
                                })?;
                        self.stored_buckets[offset].state = State::Loaded { data };
                    }
                    self.cursor = Some(Cursor { offset, position });
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
            for bucket in self.stored_buckets.range(cursor.offset..) {
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
            for bucket in self.stored_buckets.range(cursor.offset..) {
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
                unreachable!("Cursor should be Some when stored_count > 0");
            };
            let mut position = cursor.position;
            for (offset, bucket) in self.stored_buckets.iter().enumerate().skip(cursor.offset) {
                let size = bucket.len() - position;
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
            unreachable!("BucketQueueView::read_back: iterated past all stored buckets without finding the requested position");
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

    use linera_base::data_types::ArithmeticError;

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
            Ok(u32::try_from(self.count()).map_err(|_| ArithmeticError::Overflow)?)
        }

        async fn entries(&self, count: Option<usize>) -> async_graphql::Result<Vec<T>> {
            Ok(self
                .read_front(count.unwrap_or_else(|| self.count()))
                .await?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        batch::Batch,
        context::{Context, MemoryContext},
        store::WritableKeyValueStore as _,
    };

    /// Regression test: a failed load while advancing the cursor in
    /// `delete_front` must not leave the view in a state where the bucket at
    /// `cursor.offset` is `NotLoaded`. Previously the cursor was advanced
    /// before the load was attempted, so a subsequent `pre_save` would hit
    /// `unreachable!("The front bucket is always loaded.")`.
    #[tokio::test]
    async fn delete_front_load_failure_preserves_invariant() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view = BucketQueueView::<_, u8, 2>::load(context.clone()).await?;
        // Six elements -> front=[1,2], middle=[3,4] at index 1, back=[5,6].
        for value in [1u8, 2, 3, 4, 5, 6] {
            view.push_back(value);
        }
        save(&context, &mut view).await?;

        let mut view = BucketQueueView::<_, u8, 2>::load(context.clone()).await?;

        // Delete the middle bucket so that loading it during `delete_front` fails.
        let middle_key = view.get_middle_key(1)?;
        let mut batch = Batch::new();
        batch.delete_key(middle_key);
        context.store().write_batch(batch).await?;

        view.delete_front().await?;
        let err = view.delete_front().await.expect_err("load should fail");
        assert!(matches!(err, ViewError::MissingEntries(_)));

        save(&context, &mut view).await?;

        Ok(())
    }

    /// Roundtrip a queue through save/reload at several sizes around `N` to
    /// exercise the front/middle/back layout: empty, partial-front-only,
    /// exactly-one-bucket, front+back without middles, and several layouts with
    /// middle buckets.
    #[tokio::test]
    async fn save_load_roundtrip_across_sizes() -> Result<(), ViewError> {
        const N: usize = 3;
        for size in [0usize, 1, 2, N, N + 1, 2 * N, 2 * N + 1, 5 * N, 5 * N - 1] {
            let context = MemoryContext::new_for_testing(());
            let mut view = BucketQueueView::<_, u32, N>::load(context.clone()).await?;
            for i in 0..u32::try_from(size).unwrap() {
                view.push_back(i);
            }
            save(&context, &mut view).await?;

            let reloaded = BucketQueueView::<_, u32, N>::load(context).await?;
            let elements = reloaded.elements().await?;
            let expected: Vec<u32> = (0..u32::try_from(size).unwrap()).collect();
            assert_eq!(elements, expected, "size = {size}");
            assert_eq!(reloaded.count(), size, "count for size = {size}");
        }
        Ok(())
    }

    /// Middle buckets must always contain exactly `N` elements after save —
    /// this is the invariant that makes the new `BucketLayout` metadata O(1).
    /// Verify the in-memory state directly after several save patterns.
    #[tokio::test]
    async fn middle_buckets_are_always_full() -> Result<(), ViewError> {
        const N: usize = 4;
        let context = MemoryContext::new_for_testing(());
        let mut view = BucketQueueView::<_, u32, N>::load(context.clone()).await?;
        // Push enough to create several middles, then save.
        for i in 0..u32::try_from(5 * N + 2).unwrap() {
            view.push_back(i);
        }
        save(&context, &mut view).await?;

        // Push a partial back to verify the next save merges and re-chunks correctly.
        view.push_back(1000);
        view.push_back(1001);
        save(&context, &mut view).await?;

        // Drop a few front elements (less than N), save, and check again.
        for _ in 0..(N - 1) {
            view.delete_front().await?;
        }
        save(&context, &mut view).await?;

        // After all this, every middle bucket in storage must have exactly N
        // elements. Reload and inspect the in-memory state.
        let view = BucketQueueView::<_, u32, N>::load(context).await?;
        let len = view.stored_buckets.len();
        for (offset, bucket) in view.stored_buckets.iter().enumerate() {
            let is_endpoint = offset == 0 || offset == len - 1;
            if !is_endpoint {
                let State::NotLoaded { length } = bucket.state else {
                    panic!("middle bucket at offset {offset} should be NotLoaded");
                };
                assert_eq!(
                    length, N,
                    "middle at offset {offset} should hold N elements"
                );
            }
        }
        Ok(())
    }

    /// `stored_count` must be exact across the partial-front and partial-back
    /// edge cases, even when the cursor has advanced inside the front bucket.
    #[tokio::test]
    async fn stored_count_is_exact() -> Result<(), ViewError> {
        const N: usize = 3;
        let context = MemoryContext::new_for_testing(());
        let mut view = BucketQueueView::<_, u32, N>::load(context.clone()).await?;
        // 7 elements -> front [0,1,2], middle [3,4,5], back [6].
        for i in 0..7u32 {
            view.push_back(i);
        }
        save(&context, &mut view).await?;

        let mut view = BucketQueueView::<_, u32, N>::load(context).await?;
        assert_eq!(view.stored_count(), 7);
        view.delete_front().await?; // drop 0
        assert_eq!(view.stored_count(), 6);
        view.delete_front().await?; // drop 1
        assert_eq!(view.stored_count(), 5);
        view.delete_front().await?; // drop 2, crosses into middle bucket
        assert_eq!(view.stored_count(), 4);
        Ok(())
    }

    /// Build a view that hits each `SaveCase` variant and verify dispatch +
    /// roundtrip. Pinning each case to a concrete scenario means a refactor
    /// that silently drops a branch fails loudly instead of relying on the
    /// random fuzz to catch it eventually.
    #[tokio::test]
    async fn save_plan_covers_each_case() -> Result<(), ViewError> {
        const N: usize = 3;

        // Empty: a freshly-loaded view with no pending changes.
        let context = MemoryContext::new_for_testing(());
        let view = BucketQueueView::<_, u32, N>::load(context).await?;
        assert!(matches!(view.save_plan()?.case, SaveCase::Empty));

        // MetadataOnly: a single stored bucket with the cursor advanced inside it.
        let context = MemoryContext::new_for_testing(());
        let mut view = BucketQueueView::<_, u32, N>::load(context.clone()).await?;
        view.push_back(10);
        view.push_back(20);
        save(&context, &mut view).await?;
        let mut view = BucketQueueView::<_, u32, N>::load(context).await?;
        view.delete_front().await?;
        assert!(matches!(view.save_plan()?.case, SaveCase::MetadataOnly));

        // Rewrite: <= 1 bucket survives but there is a structural change
        // (consumed past the front bucket; only the back remains).
        let context = MemoryContext::new_for_testing(());
        let mut view = BucketQueueView::<_, u32, N>::load(context.clone()).await?;
        for i in 0..5u32 {
            view.push_back(i);
        }
        save(&context, &mut view).await?;
        let mut view = BucketQueueView::<_, u32, N>::load(context).await?;
        for _ in 0..N {
            view.delete_front().await?;
        }
        assert!(matches!(view.save_plan()?.case, SaveCase::Rewrite));

        // Patch: >= 2 buckets survive (here: front + middle + back, with a
        // pending new value to force the re-chunk path).
        let context = MemoryContext::new_for_testing(());
        let mut view = BucketQueueView::<_, u32, N>::load(context.clone()).await?;
        for i in 0..7u32 {
            view.push_back(i);
        }
        save(&context, &mut view).await?;
        let mut view = BucketQueueView::<_, u32, N>::load(context).await?;
        view.push_back(100);
        assert!(matches!(view.save_plan()?.case, SaveCase::Patch { .. }));

        Ok(())
    }

    /// N=1 is degenerate: every bucket holds exactly one element, the front
    /// and back can't share a bucket, and every middle is also a single
    /// element. Exercise enough operations to cross several bucket boundaries.
    #[tokio::test]
    async fn n_equals_one_roundtrip() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view = BucketQueueView::<_, u32, 1>::load(context.clone()).await?;
        for i in 0..5u32 {
            view.push_back(i);
        }
        save(&context, &mut view).await?;

        let mut view = BucketQueueView::<_, u32, 1>::load(context.clone()).await?;
        assert_eq!(view.elements().await?, vec![0, 1, 2, 3, 4]);
        view.delete_front().await?;
        view.delete_front().await?;
        view.push_back(99);
        save(&context, &mut view).await?;

        let view = BucketQueueView::<_, u32, 1>::load(context).await?;
        assert_eq!(view.elements().await?, vec![2, 3, 4, 99]);
        Ok(())
    }

    /// `rollback` must wipe in-memory edits and restore the cursor / state
    /// to whatever was last saved.
    #[tokio::test]
    async fn rollback_restores_saved_state() -> Result<(), ViewError> {
        const N: usize = 3;
        let context = MemoryContext::new_for_testing(());
        let mut view = BucketQueueView::<_, u32, N>::load(context.clone()).await?;
        for i in 0..5u32 {
            view.push_back(i);
        }
        save(&context, &mut view).await?;

        let mut view = BucketQueueView::<_, u32, N>::load(context).await?;
        view.delete_front().await?;
        view.delete_front().await?;
        view.push_back(100);
        view.push_back(101);
        assert_eq!(view.elements().await?, vec![2, 3, 4, 100, 101]);

        view.rollback();
        assert_eq!(view.elements().await?, vec![0, 1, 2, 3, 4]);

        // After rollback, has_pending_changes must be false again.
        assert!(!view.has_pending_changes().await);
        Ok(())
    }

    async fn save<V: View>(context: &V::Context, view: &mut V) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        view.pre_save(&mut batch)?;
        context.store().write_batch(batch).await?;
        view.post_save();
        Ok(())
    }
}
