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
    /// Key tag for the `BucketLayout` metadata.
    Layout = MIN_VIEW_TAG,
    /// Key tag for the front bucket.
    Front,
    /// Key tag for the content of middle buckets.
    Middle,
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
    /// from front) has storage key `KeyTag::Middle + (first_index + p)`.
    first_index: u32,
}

/// The position of the queue's front value within the stored buckets.
#[derive(Copy, Clone, Debug, Allocative)]
struct Cursor {
    /// The offset of the current front bucket from the front bucket of the saved
    /// layout (`0` = the saved `stored_front` bucket).
    offset: usize,
    /// The position of the value within that bucket.
    position: usize,
}

/// A view that supports a FIFO queue for values of type `T`.
/// The size `N` has to be chosen by taking into account the size of the type `T`
/// and the basic size of a block. For example a total size of 100 bytes to 10 KB
/// seems adequate.
///
/// Only the endpoints of the stored sequence are materialized: `stored_front` and
/// `stored_back` are kept in memory, while the middle buckets live solely in storage
/// (each holds exactly `N` elements, so they are tracked by `stored_num_buckets` alone
/// and read back lazily). The sole exception is `current_middle`, which holds the bucket
/// the cursor currently points into once `delete_front` has advanced past the front.
//#[allocative(bound = "T: Allocative")]
#[derive(Debug, Allocative)]
#[allocative(bound = "C, T: Allocative, const N: usize")]
pub struct BucketQueueView<C, T, const N: usize> {
    /// The view context.
    #[allocative(skip)]
    context: C,
    /// The newly inserted back values, not yet persisted.
    new_back_values: VecDeque<T>,
    /// Storage index of the front bucket (the bucket at cursor offset `0`).
    stored_first_index: u32,
    /// The total number of stored buckets (front + middles + back).
    stored_num_buckets: u32,
    /// The front bucket's data, fully materialized; empty iff `stored_num_buckets == 0`.
    ///
    /// This is the saved front bucket and the anchor that `rollback` restores to: it is
    /// never mutated by `delete_front`, so `rollback` (which is synchronous and reads
    /// neither storage nor the live cursor) can rebuild the cursor from it together with
    /// `stored_front_position`. In-memory mutations such as `clear` must leave it intact.
    stored_front: Vec<T>,
    /// The back bucket's data, fully materialized; empty when `stored_num_buckets < 2`
    /// (a single stored bucket is represented by `stored_front` alone).
    stored_back: Vec<T>,
    /// Position of the front value within `stored_front`, as of the last save.
    stored_front_position: u32,
    /// The live cursor over the stored buckets, or `None` when no stored value remains
    /// (so the front, if any, is the first of `new_back_values`).
    ///
    /// `None` does not imply `stored_num_buckets == 0`: after enough `delete_front`s the
    /// cursor walks off the end and becomes `None` while the saved layout stays untouched
    /// until the next save.
    cursor: Option<Cursor>,
    /// The data of the middle bucket the cursor currently points into, materialized when
    /// `delete_front` has advanced strictly inside the middles (`0 < cursor.offset <
    /// stored_num_buckets - 1`). `None` otherwise.
    current_middle: Option<Vec<T>>,
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
        let key1 = context.base_key().base_tag(KeyTag::Layout as u8);
        let key2 = context.base_key().base_tag(KeyTag::Front as u8);
        let key3 = context.base_key().base_tag(KeyTag::Back as u8);
        Ok(vec![key1, key2, key3])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let value_layout = values.first().ok_or(ViewError::PostLoadValuesError)?;
        let value_front = values.get(1).ok_or(ViewError::PostLoadValuesError)?;
        let value_back = values.get(2).ok_or(ViewError::PostLoadValuesError)?;
        let front = from_bytes_option::<Vec<T>>(value_front)?;
        let back = from_bytes_option::<Vec<T>>(value_back)?;
        let layout = from_bytes_option_or_default::<BucketLayout>(value_layout)?;
        // The front bucket is present iff there is stored data. The middle buckets are
        // read back lazily and the back bucket is materialized only when `num_buckets >= 2`.
        let (stored_first_index, stored_num_buckets, stored_front, stored_back, cursor) =
            match front {
                Some(front) => {
                    let back = if layout.num_buckets >= 2 {
                        back.unwrap_or_default()
                    } else {
                        Vec::new()
                    };
                    let cursor = Cursor {
                        offset: 0,
                        position: layout.front_position as usize,
                    };
                    (
                        layout.first_index,
                        layout.num_buckets,
                        front,
                        back,
                        Some(cursor),
                    )
                }
                None => (0, 0, Vec::new(), Vec::new(), None),
            };
        Ok(Self {
            context,
            new_back_values: VecDeque::new(),
            stored_first_index,
            stored_num_buckets,
            stored_front,
            stored_back,
            stored_front_position: layout.front_position,
            cursor,
            current_middle: None,
            delete_storage_first: false,
        })
    }

    fn rollback(&mut self) {
        // The saved layout (`stored_front`, `stored_back`, `stored_first_index`,
        // `stored_num_buckets`, `stored_front_position`) is never mutated outside of a
        // save, so restoring the last-saved state only requires resetting the live cursor
        // and dropping the pending back values.
        self.delete_storage_first = false;
        self.cursor = (self.stored_num_buckets > 0).then_some(Cursor {
            offset: 0,
            position: self.stored_front_position as usize,
        });
        self.current_middle = None;
        self.new_back_values.clear();
    }

    async fn has_pending_changes(&self) -> bool {
        if self.delete_storage_first {
            return true;
        }
        if self.stored_num_buckets > 0 {
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
                        front_position: plan.cursor_position,
                        num_buckets: 1,
                        first_index: self.stored_first_index,
                    },
                )?;
                Ok(false)
            }
            SaveCase::Rewrite => {
                if plan.has_storage {
                    batch.delete_key_prefix(self.context.base_key().bytes.clone());
                }
                let mut all_data = Vec::new();
                if let Some(data) = self.current_front_data() {
                    all_data.extend(data[plan.cursor_position as usize..].iter().cloned());
                }
                all_data.extend(self.new_back_values.iter().cloned());
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
                // Delete the keys of the consumed middle buckets (relative offsets
                // 1..remaining_offset). Offset 0 was the old front (its KeyTag::Front is
                // overwritten below if the front moved). The back is preserved since
                // remaining_count >= 2.
                for offset in 1..plan.remaining_offset {
                    let index = self.stored_first_index
                        + u32::try_from(offset).map_err(|_| ArithmeticError::Overflow)?;
                    batch.delete_key(self.get_middle_key(index)?);
                }
                // Promote the new front bucket if the cursor crossed buckets.
                if plan.remaining_offset > 0 {
                    let data = self
                        .current_front_data()
                        .expect("Patch implies a live cursor within the stored buckets");
                    batch.put_key_value(self.front_key(), &data.to_vec())?;
                    batch.delete_key(self.get_middle_key(new_first_index)?);
                }

                let num_buckets = if self.new_back_values.is_empty() {
                    remaining_count
                } else {
                    // Merge old back + new values, re-chunk into full-N middles + new back.
                    let mut merged = self.stored_back.clone();
                    merged.extend(self.new_back_values.iter().cloned());
                    let chunks = merged.chunks(N).collect::<Vec<_>>();
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
                        front_position: plan.cursor_position,
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
                self.stored_first_index = 0;
                self.stored_num_buckets = 0;
                self.stored_front = Vec::new();
                self.stored_back = Vec::new();
                self.cursor = None;
                self.current_middle = None;
                self.stored_front_position = 0;
            }
            SaveCase::MetadataOnly => {
                // The single front bucket survives unchanged; only the cursor advanced.
                self.cursor = Some(Cursor {
                    offset: 0,
                    position: plan.cursor_position as usize,
                });
                self.current_middle = None;
                self.stored_front_position = plan.cursor_position;
            }
            SaveCase::Rewrite => {
                let mut all_data = Vec::new();
                if let Some(data) = self.current_front_data() {
                    all_data.extend(data[plan.cursor_position as usize..].iter().cloned());
                }
                all_data.extend(std::mem::take(&mut self.new_back_values));
                // Mirror the `post_load` shape: only the front and back are materialized;
                // the middles were written by `pre_save` and are read back lazily.
                let num_chunks = all_data.chunks(N).len();
                self.stored_first_index = 0;
                self.stored_num_buckets = u32::try_from(num_chunks).expect("verified in pre_save");
                self.current_middle = None;
                self.stored_front_position = 0;
                if num_chunks == 0 {
                    self.stored_front = Vec::new();
                    self.stored_back = Vec::new();
                    self.cursor = None;
                } else {
                    self.stored_back = if num_chunks >= 2 {
                        all_data[(num_chunks - 1) * N..].to_vec()
                    } else {
                        Vec::new()
                    };
                    all_data.truncate(N); // keep only the front chunk
                    self.stored_front = all_data;
                    self.cursor = Some(Cursor {
                        offset: 0,
                        position: 0,
                    });
                }
            }
            SaveCase::Patch {
                new_first_index,
                remaining_count,
            } => {
                let cursor = self.cursor.expect("Patch implies a live cursor");
                // Promote the current front bucket to the new saved front. With >= 2
                // buckets surviving the cursor is never on the back, so a moved front is
                // always the materialized `current_middle`.
                if plan.remaining_offset > 0 {
                    self.stored_front = self
                        .current_middle
                        .take()
                        .expect("the middle the cursor points into is loaded");
                }
                self.current_middle = None;
                self.stored_first_index = new_first_index;
                self.stored_num_buckets = if self.new_back_values.is_empty() {
                    remaining_count
                } else {
                    // Merge old back + new values; the last chunk is the new back and the
                    // earlier chunks are new middles (storage-only).
                    let mut merged = std::mem::take(&mut self.stored_back);
                    merged.extend(std::mem::take(&mut self.new_back_values));
                    let num_new_chunks =
                        u32::try_from(merged.chunks(N).len()).expect("verified in pre_save");
                    let back_start = (num_new_chunks as usize - 1) * N;
                    self.stored_back = merged.split_off(back_start);
                    (remaining_count - 1) + num_new_chunks
                };
                self.cursor = Some(Cursor {
                    offset: 0,
                    position: cursor.position,
                });
                self.stored_front_position = plan.cursor_position;
            }
        }
    }

    fn clear(&mut self) {
        // Leaves the saved layout in place (the `rollback` anchor); the next save sees
        // `delete_storage_first` and wipes storage regardless.
        self.delete_storage_first = true;
        self.new_back_values.clear();
        self.cursor = None;
        self.current_middle = None;
    }
}

impl<C: Clone, T: Clone, const N: usize> ClonableView for BucketQueueView<C, T, N>
where
    Self: View,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(BucketQueueView {
            context: self.context.clone(),
            new_back_values: self.new_back_values.clone(),
            stored_first_index: self.stored_first_index,
            stored_num_buckets: self.stored_num_buckets,
            stored_front: self.stored_front.clone(),
            stored_back: self.stored_back.clone(),
            stored_front_position: self.stored_front_position,
            cursor: self.cursor,
            current_middle: self.current_middle.clone(),
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
    /// Relative bucket offset of the first surviving bucket (`cursor.offset`, or
    /// `stored_num_buckets` when everything is dropped).
    remaining_offset: usize,
    /// Position of the cursor within the front bucket, validated as `u32` (the type
    /// stored in `BucketLayout`). Cast to `usize` at the few sites that index into a
    /// bucket's data.
    cursor_position: u32,
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
            .derive_tag_key(KeyTag::Middle as u8, &index)?)
    }

    /// The data of the bucket the live cursor points into (the queue front within the
    /// stored buckets), or `None` when the stored portion is exhausted.
    fn current_front_data(&self) -> Option<&[T]> {
        let cursor = self.cursor?;
        let num_buckets = self.stored_num_buckets as usize;
        Some(if cursor.offset == 0 {
            &self.stored_front
        } else if cursor.offset + 1 == num_buckets {
            &self.stored_back
        } else {
            self.current_middle
                .as_deref()
                .expect("the middle bucket the cursor points into is loaded")
        })
    }

    /// The number of elements in the stored bucket at relative `offset`. Middle buckets
    /// always hold exactly `N` (invariant), so they need not be materialized.
    fn bucket_len(&self, offset: usize) -> usize {
        if offset == 0 {
            self.stored_front.len()
        } else if offset + 1 == self.stored_num_buckets as usize {
            self.stored_back.len()
        } else {
            N
        }
    }

    /// Classifies the pending save based on the current view state.
    /// Called once by `pre_save` and once by `post_save`; since `&self` is
    /// unchanged between the two it returns the same plan both times.
    fn save_plan(&self) -> Result<SavePlan, ViewError> {
        let num_buckets = self.stored_num_buckets as usize;
        let remaining_offset = if self.delete_storage_first {
            num_buckets
        } else {
            self.cursor.map_or(num_buckets, |c| c.offset)
        };
        let remaining_count = num_buckets - remaining_offset;
        let cursor_position = u32::try_from(self.cursor.map_or(0, |c| c.position))
            .map_err(|_| ArithmeticError::Overflow)?;
        let has_storage = self.stored_num_buckets > 0 || self.delete_storage_first;
        let new_back_empty = self.new_back_values.is_empty();

        let case = if remaining_count == 0 && new_back_empty {
            SaveCase::Empty
        } else if remaining_count == 1 && remaining_offset == 0 && new_back_empty {
            SaveCase::MetadataOnly
        } else if remaining_count <= 1 {
            SaveCase::Rewrite
        } else {
            SaveCase::Patch {
                new_first_index: self.stored_first_index
                    + u32::try_from(remaining_offset).map_err(|_| ArithmeticError::Overflow)?,
                remaining_count: u32::try_from(remaining_count)
                    .map_err(|_| ArithmeticError::Overflow)?,
            }
        };
        Ok(SavePlan {
            case,
            remaining_offset,
            cursor_position,
            has_storage,
        })
    }

    /// Splits `data` into N-sized chunks and writes them as front (KeyTag::Front),
    /// middles (KeyTag::Middle, starting at `first_index`+1), and back (KeyTag::Back).
    /// Returns the total number of buckets written. The caller is responsible for
    /// writing the matching `BucketLayout` entry. Used by `Rewrite`.
    fn write_chunks(
        &self,
        batch: &mut Batch,
        data: &[T],
        first_index: u32,
    ) -> Result<u32, ViewError>
    where
        T: Serialize + Clone,
    {
        let chunks = data.chunks(N).collect::<Vec<_>>();
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

    /// Gets the number of entries that are in the container and in storage.
    fn stored_count(&self) -> usize {
        if self.delete_storage_first {
            return 0;
        }
        let Some(cursor) = self.cursor else {
            return 0;
        };
        let remaining = self.stored_num_buckets as usize - cursor.offset;
        // Current front bucket: count the valid elements after the cursor position.
        let front_count = self.bucket_len(cursor.offset) - cursor.position;
        if remaining == 1 {
            return front_count;
        }
        // Back bucket plus the full middles between the cursor and the back.
        let back_count = self.stored_back.len();
        let num_middles = remaining - 2;
        front_count + num_middles * N + back_count
    }

    /// The total number of entries of the container.
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
            Some(cursor) => {
                let data = self
                    .current_front_data()
                    .expect("cursor is Some, so the current front is available");
                Some(&data[cursor.position])
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
            Some(cursor) => {
                let num_buckets = self.stored_num_buckets as usize;
                let data = if cursor.offset == 0 {
                    &mut self.stored_front
                } else if cursor.offset + 1 == num_buckets {
                    &mut self.stored_back
                } else {
                    self.current_middle
                        .as_mut()
                        .expect("the middle bucket the cursor points into is loaded")
                };
                Some(
                    data.get_mut(cursor.position)
                        .expect("cursor.position must be a valid position within the front bucket"),
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
        let Some(cursor) = self.cursor else {
            self.new_back_values.pop_front();
            return Ok(());
        };
        let current_len = self
            .current_front_data()
            .expect("cursor points into the stored buckets")
            .len();
        let num_buckets = self.stored_num_buckets as usize;
        let mut offset = cursor.offset;
        let mut position = cursor.position + 1;
        if position == current_len {
            offset += 1;
            position = 0;
            if offset == num_buckets {
                // The stored portion is now exhausted.
                self.cursor = None;
                self.current_middle = None;
                return Ok(());
            }
            // The cursor crossed into the bucket at `offset` (>= 1). Materialize it as the
            // new front *before* moving the cursor, so a failed load leaves the view's
            // invariant intact (the current front bucket is always materialized).
            if offset + 1 == num_buckets {
                // The back bucket is already materialized.
                self.current_middle = None;
            } else {
                let index = self.stored_first_index
                    + u32::try_from(offset).map_err(|_| ArithmeticError::Overflow)?;
                let key = self.get_middle_key(index)?;
                let data = self
                    .context
                    .store()
                    .read_value(&key)
                    .await?
                    .ok_or_else(|| {
                        ViewError::MissingEntries("BucketQueueView::delete_front".into())
                    })?;
                self.current_middle = Some(data);
            }
        }
        self.cursor = Some(Cursor { offset, position });
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
        // The last stored element is the back bucket's last (or the front's, for a
        // single stored bucket).
        let last = if self.stored_num_buckets >= 2 {
            self.stored_back.last()
        } else {
            self.stored_front.last()
        };
        Ok(last.cloned())
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
            let num_buckets = self.stored_num_buckets as usize;
            // First pass: gather the storage keys of the middle buckets we will read.
            let mut keys = Vec::new();
            let mut position = cursor.position;
            let mut remain = count;
            for offset in cursor.offset..num_buckets {
                if offset != 0 && offset + 1 != num_buckets {
                    let index = self.stored_first_index
                        + u32::try_from(offset).map_err(|_| ArithmeticError::Overflow)?;
                    keys.push(self.get_middle_key(index)?);
                }
                let size = self.bucket_len(offset) - position;
                if size >= remain {
                    break;
                }
                remain -= size;
                position = 0;
            }
            let values = self.context.store().read_multi_values_bytes(&keys).await?;
            // Second pass: assemble the elements, reading middles from `values`.
            let mut value_pos = 0;
            let mut position = cursor.position;
            for offset in cursor.offset..num_buckets {
                let read_buf;
                let data: &[T] = if offset == 0 {
                    &self.stored_front
                } else if offset + 1 == num_buckets {
                    &self.stored_back
                } else {
                    let value = values[value_pos].as_ref().ok_or_else(|| {
                        ViewError::MissingEntries("BucketQueueView::read_context".into())
                    })?;
                    value_pos += 1;
                    read_buf = bcs::from_bytes::<Vec<T>>(value)?;
                    &read_buf
                };
                let size = data.len() - position;
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
            let num_buckets = self.stored_num_buckets as usize;
            let mut position = cursor.position;
            for offset in cursor.offset..num_buckets {
                let size = self.bucket_len(offset) - position;
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
            self.current_middle = None;
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

    /// Regression test: a failed load while advancing the cursor in `delete_front`
    /// must not leave the view in a state where the current front bucket is not
    /// materialized. The next bucket is loaded *before* the cursor advances, so a
    /// failed load leaves the cursor (and `current_middle`) untouched.
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
            let expected = (0..u32::try_from(size).unwrap()).collect::<Vec<_>>();
            assert_eq!(elements, expected, "size = {size}");
            assert_eq!(reloaded.count(), size, "count for size = {size}");
        }
        Ok(())
    }

    /// Middle buckets must always contain exactly `N` elements after save — this is
    /// the invariant that lets the layout track them by count alone. Read the middle
    /// bucket keys back from storage and check each holds `N` elements.
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

        // After all this, every middle bucket in storage must hold exactly N elements.
        let view = BucketQueueView::<_, u32, N>::load(context.clone()).await?;
        let first_index = view.stored_first_index;
        for offset in 1..view.stored_num_buckets.saturating_sub(1) {
            let key = view.get_middle_key(first_index + offset)?;
            let data = context
                .store()
                .read_value::<Vec<u32>>(&key)
                .await?
                .expect("middle bucket should be present in storage");
            assert_eq!(
                data.len(),
                N,
                "middle at offset {offset} should hold N elements"
            );
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

    /// `clear()` followed by `rollback()` must restore the last-saved state,
    /// including a non-zero saved front position. `rollback` is synchronous and
    /// rebuilds the cursor purely from the in-memory saved layout, so `clear` must
    /// preserve it.
    #[tokio::test]
    async fn rollback_after_clear_restores_front_position() -> Result<(), ViewError> {
        const N: usize = 5;
        let context = MemoryContext::new_for_testing(());
        let mut view = BucketQueueView::<_, u32, N>::load(context.clone()).await?;
        for value in [10u32, 20, 30] {
            view.push_back(value);
        }
        save(&context, &mut view).await?;

        // Advance the saved front past position 0 (single bucket -> MetadataOnly).
        let mut view = BucketQueueView::<_, u32, N>::load(context.clone()).await?;
        view.delete_front().await?; // drop 10
        save(&context, &mut view).await?;

        let mut view = BucketQueueView::<_, u32, N>::load(context).await?;
        assert_eq!(view.elements().await?, vec![20, 30]);

        view.clear();
        assert_eq!(view.elements().await?, Vec::<u32>::new());

        view.rollback();
        assert_eq!(view.elements().await?, vec![20, 30]);
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
