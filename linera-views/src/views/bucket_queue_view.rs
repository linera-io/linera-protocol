// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{vec_deque::IterMut, VecDeque},
    fmt::Debug,
};

use async_trait::async_trait;
use serde::{Deserialize};
use serde::{de::DeserializeOwned, Serialize};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{self, MeasureLatency},
    prometheus::HistogramVec,
};

use crate::{
    batch::Batch,
    common::{from_bytes_option, from_bytes_option_or_default, MIN_VIEW_TAG},
    context::Context,
    views::{ClonableView, HashableView, Hasher, View, ViewError},
};

/// Key tags to create the sub-keys of a BucketQueueView on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the front of the view
    Front = MIN_VIEW_TAG,
    /// Prefix for the storing of the stored-indices information
    Store,
    /// Prefix for the indices of the log.
    Index,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct StoredIndices {
    indices: Vec<(usize,usize)>,
    position: usize,
}

impl StoredIndices {
    fn is_empty(&self) -> bool {
        self.indices.len() == 0
    }
}

#[derive(Debug, Clone)]
struct Cursor {
    position: Option<(usize,usize)>,
}

impl Cursor {
    pub fn new(stored_indices: &StoredIndices) -> Self {
        if stored_indices.indices.len() == 0 {
            Cursor { position: None }
        } else {
            Cursor { position: Some((0, stored_indices.position)) }
        }
    }

    pub fn is_incrementable(&self) -> bool {
        self.position.is_some()
    }
}

/// A view that supports a FIFO queue for values of type `T`.
pub struct BucketQueueView<C, T, const N: usize> {
    context: C,
    data: Vec<Option<Vec<T>>>,
    new_back_values: VecDeque<T>,
    stored_indices: StoredIndices,
    cursor: Cursor,
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
        println!("pre_load key1(front)={:?}", key1);
        println!("pre_load key2(store)={:?}", key2);
        Ok(vec![key1, key2])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let value1 = values.first().ok_or(ViewError::PostLoadValuesError)?;
        let value2 = values.get(1).ok_or(ViewError::PostLoadValuesError)?;
        let front = from_bytes_option::<Vec<T>, _>(value1)?;
        let mut data = match front {
            Some(front) => {
                println!("post_load, |front|={}", front.len());
                vec![Some(front)]
            },
            None => {
                println!("post_load, None case");
                vec![]
            },
        };
        let stored_indices = from_bytes_option_or_default::<StoredIndices, _>(value2)?;
        for _ in 1..stored_indices.indices.len() {
            data.push(None);
        }
        let cursor = Cursor::new(&stored_indices);
        println!("post_load stored_indices={:?}", stored_indices);
        println!("post_load         cursor={:?}", cursor);
        Ok(Self {
            context,
            data,
            new_back_values: VecDeque::new(),
            stored_indices,
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
        self.cursor = Cursor::new(&self.stored_indices);
        self.new_back_values.clear();
    }

    async fn has_pending_changes(&self) -> bool {
        println!("---------------- has_pending_changes --------------");
        if self.delete_storage_first {
            println!("has_pending_changes, exit 1");
            return true;
        }
        println!("self.cursor={:?}", self.cursor);
        println!("self.stored_indices={:?}", self.stored_indices);
        if self.stored_indices.indices.len() > 0 {
            let Some((i_block, position)) = self.cursor.position else {
                println!("has_pending_changes, exit 2");
                return true;
            };
            if i_block != 0 || position != self.stored_indices.position {
                println!("has_pending_changes i_block={} position={} stored_indices.position={}", i_block, position, self.stored_indices.position);
                println!("has_pending_changes, exit 3");
                return true;
            }
        }
        println!("has_pending_changes, |new_back_values|={}", self.new_back_values.len());
        !self.new_back_values.is_empty()
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        println!("------- F L U S H (start) ---------");
        println!("flush stored_indices={:?}", self.stored_indices);
        println!("flush cursor={:?}", self.cursor);
        println!("flush |new_back_values|={}", self.new_back_values.len());
        assert_eq!(self.data.len(), self.stored_indices.indices.len());
        if self.delete_storage_first {
            let key_prefix = self.context.base_key();
            println!("flush delete_key_prefix 1 for key_prefix={:?}", key_prefix);
            batch.delete_key_prefix(key_prefix);
            delete_view = true;
        }
        println!("flush stored_count={}", self.stored_count());
        if self.stored_count() == 0 {
            let key_prefix = self.context.base_key();
            println!("flush delete_key_prefix 2 for key_prefix={:?}", key_prefix);
            batch.delete_key_prefix(key_prefix);
            self.stored_indices = StoredIndices::default();
            self.data.clear();
        } else {
            if let Some((i_block, position)) = self.cursor.position {
                println!("flush i_block={} position={}", i_block, position);
                for block in 0..i_block {
                    let index = self.stored_indices.indices[block].1;
                    println!("flush block={} index={}", block, index);
                    let key = self.get_index_key(index)?;
                    batch.delete_key(key);
                }
                let indices = self.stored_indices.indices[i_block..].to_vec();
                println!("flush before |self.data|={}", self.data.len());
                self.data.drain(0..i_block);
                println!("flush after |self.data|={}", self.data.len());
                println!("flush indices={:?}", indices);
                self.stored_indices = StoredIndices { indices, position };
                self.cursor = Cursor { position: Some((0, position)) };
                println!("flush stored_indices 1={:?}", self.stored_indices);
                // We need to ensure that the first index is in the front.
                let first_index = self.stored_indices.indices[0].1;
                println!("flush first_index={}", first_index);
                if first_index != 0 {
                    let size = self.stored_indices.indices[0].0;
                    self.stored_indices.indices[0] = (size, 0);
                    let key = self.get_index_key(first_index)?;
                    println!("flush   key(fi)={:?}", key);
                    batch.delete_key(key);
                    let key = self.get_index_key(0)?;
                    println!("flush   key(0)={:?}", key);
                    let data0 = self.data.first().unwrap().as_ref().unwrap();
                    println!("flush |data0|={}", data0.len());
                    batch.put_key_value(key, &data0)?;
                }
            }
        }
        if !self.new_back_values.is_empty() {
            delete_view = false;
            let mut unused_index = match self.stored_indices.indices.last() {
                Some((_, index)) => index + 1,
                None => 0,
            };
            println!("flush unused_index={}", unused_index);
            let new_back_values : VecDeque<T> = std::mem::take(&mut self.new_back_values);
            let new_back_values : Vec<T> = new_back_values.into_iter().collect::<Vec<_>>();
            for value_chunk in new_back_values.chunks(N) {
                println!("flush index={} |value_chunk|={}", unused_index, value_chunk.len());
                self.stored_indices.indices.push((value_chunk.len(), unused_index));
                let value_chunk = value_chunk.to_vec();
                let key = self.get_index_key(unused_index)?;
                batch.put_key_value(key, &value_chunk)?;
                self.data.push(Some(value_chunk));
                unused_index += 1;
            }
            if !self.cursor.is_incrementable() {
                self.cursor = Cursor { position: Some((0,0)) }
            }
            println!("flush |new_back_values|={}", self.new_back_values.len());
        }
        println!("flush stored_indices 2 : {:?}", self.stored_indices);
        if !self.delete_storage_first || !self.stored_indices.is_empty() {
            let key = self.context.base_tag(KeyTag::Store as u8);
            batch.put_key_value(key, &self.stored_indices)?;
        }
        self.delete_storage_first = false;
        println!("flush batch={:?}", batch);
        println!("flush delete_view={}", delete_view);
        for i in 0..self.data.len() {
            match &self.data[i] {
                Some(vec) => {
                    println!("  i={} |self.data[u]|={}", i, vec.len());
                },
                None => {
                    println!("  i={} None", i);
                }
            }
        }
        println!("------- F L U S H (end) ---------");
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
            data: self.data.clone(),
	    new_back_values: self.new_back_values.clone(),
            stored_indices: self.stored_indices.clone(),
	    cursor: self.cursor.clone(),
            delete_storage_first: self.delete_storage_first,
        })
    }
}

impl<'a, C, T, const N: usize> BucketQueueView<C, T, N>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
{
    /// Get the key corresponding to the index
    fn get_index_key(&self, index: usize) -> Result<Vec<u8>, ViewError> {
        Ok(if index == 0 {
            self.context.base_tag(KeyTag::Front as u8)
        } else {
            self.context.derive_tag_key(KeyTag::Index as u8, &index)?
        })
    }

    /// Get the stored_count
    fn stored_count(&self) -> usize {
        if self.delete_storage_first {
            0
        } else {
            let Some((i_block, position)) = self.cursor.position else {
                return 0;
            };
            let mut stored_count = 0;
            for block in i_block..self.stored_indices.indices.len() {
                stored_count += self.stored_indices.indices[block].0;
            }
            stored_count -= position;
            stored_count
        }
    }

    /// The total number of entries of the container
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
    /// assert_eq!(queue.front(), Some(34));
    /// # })
    /// ```
    pub fn front(&self) -> Option<T> {
        match self.cursor.position {
            Some((i_block, position)) => {
                let block = &self.data[i_block];
                let block = block.as_ref().unwrap();
                Some(block[position].clone())
            },
            None => {
                self.new_back_values.front().cloned()
            },
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
        println!("delete_front beginning");
        match self.cursor.position {
            Some((mut i_block, mut position)) => {
                position += 1;
                if self.stored_indices.indices[i_block].0 == position {
                    i_block += 1;
                    position = 0;
                }
                if i_block == self.stored_indices.indices.len() {
                    self.cursor = Cursor { position: None };
                } else {
                    self.cursor = Cursor { position: Some((i_block, position)) };
                    if self.data[i_block].is_none() {
                        let index = self.stored_indices.indices[i_block].1;
                        let key = self.get_index_key(index)?;
                        let value = self.context.read_value_bytes(&key).await?;
                        let value = value.ok_or(ViewError::MissingEntries)?;
                        let value = bcs::from_bytes(&value)?;
                        self.data[i_block] = Some(value);
                    }
                }
            },
            None => {
                self.new_back_values.pop_front();
            },
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
        let Some((len, index)) = self.stored_indices.indices.last() else {
            return Ok(None);
        };
        if let Some(vec) = self.data.last().unwrap() {
            return Ok(Some(vec.last().unwrap().clone()));
        }
        let key = self.get_index_key(*index)?;
        let value = self.context.read_value_bytes(&key).await?;
        let value = value.as_ref().ok_or(ViewError::MissingEntries)?;
        let value = bcs::from_bytes::<Vec<T>>(&value)?;
        Ok(Some(value[len-1].clone()))
    }

    async fn read_context(&self, position: Option<(usize,usize)>, count: usize) -> Result<Vec<T>, ViewError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut elements = Vec::<T>::new();
        let mut count_remain = count;
        if let Some(pair) = position {
            let mut keys = Vec::new();
            let (mut i_block, mut position) = pair.clone();
            println!("read_front: 1, i_block={} position={} len={}", i_block, position, self.data.len());
            for block in i_block..self.data.len() {
                let size = self.stored_indices.indices[block].0 - position;
                println!("read_front:  block={} position={} size={}", block, position, size);
                if self.data[block].is_none() {
                    let index = self.stored_indices.indices[block].1;
                    let key = self.get_index_key(index)?;
                    keys.push(key);
                }
                if size >= count_remain {
                    break;
                }
                count_remain -= size;
                i_block += 1;
                position = 0;
            }
            let values = self.context.read_multi_values_bytes(keys).await?;
            let (mut i_block, mut position) = pair.clone();
            println!("read_front: 2, i_block={} position={}", i_block, position);
            let mut pos = 0;
            count_remain = count;
            for block in i_block..self.data.len() {
                let size = self.stored_indices.indices[block].0 - position;
                let vec = match &self.data[block] {
                    Some(vec) => {
                        vec
                    },
                    None => {
                        let value = values[pos].as_ref().ok_or(ViewError::MissingEntries)?;
                        pos += 1;
                        &bcs::from_bytes::<Vec<T>>(&value)?
                    },
                };
                let end = if count_remain <= size {
                    position + count_remain
                } else {
                    position + size
                };
                println!("read_front  block={}  start={} end={} size={}", block, position, end, size);
                for element in &vec[position..end] {
                    elements.push(element.clone());
                }
                println!("read_front  |elements|={} size={} count_remain={}", elements.len(), size, count_remain);
                if size >= count_remain {
                    count_remain = 0;
                    break;
                }
                count_remain -= size;
                i_block += 1;
                position = 0;
            }
        }
        if count_remain > 0 {
            let count_read = std::cmp::min(count_remain, self.new_back_values.len());
            elements.extend(self.new_back_values.range(0..count_read).cloned());
        }
        Ok(elements)
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
    /// assert_eq!(queue.read_front(2).await.unwrap(), vec![34, 37]);
    /// # })
    /// ```
    pub async fn read_back(&self, count: usize) -> Result<Vec<T>, ViewError> {
        if count <= self.new_back_values.len() {
            let start = self.new_back_values.len() - count;
            Ok(self.new_back_values.range(start..).cloned().collect::<Vec<_>>())
        } else {
            println!("count={} self.count()={}", count, self.count());
            let mut increment = self.count() - count;
            let Some((i_block, mut position)) = self.cursor.position else {
                unreachable!();
            };
            for block in i_block..self.data.len() {
                let size = self.stored_indices.indices[block].0 - position;
                if increment < size {
                    return self.read_context(Some((block, position + increment)), count).await
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
            println!("load_all, |elements|={}", elements.len());
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
    /// let mut queue = QueueView::<_,u8, 5>::load(context).await.unwrap();
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
        let _hash_latency = QUEUE_VIEW_HASH_RUNTIME.measure_latency();
        let elements = self.elements().await?;
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bcs_bytes(&elements)?;
        Ok(hasher.finalize())
    }
}

