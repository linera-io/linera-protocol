// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A set of functionalities for building batches to be written into the database.
//! A batch can contain three kinds of operations on a key/value store:
//! * Insertion of a key with an associated value
//! * Deletion of a specific key
//! * Deletion of all keys which contain a specified prefix
//!
//! The deletion using prefixes is generally but not always faster than deleting keys
//! one by one. The only purpose of the batch is to write some transactions into the
//! database.
//!
//! Note that normal users should not have to manipulate batches. The functionality
//! is public because some other libraries require it. But the users using views should
//! not have to deal with batches.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::Debug,
    iter::Peekable,
    ops::Bound,
    vec::IntoIter,
};

use async_trait::async_trait;
use bcs::serialized_size;
use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Serialize};

use crate::{
    common::{get_interval, get_uleb128_size},
    views::ViewError,
};

/// A write operation as requested by a view when it needs to persist staged changes.
/// There are 3 possibilities for the batch:
/// * Deletion of a specific key.
/// * Deletion of all keys matching a specific prefix.
/// * Insertion or replacement of a key with a value.
#[derive(Clone, Debug, Eq, PartialEq, WitType, WitLoad, WitStore)]
pub enum WriteOperation {
    /// Delete the given key.
    Delete {
        /// The key that will be deleted.
        key: Vec<u8>,
    },
    /// Delete all the keys matching the given prefix.
    DeletePrefix {
        /// The prefix of the keys to be deleted.
        key_prefix: Vec<u8>,
    },
    /// Set or replace the value of a given key.
    Put {
        /// The key to be inserted or replaced.
        key: Vec<u8>,
        /// The value to be inserted on the key.
        value: Vec<u8>,
    },
}

/// A batch of write operations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Batch {
    /// The write operations.
    pub operations: Vec<WriteOperation>,
}

/// A batch of deletions and insertions that operate on disjoint keys, thus can be
/// executed in any order.
#[derive(Default, Serialize, Deserialize)]
pub struct SimpleUnorderedBatch {
    /// The deletions.
    pub deletions: Vec<Vec<u8>>,
    /// The insertions.
    pub insertions: Vec<(Vec<u8>, Vec<u8>)>,
}

/// An unordered batch of deletions and insertions, together with a set of key-prefixes to
/// delete. Key-prefix deletions must happen before the insertions and the deletions.
#[derive(Default, Serialize, Deserialize)]
pub struct UnorderedBatch {
    /// The key-prefix deletions.
    pub key_prefix_deletions: Vec<Vec<u8>>,
    /// The batch of deletions and insertions.
    pub simple_unordered_batch: SimpleUnorderedBatch,
}

impl UnorderedBatch {
    /// From an `UnorderedBatch`, creates a [`SimpleUnorderedBatch`] that does not contain the
    /// `key_prefix_deletions`. This requires accessing the database to eliminate them.
    pub async fn expand_delete_prefixes<DB: DeletePrefixExpander>(
        self,
        db: &DB,
    ) -> Result<SimpleUnorderedBatch, DB::Error> {
        let mut insert_set = HashSet::new();
        for (key, _) in &self.simple_unordered_batch.insertions {
            insert_set.insert(key.clone());
        }
        let insertions = self.simple_unordered_batch.insertions;
        let mut deletions = self.simple_unordered_batch.deletions;
        for key_prefix in self.key_prefix_deletions {
            for short_key in db.expand_delete_prefix(&key_prefix).await?.iter() {
                let mut key = key_prefix.clone();
                key.extend(short_key);
                if !insert_set.contains(&key) {
                    deletions.push(key);
                }
            }
        }
        Ok(SimpleUnorderedBatch {
            deletions,
            insertions,
        })
    }

    /// Modifies an [`UnorderedBatch`] so that the key-prefix deletions do not conflict
    /// with subsequent insertions. This may require accessing the database to compute
    /// lists of deleted keys.
    pub async fn expand_colliding_prefix_deletions<DB: DeletePrefixExpander>(
        &mut self,
        db: &DB,
    ) -> Result<(), DB::Error> {
        if self.key_prefix_deletions.is_empty() {
            return Ok(());
        }
        let inserted_keys = self
            .simple_unordered_batch
            .insertions
            .iter()
            .map(|x| x.0.clone())
            .collect::<BTreeSet<_>>();
        let mut key_prefix_deletions = Vec::new();
        for key_prefix in &self.key_prefix_deletions {
            if inserted_keys
                .range(get_interval(key_prefix.clone()))
                .next()
                .is_some()
            {
                for short_key in db.expand_delete_prefix(key_prefix).await?.iter() {
                    let mut key = key_prefix.clone();
                    key.extend(short_key);
                    if !inserted_keys.contains(&key) {
                        self.simple_unordered_batch.deletions.push(key);
                    }
                }
            } else {
                key_prefix_deletions.push(key_prefix.to_vec());
            }
        }
        self.key_prefix_deletions = key_prefix_deletions;
        Ok(())
    }

    /// The total number of entries of the batch.
    pub fn len(&self) -> usize {
        self.key_prefix_deletions.len() + self.simple_unordered_batch.len()
    }

    /// Tests whether the batch is empty or not
    pub fn is_empty(&self) -> bool {
        self.key_prefix_deletions.is_empty() && self.simple_unordered_batch.is_empty()
    }
}

/// Checks if `key` is matched by any prefix in `key_prefix_set`.
/// The set `key_prefix_set` must be minimal for the function to work correctly.
/// That is, there should not be any two prefixes p1 and p2 such that p1 < p2 for
/// the lexicographic ordering on `Vec<u8>` entries.
/// Under this condition we have equivalence between the following two statements:
/// * There is an key_prefix in `key_prefiw_set` that matches `key`.
/// * The highest key_prefix in `key_prefix_set` is actually matching.
fn is_prefix_matched(key_prefix_set: &BTreeSet<Vec<u8>>, key: &[u8]) -> bool {
    let range = (Bound::Unbounded, Bound::Included(key.to_vec()));
    let range = key_prefix_set.range(range);
    if let Some(value) = range.last() {
        if value.len() > key.len() {
            return false;
        }
        return value == &key[0..value.len()];
    }
    false
}

impl Batch {
    /// Creates an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// The total size of the batch
    pub fn size(&self) -> usize {
        self.operations
            .iter()
            .map(|operation| match operation {
                WriteOperation::Delete { key } => key.len(),
                WriteOperation::Put { key, value } => key.len() + value.len(),
                WriteOperation::DeletePrefix { key_prefix } => key_prefix.len(),
            })
            .sum()
    }

    /// Whether the batch is empty or not
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Returns the number of operations in this [`Batch`].
    pub fn num_operations(&self) -> usize {
        self.operations.len()
    }

    /// Builds a batch from a builder function.
    pub async fn build<F>(builder: F) -> Result<Self, ViewError>
    where
        F: FnOnce(&mut Batch) -> futures::future::BoxFuture<Result<(), ViewError>> + Send + Sync,
    {
        let mut batch = Batch::new();
        builder(&mut batch).await?;
        Ok(batch)
    }

    /// Simplifies the batch by removing operations that are overwritten by others.
    ///
    /// A key may appear multiple times in the batch, as an insert, a delete
    /// or matched by a delete prefix.
    /// ```rust
    /// # use linera_views::batch::Batch;
    /// let mut batch = Batch::new();
    /// batch.put_key_value(vec![0, 1], &(34 as u128));
    /// batch.delete_key(vec![0, 1]);
    /// let unordered_batch = batch.simplify();
    /// assert_eq!(unordered_batch.key_prefix_deletions.len(), 0);
    /// assert_eq!(unordered_batch.simple_unordered_batch.insertions.len(), 0);
    /// assert_eq!(unordered_batch.simple_unordered_batch.deletions.len(), 1);
    /// ```
    pub fn simplify(self) -> UnorderedBatch {
        let mut delete_and_insert_map = BTreeMap::new();
        let mut delete_prefix_set = BTreeSet::new();
        for operation in self.operations {
            match operation {
                WriteOperation::Delete { key } => {
                    // If `key` is matched by a deleted prefix, then remove any inserted
                    // value. Otherwise, add the key to the set of deletions.
                    if is_prefix_matched(&delete_prefix_set, &key) {
                        delete_and_insert_map.remove(&key);
                    } else {
                        delete_and_insert_map.insert(key, None);
                    }
                }
                WriteOperation::Put { key, value } => {
                    // Record the insertion.
                    delete_and_insert_map.insert(key, Some(value));
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    // Remove the previous deletions and insertions covered by `key_prefix`.
                    let keys = delete_and_insert_map
                        .range(get_interval(key_prefix.clone()))
                        .map(|x| x.0.to_vec())
                        .collect::<Vec<_>>();
                    for key in keys {
                        delete_and_insert_map.remove(&key);
                    }
                    // If `key_prefix` is covered by a previous deleted prefix, then we're done.
                    if is_prefix_matched(&delete_prefix_set, &key_prefix) {
                        continue;
                    }
                    // Otherwise, find the prefixes that are covered by the new key
                    // prefix.
                    let key_prefixes = delete_prefix_set
                        .range(get_interval(key_prefix.clone()))
                        .map(|x: &Vec<u8>| x.to_vec())
                        .collect::<Vec<_>>();
                    // Delete them.
                    for key_prefix in key_prefixes {
                        delete_prefix_set.remove(&key_prefix);
                    }
                    // Then, insert the new key prefix.
                    delete_prefix_set.insert(key_prefix);
                }
            }
        }
        let key_prefix_deletions = delete_prefix_set.into_iter().collect();
        let mut deletions = Vec::new();
        let mut insertions = Vec::new();
        for (key, val) in delete_and_insert_map {
            match val {
                Some(value) => insertions.push((key, value)),
                None => deletions.push(key),
            }
        }
        let simple_unordered_batch = SimpleUnorderedBatch {
            deletions,
            insertions,
        };
        UnorderedBatch {
            key_prefix_deletions,
            simple_unordered_batch,
        }
    }

    /// Checks the size of the values of the batch.
    pub fn check_value_size(&self, max_value_size: usize) -> bool {
        for operation in &self.operations {
            if let WriteOperation::Put { key: _, value } = operation {
                if value.len() > max_value_size {
                    return false;
                }
            }
        }
        true
    }

    /// Adds the insertion of a key-value pair into the batch with a serializable value.
    /// ```rust
    /// # use linera_views::batch::Batch;
    /// let mut batch = Batch::new();
    /// batch.put_key_value(vec![0, 1], &(34 as u128));
    /// ```
    #[inline]
    pub fn put_key_value(
        &mut self,
        key: Vec<u8>,
        value: &impl Serialize,
    ) -> Result<(), bcs::Error> {
        let bytes = bcs::to_bytes(value)?;
        self.put_key_value_bytes(key, bytes);
        Ok(())
    }

    /// Adds the insertion of a `(key, value)` pair into the batch with `value` a vector of `u8`.
    /// ```rust
    /// # use linera_views::batch::Batch;
    /// let mut batch = Batch::new();
    /// batch.put_key_value_bytes(vec![0, 1], vec![3, 4, 5]);
    /// ```
    #[inline]
    pub fn put_key_value_bytes(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push(WriteOperation::Put { key, value });
    }

    /// Inserts the deletion of a `key` into the batch.
    /// ```rust
    /// # use linera_views::batch::Batch;
    /// let mut batch = Batch::new();
    /// batch.delete_key(vec![0, 1]);
    /// ```
    #[inline]
    pub fn delete_key(&mut self, key: Vec<u8>) {
        self.operations.push(WriteOperation::Delete { key });
    }

    /// Inserts the deletion of a `key_prefix` into the batch.
    /// ```rust
    /// # use linera_views::batch::Batch;
    /// let mut batch = Batch::new();
    /// batch.delete_key_prefix(vec![0, 1]);
    /// ```
    #[inline]
    pub fn delete_key_prefix(&mut self, key_prefix: Vec<u8>) {
        self.operations
            .push(WriteOperation::DeletePrefix { key_prefix });
    }
}

/// A trait to expand delete_prefix operations.
/// Certain databases (e.g. DynamoDB) do not support the deletion by prefix.
/// Thus we need to access the databases in order to replace a `DeletePrefix`
/// by a vector of the keys to be removed.
#[trait_variant::make(DeletePrefixExpander: Send)]
pub trait LocalDeletePrefixExpander {
    /// The error type that can happen when expanding the key_prefix.
    type Error: Debug;

    /// Returns the list of keys to be appended to the list.
    async fn expand_delete_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error>;
}

/// A notion of batch useful for certain computations (notably journaling).
#[async_trait]
pub trait SimplifiedBatch: Sized + Send + Sync {
    /// The iterator type used to process values from the batch.
    type Iter: BatchValueWriter<Self>;

    /// Creates a simplified batch from a standard one.
    async fn from_batch<S: DeletePrefixExpander + Send + Sync>(
        store: S,
        batch: Batch,
    ) -> Result<Self, S::Error>;

    /// Returns an owning iterator over the values in the batch.
    fn into_iter(self) -> Self::Iter;

    /// Returns the total number of entries in the batch.
    fn len(&self) -> usize;

    /// Returns the total number of bytes in the batch.
    fn num_bytes(&self) -> usize;

    /// Returns the overhead size of the batch.
    fn overhead_size(&self) -> usize;

    /// Adds the deletion of key to the batch.
    fn add_delete(&mut self, key: Vec<u8>);

    /// Adds the insertion of a key-value pair to the batch.
    fn add_insert(&mut self, key: Vec<u8>, value: Vec<u8>);

    /// Returns true if the batch is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// An iterator-like object that can write values one by one to a batch while updating the
/// total size of the batch.
pub trait BatchValueWriter<Batch>: Send + Sync {
    /// Returns true if there are no more values to write.
    fn is_empty(&self) -> bool;

    /// Writes the next value (if any) to the batch and updates the batch size accordingly.
    ///
    /// Returns false if there was no value to write.
    fn write_next_value(
        &mut self,
        batch: &mut Batch,
        batch_size: &mut usize,
    ) -> Result<bool, bcs::Error>;

    /// Computes the batch size that we would obtain if we wrote the next value (if any)
    /// without consuming the value.
    fn next_batch_size(
        &mut self,
        batch: &Batch,
        batch_size: usize,
    ) -> Result<Option<usize>, bcs::Error>;
}

/// The iterator that corresponds to a SimpleUnorderedBatch
pub struct SimpleUnorderedBatchIter {
    delete_iter: Peekable<IntoIter<Vec<u8>>>,
    insert_iter: Peekable<IntoIter<(Vec<u8>, Vec<u8>)>>,
}

#[async_trait]
impl SimplifiedBatch for SimpleUnorderedBatch {
    type Iter = SimpleUnorderedBatchIter;

    fn into_iter(self) -> Self::Iter {
        let delete_iter = self.deletions.into_iter().peekable();
        let insert_iter = self.insertions.into_iter().peekable();
        Self::Iter {
            delete_iter,
            insert_iter,
        }
    }

    fn len(&self) -> usize {
        self.deletions.len() + self.insertions.len()
    }

    fn num_bytes(&self) -> usize {
        let mut total_size = 0;
        for (key, value) in &self.insertions {
            total_size += key.len() + value.len();
        }
        for deletion in &self.deletions {
            total_size += deletion.len();
        }
        total_size
    }

    fn overhead_size(&self) -> usize {
        get_uleb128_size(self.deletions.len()) + get_uleb128_size(self.insertions.len())
    }

    fn add_delete(&mut self, key: Vec<u8>) {
        self.deletions.push(key)
    }

    fn add_insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.insertions.push((key, value))
    }

    async fn from_batch<S: DeletePrefixExpander + Send + Sync>(
        store: S,
        batch: Batch,
    ) -> Result<Self, S::Error> {
        let unordered_batch = batch.simplify();
        unordered_batch.expand_delete_prefixes(&store).await
    }
}

impl BatchValueWriter<SimpleUnorderedBatch> for SimpleUnorderedBatchIter {
    fn is_empty(&self) -> bool {
        self.delete_iter.len() == 0 && self.insert_iter.len() == 0
    }

    fn write_next_value(
        &mut self,
        batch: &mut SimpleUnorderedBatch,
        batch_size: &mut usize,
    ) -> Result<bool, bcs::Error> {
        if let Some(delete) = self.delete_iter.next() {
            *batch_size += serialized_size(&delete)?;
            batch.deletions.push(delete);
            Ok(true)
        } else if let Some((key, value)) = self.insert_iter.next() {
            *batch_size += serialized_size(&key)? + serialized_size(&value)?;
            batch.insertions.push((key, value));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn next_batch_size(
        &mut self,
        batch: &SimpleUnorderedBatch,
        batch_size: usize,
    ) -> Result<Option<usize>, bcs::Error> {
        if let Some(delete) = self.delete_iter.peek() {
            let next_size = serialized_size(&delete)?;
            Ok(Some(
                batch_size
                    + next_size
                    + get_uleb128_size(batch.deletions.len() + 1)
                    + get_uleb128_size(batch.insertions.len()),
            ))
        } else if let Some((key, value)) = self.insert_iter.peek() {
            let next_size = serialized_size(&key)? + serialized_size(&value)?;
            Ok(Some(
                batch_size
                    + next_size
                    + get_uleb128_size(batch.deletions.len())
                    + get_uleb128_size(batch.insertions.len() + 1),
            ))
        } else {
            Ok(None)
        }
    }
}

/// The iterator that corresponds to a SimpleUnorderedBatch
pub struct UnorderedBatchIter {
    delete_prefix_iter: Peekable<IntoIter<Vec<u8>>>,
    insert_deletion_iter: SimpleUnorderedBatchIter,
}

#[async_trait]
impl SimplifiedBatch for UnorderedBatch {
    type Iter = UnorderedBatchIter;

    fn into_iter(self) -> Self::Iter {
        let delete_prefix_iter = self.key_prefix_deletions.into_iter().peekable();
        let insert_deletion_iter = self.simple_unordered_batch.into_iter();
        Self::Iter {
            delete_prefix_iter,
            insert_deletion_iter,
        }
    }

    fn len(&self) -> usize {
        self.key_prefix_deletions.len() + self.simple_unordered_batch.len()
    }

    fn num_bytes(&self) -> usize {
        let mut total_size = self.simple_unordered_batch.num_bytes();
        for prefix_deletion in &self.key_prefix_deletions {
            total_size += prefix_deletion.len();
        }
        total_size
    }

    fn overhead_size(&self) -> usize {
        get_uleb128_size(self.key_prefix_deletions.len())
            + self.simple_unordered_batch.overhead_size()
    }

    fn add_delete(&mut self, key: Vec<u8>) {
        self.simple_unordered_batch.add_delete(key)
    }

    fn add_insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.simple_unordered_batch.add_insert(key, value)
    }

    async fn from_batch<S: DeletePrefixExpander + Send + Sync>(
        store: S,
        batch: Batch,
    ) -> Result<Self, S::Error> {
        let mut unordered_batch = batch.simplify();
        unordered_batch
            .expand_colliding_prefix_deletions(&store)
            .await?;
        Ok(unordered_batch)
    }
}

impl BatchValueWriter<UnorderedBatch> for UnorderedBatchIter {
    fn is_empty(&self) -> bool {
        self.delete_prefix_iter.len() == 0 && self.insert_deletion_iter.is_empty()
    }

    fn write_next_value(
        &mut self,
        batch: &mut UnorderedBatch,
        batch_size: &mut usize,
    ) -> Result<bool, bcs::Error> {
        if let Some(delete_prefix) = self.delete_prefix_iter.next() {
            *batch_size += serialized_size(&delete_prefix)?;
            batch.key_prefix_deletions.push(delete_prefix);
            Ok(true)
        } else {
            self.insert_deletion_iter
                .write_next_value(&mut batch.simple_unordered_batch, batch_size)
        }
    }

    fn next_batch_size(
        &mut self,
        batch: &UnorderedBatch,
        batch_size: usize,
    ) -> Result<Option<usize>, bcs::Error> {
        if let Some(delete_prefix) = self.delete_prefix_iter.peek() {
            let next_size = serialized_size(&delete_prefix)?;
            Ok(Some(
                batch_size
                    + next_size
                    + get_uleb128_size(batch.key_prefix_deletions.len() + 1)
                    + batch.simple_unordered_batch.overhead_size(),
            ))
        } else {
            let batch_size = batch_size + get_uleb128_size(batch.key_prefix_deletions.len());
            self.insert_deletion_iter
                .next_batch_size(&batch.simple_unordered_batch, batch_size)
        }
    }
}

#[cfg(test)]
mod tests {
    use linera_views::{
        batch::{Batch, SimpleUnorderedBatch, UnorderedBatch},
        context::{create_test_memory_context, Context},
    };

    #[test]
    fn test_simplify_batch1() {
        let mut batch = Batch::new();
        batch.put_key_value_bytes(vec![1, 2], vec![]);
        batch.put_key_value_bytes(vec![1, 3, 3], vec![33, 2]);
        batch.put_key_value_bytes(vec![1, 2, 3], vec![34, 2]);
        batch.delete_key_prefix(vec![1, 2]);
        let unordered_batch = batch.simplify();
        assert_eq!(unordered_batch.key_prefix_deletions, vec![vec![1, 2]]);
        assert!(unordered_batch.simple_unordered_batch.deletions.is_empty());
        assert_eq!(
            unordered_batch.simple_unordered_batch.insertions,
            vec![(vec![1, 3, 3], vec![33, 2])]
        );
    }

    #[test]
    fn test_simplify_batch2() {
        let mut batch = Batch::new();
        batch.delete_key(vec![1, 2, 3]);
        batch.delete_key_prefix(vec![1, 2]);
        batch.delete_key(vec![1, 2, 4]);
        let unordered_batch = batch.simplify();
        assert_eq!(unordered_batch.key_prefix_deletions, vec![vec![1, 2]]);
        assert!(unordered_batch.simple_unordered_batch.deletions.is_empty());
        assert!(unordered_batch.simple_unordered_batch.insertions.is_empty());
    }

    #[test]
    fn test_simplify_batch3() {
        let mut batch = Batch::new();
        batch.delete_key_prefix(vec![1, 2]);
        batch.put_key_value_bytes(vec![1, 2, 3, 4], vec![]);
        batch.delete_key_prefix(vec![1, 2, 3]);
        let unordered_batch = batch.simplify();
        assert_eq!(unordered_batch.key_prefix_deletions, vec![vec![1, 2]]);
        assert!(unordered_batch.simple_unordered_batch.deletions.is_empty());
        assert!(unordered_batch.simple_unordered_batch.insertions.is_empty());
    }

    #[test]
    fn test_simplify_batch4() {
        let mut batch = Batch::new();
        batch.delete_key_prefix(vec![1, 2]);
        batch.put_key_value_bytes(vec![1, 2, 3], vec![4, 5]);
        batch.delete_key(vec![1, 2, 3]);
        let unordered_batch = batch.simplify();
        assert_eq!(unordered_batch.key_prefix_deletions, vec![vec![1, 2]]);
        assert!(unordered_batch.simple_unordered_batch.deletions.is_empty());
        assert!(unordered_batch.simple_unordered_batch.insertions.is_empty());
    }

    #[tokio::test]
    async fn test_simplify_batch5() {
        let context = create_test_memory_context();
        let mut batch = Batch::new();
        batch.put_key_value_bytes(vec![1, 2, 3], vec![]);
        batch.put_key_value_bytes(vec![1, 2, 4], vec![]);
        batch.put_key_value_bytes(vec![1, 2, 5], vec![]);
        batch.put_key_value_bytes(vec![1, 3, 3], vec![]);
        context.write_batch(batch).await.unwrap();
        let mut batch = Batch::new();
        batch.delete_key_prefix(vec![1, 2]);
        let unordered_batch = batch.simplify();
        let simple_unordered_batch = unordered_batch
            .expand_delete_prefixes(&context)
            .await
            .unwrap();
        assert_eq!(
            simple_unordered_batch.deletions,
            vec![vec![1, 2, 3], vec![1, 2, 4], vec![1, 2, 5]]
        );
        assert!(simple_unordered_batch.insertions.is_empty());
    }

    #[tokio::test]
    async fn test_simplify_batch6() {
        let context = create_test_memory_context();
        let insertions = vec![(vec![1, 2, 3], vec![])];
        let simple_unordered_batch = SimpleUnorderedBatch {
            insertions: insertions.clone(),
            deletions: vec![],
        };
        let key_prefix_deletions = vec![vec![1, 2]];
        let mut unordered_batch = UnorderedBatch {
            simple_unordered_batch,
            key_prefix_deletions,
        };
        unordered_batch
            .expand_colliding_prefix_deletions(&context)
            .await
            .unwrap();
        assert!(unordered_batch.simple_unordered_batch.deletions.is_empty());
        assert_eq!(
            unordered_batch.simple_unordered_batch.insertions,
            insertions
        );
        assert!(unordered_batch.key_prefix_deletions.is_empty());
    }
}
