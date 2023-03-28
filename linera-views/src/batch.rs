// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    common::{get_interval, Context, KeyIterable},
    memory::{MemoryContext, MemoryContextError},
    views::ViewError,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::Debug,
    ops::Bound,
};

/// A write operation as requested by a view when it needs to persist staged changes.
#[derive(Debug)]
pub enum WriteOperation {
    /// Delete the given key.
    Delete {
        /// The key that will be deleted
        key: Vec<u8>,
    },
    /// Delete all the keys matching the given prefix.
    DeletePrefix {
        /// The prefix of the keys to be deleted
        key_prefix: Vec<u8>,
    },
    /// Set the value of the given key.
    Put {
        /// The key to be inserted or replaced
        key: Vec<u8>,
        /// The value to be inserted on the key
        value: Vec<u8>,
    },
}

/// A batch of writes inside a transaction;
#[derive(Default)]
pub struct Batch {
    /// The entries of batch
    pub operations: Vec<WriteOperation>,
}

/// Unordered list of deletes and puts being written
#[derive(Default, Serialize, Deserialize)]
pub struct SimpleUnorderedBatch {
    /// list of deletes unordered
    pub deletions: Vec<Vec<u8>>,
    /// list of inserts unorderd
    pub insertions: Vec<(Vec<u8>, Vec<u8>)>,
}

/// An unordered batch of deletes/puts and a collection of key prefix deletions
#[derive(Default)]
pub struct UnorderedBatch {
    /// key prefix deletions
    pub key_prefix_deletions: Vec<Vec<u8>>,
    /// The delete and lists
    pub simple_unordered_batch: SimpleUnorderedBatch,
}

impl UnorderedBatch {
    /// From an UnorderedBatch, create a SimpleUnorderedBatch that does not contain the key_prefixes
    pub async fn eliminate_delete_prefixes<DB: DeletePrefixExpander>(
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
}

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
    /// Create an empty batch
    pub fn new() -> Self {
        Self::default()
    }

    /// building a batch from a function
    pub async fn build<F>(builder: F) -> Result<Self, ViewError>
    where
        F: FnOnce(&mut Batch) -> futures::future::BoxFuture<Result<(), ViewError>> + Send + Sync,
    {
        let mut batch = Batch::new();
        builder(&mut batch).await?;
        Ok(batch)
    }

    /// A key may appear multiple times in the batch
    /// The construction of BatchWriteItem and TransactWriteItem for DynamoDb does
    /// not allow this to happen.
    pub fn simplify(self) -> UnorderedBatch {
        let mut delete_and_insert_map = BTreeMap::new();
        let mut delete_prefix_set = BTreeSet::new();
        for op in self.operations {
            match op {
                WriteOperation::Delete { key } => {
                    if !is_prefix_matched(&delete_prefix_set, &key) {
                        delete_and_insert_map.insert(key, None);
                    }
                }
                WriteOperation::Put { key, value } => {
                    delete_and_insert_map.insert(key, Some(value));
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    let key_list: Vec<Vec<u8>> = delete_and_insert_map
                        .range(get_interval(key_prefix.clone()))
                        .map(|x| x.0.to_vec())
                        .collect();
                    for key in key_list {
                        delete_and_insert_map.remove(&key);
                    }
                    let key_prefix_list: Vec<Vec<u8>> = delete_prefix_set
                        .range(get_interval(key_prefix.clone()))
                        .map(|x: &Vec<u8>| x.to_vec())
                        .collect();
                    for key_prefix in key_prefix_list {
                        delete_prefix_set.remove(&key_prefix);
                    }
                    delete_prefix_set.insert(key_prefix);
                }
            }
        }
        // It is important to note that DeletePrefix operations have to be done before other
        // insert operations.
        let mut key_prefix_deletions = Vec::new();
        for key_prefix in delete_prefix_set {
            key_prefix_deletions.push(key_prefix);
        }
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

    /// Insert a Put { key, value } into the batch
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

    /// Insert a Put { key, value } into the batch
    #[inline]
    pub fn put_key_value_bytes(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push(WriteOperation::Put { key, value });
    }

    /// Insert a Delete { key } into the batch
    #[inline]
    pub fn delete_key(&mut self, key: Vec<u8>) {
        self.operations.push(WriteOperation::Delete { key });
    }

    /// Insert a DeletePrefix { key_prefix } into the batch
    #[inline]
    pub fn delete_key_prefix(&mut self, key_prefix: Vec<u8>) {
        self.operations
            .push(WriteOperation::DeletePrefix { key_prefix });
    }
}

/// A trait to expand delete_prefix operations.
#[async_trait]
pub trait DeletePrefixExpander {
    /// The error type that can happen when expanding the key_prefix
    type Error: Debug;
    /// Return the list of keys to be appended to the list.
    async fn expand_delete_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error>;
}

#[async_trait]
impl DeletePrefixExpander for MemoryContext<()> {
    type Error = MemoryContextError;
    async fn expand_delete_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let mut vector_list = Vec::new();
        for key in <Vec<Vec<u8>> as KeyIterable<Self::Error>>::iterator(
            &self.find_keys_by_prefix(key_prefix).await?,
        ) {
            vector_list.push(key?.to_vec());
        }
        Ok(vector_list)
    }
}

#[cfg(test)]
mod tests {
    use linera_views::{batch::Batch, common::Context, memory::create_test_context};

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

    #[tokio::test]
    async fn test_simplify_batch3() {
        let context = create_test_context();
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
            .eliminate_delete_prefixes(&context)
            .await
            .unwrap();
        assert_eq!(
            simple_unordered_batch.deletions,
            vec![vec![1, 2, 3], vec![1, 2, 4], vec![1, 2, 5]]
        );
        assert!(simple_unordered_batch.insertions.is_empty());
    }
}
