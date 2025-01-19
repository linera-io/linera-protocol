// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Turns a `DirectKeyValueStore` into a `KeyValueStore` by adding journaling.
//!
//! Journaling aims to allow writing arbitrarily large batches of data in an atomic way.
//! This is useful for database backends that limit the number of keys and/or the size of
//! the data that can be written atomically (i.e. in the same database transaction).
//!
//! Journaling requires to set aside a range of keys to hold a possible "header" and an
//! array of unwritten entries called "blocks".
//!
//! When a new batch to be written exceeds the capacity of the underlying storage, the
//! "slow path" is taken: the batch of operations is first written into blocks, then the
//! journal header is (atomically) updated to make the batch of updates persistent.
//!
//! Before any new read or write operation, if a journal is present, it must first be
//! cleared. This is done by processing every block of the journal successively. Every
//! time the data in a block are written, the journal header is updated in the same
//! transaction to mark the block as processed.

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use static_assertions as sa;
use thiserror::Error;

use crate::{
    batch::{Batch, BatchValueWriter, DeletePrefixExpander, SimplifiedBatch},
    store::{
        AdminKeyValueStore, KeyIterable, ReadableKeyValueStore, WithError, WritableKeyValueStore,
    },
    views::MIN_VIEW_TAG,
};

/// The tag used for the journal stuff.
const JOURNAL_TAG: u8 = 0;
// To prevent collisions, the tag value 0 is reserved for journals.
// The tags used by views must be greater or equal than `MIN_VIEW_TAG`.
sa::const_assert!(JOURNAL_TAG < MIN_VIEW_TAG);

/// Data type indicating that the database is not consistent
#[derive(Error, Debug)]
pub enum JournalConsistencyError {
    /// The journal block could not be retrieved, it could be missing or corrupted
    #[error("the journal block could not be retrieved, it could be missing or corrupted")]
    FailureToRetrieveJournalBlock,
}

#[repr(u8)]
enum KeyTag {
    /// Prefix for the storing of the header of the journal.
    Journal = 1,
    /// Prefix for the block entry.
    Entry,
}

fn get_journaling_key(tag: u8, pos: u32) -> Result<Vec<u8>, bcs::Error> {
    let mut key = vec![JOURNAL_TAG];
    key.extend([tag]);
    bcs::serialize_into(&mut key, &pos)?;
    Ok(key)
}

/// Low-level, asynchronous direct write key-value operations with simplified batch
#[async_trait]
pub trait DirectWritableKeyValueStore: WithError {
    /// The maximal number of items in a batch.
    const MAX_BATCH_SIZE: usize;

    /// The maximal number of bytes of a batch.
    const MAX_BATCH_TOTAL_SIZE: usize;

    /// The maximal size of values that can be stored.
    const MAX_VALUE_SIZE: usize;

    /// The batch type.
    type Batch: SimplifiedBatch + Serialize + DeserializeOwned + Default;

    /// Writes the batch to the database.
    async fn write_batch(&self, batch: Self::Batch) -> Result<(), Self::Error>;
}

/// Low-level, asynchronous direct read/write key-value operations with simplified batch
pub trait DirectKeyValueStore:
    ReadableKeyValueStore + DirectWritableKeyValueStore + AdminKeyValueStore
{
}

impl<T> DirectKeyValueStore for T where
    T: ReadableKeyValueStore + DirectWritableKeyValueStore + AdminKeyValueStore
{
}

/// The header that contains the current state of the journal.
#[derive(Serialize, Deserialize, Debug, Default)]
struct JournalHeader {
    block_count: u32,
}

/// A journaling Key Value Store built from an inner [`DirectKeyValueStore`].
#[derive(Clone)]
pub struct JournalingKeyValueStore<K> {
    /// The inner store.
    store: K,
}

impl<K> DeletePrefixExpander for &JournalingKeyValueStore<K>
where
    K: DirectKeyValueStore + Send + Sync,
{
    type Error = K::Error;
    async fn expand_delete_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let mut vector_list = Vec::new();
        for key in self.store.find_keys_by_prefix(key_prefix).await?.iterator() {
            vector_list.push(key?.to_vec());
        }
        Ok(vector_list)
    }
}

impl<K> WithError for JournalingKeyValueStore<K>
where
    K: WithError,
{
    type Error = K::Error;
}

impl<K> ReadableKeyValueStore for JournalingKeyValueStore<K>
where
    K: ReadableKeyValueStore + Send + Sync,
    K::Error: From<JournalConsistencyError>,
{
    /// The size constant do not change
    const MAX_KEY_SIZE: usize = K::MAX_KEY_SIZE;
    /// The basic types do not change
    type Keys = K::Keys;
    type KeyValues = K::KeyValues;

    /// The read stuff does not change
    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.store.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error> {
        self.store.contains_key(key).await
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Self::Error> {
        self.store.contains_keys(keys).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.store.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

impl<K> AdminKeyValueStore for JournalingKeyValueStore<K>
where
    K: AdminKeyValueStore + Send + Sync,
{
    type Config = K::Config;

    fn get_name() -> String {
        format!("journaling {}", K::get_name())
    }

    async fn connect(
        config: &Self::Config,
        namespace: &str,
        root_key: &[u8],
    ) -> Result<Self, Self::Error> {
        let store = K::connect(config, namespace, root_key).await?;
        Ok(Self { store })
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, Self::Error> {
        let store = self.store.clone_with_root_key(root_key)?;
        Ok(Self { store })
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, Self::Error> {
        K::list_all(config).await
    }

    async fn delete_all(config: &Self::Config) -> Result<(), Self::Error> {
        K::delete_all(config).await
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, Self::Error> {
        K::exists(config, namespace).await
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        K::create(config, namespace).await
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        K::delete(config, namespace).await
    }
}

impl<K> WritableKeyValueStore for JournalingKeyValueStore<K>
where
    K: DirectKeyValueStore + Send + Sync,
    K::Error: From<JournalConsistencyError>,
{
    /// The size constant do not change
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        let batch = K::Batch::from_batch(self, batch).await?;
        if Self::is_fastpath_feasible(&batch) {
            self.store.write_batch(batch).await
        } else {
            let header = self.write_journal(batch).await?;
            self.coherently_resolve_journal(header).await
        }
    }

    async fn clear_journal(&self) -> Result<(), Self::Error> {
        let key = get_journaling_key(KeyTag::Journal as u8, 0)?;
        let value = self.read_value::<JournalHeader>(&key).await?;
        if let Some(header) = value {
            self.coherently_resolve_journal(header).await?;
        }
        Ok(())
    }
}

impl<K> JournalingKeyValueStore<K>
where
    K: DirectKeyValueStore + Send + Sync,
    K::Error: From<JournalConsistencyError>,
{
    /// Resolves the pending operations that were previously stored in the database
    /// journal.
    ///
    /// For each block processed, we atomically update the journal header as well. When
    /// the last block is processed, this atomically clears the journal and make the store
    /// finally available again (for the range of keys managed by the journal).
    ///
    /// This function respects the constraints of the underlying key-value store `K` if
    /// the following conditions are met:
    ///
    /// (1) each block contains at most `K::MAX_BATCH_SIZE - 2` operations;
    ///
    /// (2) the total size of the all operations in a block doesn't exceed:
    /// `K::MAX_BATCH_TOTAL_SIZE - sizeof(block_key) - sizeof(header_key) - sizeof(bcs_header)`
    ///
    /// (3) every operation in a block satisfies the contraints on individual database
    /// operations represented by `K::MAX_KEY_SIZE` and `K::MAX_VALUE_SIZE`.
    ///
    /// (4) `block_key` and `header_key` don't exceed `K::MAX_KEY_SIZE` and `bcs_header`
    /// doesn't exceed `K::MAX_VALUE_SIZE`.
    async fn coherently_resolve_journal(&self, mut header: JournalHeader) -> Result<(), K::Error> {
        let header_key = get_journaling_key(KeyTag::Journal as u8, 0)?;
        while header.block_count > 0 {
            let block_key = get_journaling_key(KeyTag::Entry as u8, header.block_count - 1)?;
            // Read the batch of updates (aka. "block") previously saved in the journal.
            let mut batch = self
                .store
                .read_value::<K::Batch>(&block_key)
                .await?
                .ok_or(JournalConsistencyError::FailureToRetrieveJournalBlock)?;
            // Execute the block and delete it from the journal atomically.
            batch.add_delete(block_key);
            header.block_count -= 1;
            if header.block_count > 0 {
                let value = bcs::to_bytes(&header)?;
                batch.add_insert(header_key.clone(), value);
            } else {
                batch.add_delete(header_key.clone());
            }
            self.store.write_batch(batch).await?;
        }
        Ok(())
    }

    /// Writes the content of `batch` to the journal as a succession of blocks that can be
    /// interpreted later by `coherently_resolve_journal`.
    ///
    /// Starting with a batch of operations that is typically too large to be executed in
    /// one go (see `is_fastpath_feasible()` below), the goal of this function is to split
    /// the batch into smaller blocks so that `coherently_resolve_journal` respects the
    /// constraints of the underlying key-value store (see analysis above).
    ///
    /// For efficiency reasons, we write as many blocks as possible in each "transaction"
    /// batch, using one write-operation per block. Then we also update the journal header
    /// with the final number of blocks.
    ///
    /// As a result, the constraints of the underlying database are respected if the
    /// following conditions are met while a "transaction" batch is being built:
    ///
    /// (1) The number of blocks per transaction doesn't exceed `K::MAX_BATCH_SIZE`.
    /// But it is perfectly possible to have K::MAX_BATCH_SIZE = usize::MAX.
    ///
    /// (2) The total size of BCS-serialized blocks together with their corresponding keys
    /// does not exceed `K::MAX_BATCH_TOTAL_SIZE`.
    ///
    /// (3) The size of each BCS-serialized block doesn't exceed `K::MAX_VALUE_SIZE`.
    ///
    /// (4) When processing a journal block, we have to do two other operations.
    ///   (a) removing the existing block. The cost is `key_len`.
    ///   (b) updating or removing the journal. The cost is `key_len + header_value_len`
    ///       or `key_len`. An upper bound is thus
    ///       `journal_len_upper_bound = key_len + header_value_len`.
    ///   Thus the following has to be taken as upper bound on block_size:
    ///   `K::MAX_BATCH_TOTAL_SIZE - key_len - journal_len_upper_bound`.
    ///
    /// NOTE:
    /// * Since a block must contain at least one operation and M bytes of the
    ///   serialization overhead (typically M = 2 or 3 bytes of vector sizes), condition (3)
    ///   requires that each operation in the original batch satisfies:
    ///     `sizeof(key) + sizeof(value) + M <= K::MAX_VALUE_SIZE`
    ///
    /// * Similarly, a transaction must contain at least one block so it is desirable that
    ///   the maximum size of a block insertion `1 + sizeof(block_key) + K::MAX_VALUE_SIZE`
    ///   plus M bytes of overhead doesn't exceed the threshold of condition (2).
    async fn write_journal(&self, batch: K::Batch) -> Result<JournalHeader, K::Error> {
        let header_key = get_journaling_key(KeyTag::Journal as u8, 0)?;
        let key_len = header_key.len();
        let header_value_len = bcs::serialized_size(&JournalHeader::default())?;
        let journal_len_upper_bound = key_len + header_value_len;
        // Each block in a transaction comes with a key.
        let max_transaction_size = K::MAX_BATCH_TOTAL_SIZE;
        let max_block_size = std::cmp::min(
            K::MAX_VALUE_SIZE,
            K::MAX_BATCH_TOTAL_SIZE - key_len - journal_len_upper_bound,
        );

        let mut iter = batch.into_iter();
        let mut block_batch = K::Batch::default();
        let mut block_size = 0;
        let mut block_count = 0;
        let mut transaction_batch = K::Batch::default();
        let mut transaction_size = 0;
        while iter.write_next_value(&mut block_batch, &mut block_size)? {
            let (block_flush, transaction_flush) = {
                if iter.is_empty() || transaction_batch.len() == K::MAX_BATCH_SIZE - 1 {
                    (true, true)
                } else {
                    let next_block_size = iter
                        .next_batch_size(&block_batch, block_size)?
                        .expect("iter is not empty");
                    let next_transaction_size = transaction_size + next_block_size + key_len;
                    let transaction_flush = next_transaction_size > max_transaction_size;
                    let block_flush = transaction_flush
                        || block_batch.len() == K::MAX_BATCH_SIZE - 2
                        || next_block_size > max_block_size;
                    (block_flush, transaction_flush)
                }
            };
            if block_flush {
                block_size += block_batch.overhead_size();
                let value = bcs::to_bytes(&block_batch)?;
                block_batch = K::Batch::default();
                assert_eq!(value.len(), block_size);
                let key = get_journaling_key(KeyTag::Entry as u8, block_count)?;
                transaction_batch.add_insert(key, value);
                block_count += 1;
                transaction_size += block_size + key_len;
                block_size = 0;
            }
            if transaction_flush {
                let batch = std::mem::take(&mut transaction_batch);
                self.store.write_batch(batch).await?;
                transaction_size = 0;
            }
        }
        let header = JournalHeader { block_count };
        if block_count > 0 {
            let value = bcs::to_bytes(&header)?;
            let mut batch = K::Batch::default();
            batch.add_insert(header_key, value);
            self.store.write_batch(batch).await?;
        }
        Ok(header)
    }

    fn is_fastpath_feasible(batch: &K::Batch) -> bool {
        batch.len() <= K::MAX_BATCH_SIZE && batch.num_bytes() <= K::MAX_BATCH_TOTAL_SIZE
    }
}

impl<K> JournalingKeyValueStore<K> {
    /// Creates a new journaling store.
    pub fn new(store: K) -> Self {
        Self { store }
    }
}
