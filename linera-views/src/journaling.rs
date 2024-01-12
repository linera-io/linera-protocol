// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

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

use crate::{
    batch::{Batch, BatchValueWriter, DeletePrefixExpander, SimplifiedBatch},
    common::{
        KeyIterable, KeyValueStore, ReadableKeyValueStore, WritableKeyValueStore, MIN_VIEW_TAG,
    },
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use static_assertions as sa;
use std::fmt::Debug;
use thiserror::Error;

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

fn get_journaling_key(base_key: &[u8], tag: u8, pos: u32) -> Result<Vec<u8>, bcs::Error> {
    let mut key = base_key.to_vec();
    key.extend([JOURNAL_TAG]);
    key.extend([tag]);
    bcs::serialize_into(&mut key, &pos)?;
    Ok(key)
}

/// Low-level, asynchronous direct write key-value operations with simplified batch
#[async_trait]
pub trait DirectWritableKeyValueStore<E> {
    /// The maximal number of items in a batch.
    const MAX_BATCH_SIZE: usize;

    /// The maximal number of bytes of a batch.
    const MAX_BATCH_TOTAL_SIZE: usize;

    /// The maximal size of values that can be stored.
    const MAX_VALUE_SIZE: usize;

    /// The batch type.
    type Batch: SimplifiedBatch + Serialize + DeserializeOwned + Default;

    /// Writes the batch to the database.
    async fn write_batch(&self, batch: Self::Batch) -> Result<(), E>;
}

/// Low-level, asynchronous direct read/write key-value operations with simplified batch
pub trait DirectKeyValueStore:
    ReadableKeyValueStore<Self::Error> + DirectWritableKeyValueStore<Self::Error>
{
    /// The error type.
    type Error: Debug + From<bcs::Error>;
}

/// The header that contains the current state of the journal.
#[derive(Serialize, Deserialize)]
struct JournalHeader {
    block_count: u32,
}

/// A journaling [`KeyValueStore`] built from an inner [`DirectKeyValueStore`].
#[derive(Clone)]
pub struct JournalingKeyValueStore<K> {
    /// The inner store.
    pub store: K,
}

#[async_trait]
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

#[async_trait]
impl<K> ReadableKeyValueStore<K::Error> for JournalingKeyValueStore<K>
where
    K: DirectKeyValueStore + Send + Sync,
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

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, K::Error> {
        self.store.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, K::Error> {
        self.store.contains_key(key).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, K::Error> {
        self.store.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, K::Error> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, K::Error> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

#[async_trait]
impl<K> WritableKeyValueStore<K::Error> for JournalingKeyValueStore<K>
where
    K: DirectKeyValueStore + Send + Sync,
    K::Error: From<JournalConsistencyError>,
{
    /// The size constant do not change
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), K::Error> {
        let batch = K::Batch::from_batch(self, batch).await?;
        if Self::is_fastpath_feasible(&batch) {
            self.store.write_batch(batch).await
        } else {
            let header = self.write_journal(batch, base_key).await?;
            self.coherently_resolve_journal(header, base_key).await
        }
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), K::Error> {
        let key = get_journaling_key(base_key, KeyTag::Journal as u8, 0)?;
        let value = self.read_value::<JournalHeader>(&key).await?;
        if let Some(header) = value {
            self.coherently_resolve_journal(header, base_key).await?;
        }
        Ok(())
    }
}

impl<K> KeyValueStore for JournalingKeyValueStore<K>
where
    K: DirectKeyValueStore + Send + Sync,
    K::Error: From<JournalConsistencyError> + From<bcs::Error>,
{
    type Error = K::Error;
}

impl<K> JournalingKeyValueStore<K>
where
    K: DirectKeyValueStore + Send + Sync,
    K::Error: From<JournalConsistencyError>,
{
    /// Resolves the database by using the header that has been retrieved
    async fn coherently_resolve_journal(
        &self,
        mut header: JournalHeader,
        base_key: &[u8],
    ) -> Result<(), K::Error> {
        loop {
            if header.block_count == 0 {
                break;
            }
            let key = get_journaling_key(base_key, KeyTag::Entry as u8, header.block_count - 1)?;
            let batch = self.store.read_value::<K::Batch>(&key).await?;
            if let Some(mut batch) = batch {
                batch.add_delete(key); // Delete the journal entry
                header.block_count -= 1;
                let key = get_journaling_key(base_key, KeyTag::Journal as u8, 0)?;
                if header.block_count > 0 {
                    let value = bcs::to_bytes(&header)?;
                    batch.add_insert(key, value);
                } else {
                    batch.add_delete(key);
                }
                self.store.write_batch(batch).await?;
            } else {
                return Err(JournalConsistencyError::FailureToRetrieveJournalBlock.into());
            }
        }
        Ok(())
    }

    /// Writes blocks to the database and resolves them later.
    async fn write_journal(
        &self,
        simplified_batch: K::Batch,
        base_key: &[u8],
    ) -> Result<JournalHeader, K::Error> {
        let mut iter = simplified_batch.into_iter();
        let mut block_batch = K::Batch::default();
        let mut block_size = 0;
        let mut block_count = 0;
        let mut transaction_batch = K::Batch::default();
        let mut transaction_size = 0;
        while iter.write_next_value(&mut block_batch, &mut block_size)? {
            let (block_flush, transaction_flush) = {
                if iter.is_empty() || block_batch.len() == K::MAX_BATCH_SIZE - 2 {
                    (true, true)
                } else {
                    let next_block_size =
                        block_size + iter.next_value_size()? + block_batch.overhead_size();
                    let next_transaction_size = transaction_size + next_block_size;
                    let transaction_flush = next_transaction_size > K::MAX_BATCH_TOTAL_SIZE;
                    let block_flush = transaction_flush || next_block_size > K::MAX_VALUE_SIZE;
                    (block_flush, transaction_flush)
                }
            };
            if block_flush {
                block_size += block_batch.overhead_size();
                let value = bcs::to_bytes(&block_batch)?;
                block_batch = K::Batch::default();
                assert_eq!(value.len(), block_size);
                let key = get_journaling_key(base_key, KeyTag::Entry as u8, block_count)?;
                transaction_batch.add_insert(key, value);
                block_count += 1;
                transaction_size += block_size;
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
            let key = get_journaling_key(base_key, KeyTag::Journal as u8, 0)?;
            let value = bcs::to_bytes(&header)?;
            let mut batch = K::Batch::default();
            batch.add_insert(key, value);
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
