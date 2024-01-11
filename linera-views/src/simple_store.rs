// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The Key Value Store and a simplified Key Value Store
//! The most important traits are:
//! * [`KeyValueStore`][trait1] which manages the access to a database and is clonable. It has a minimal interface
//!
//! [trait1]: common::KeyValueStore

use crate::{
    batch::{Batch, DeletePrefixExpander, SimplifiedBatch, SimplifiedBatchIter},
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
    // We used the value 0 because it does not collide with other key values.
    // since other tags are starting from 1.
    let mut key = base_key.to_vec();
    key.extend([JOURNAL_TAG]);
    key.extend([tag]);
    bcs::serialize_into(&mut key, &pos)?;
    Ok(key)
}

/// Low-level, asynchronous direct write key-value operations with simplified batch
#[async_trait]
pub trait DirectWritableKeyValueStore<E> {
    /// The maximal number of items in a simplified transaction
    const MAX_TRANSACT_WRITE_ITEM_SIZE: usize;

    /// The maximal number of bytes of a simplified transaction
    const MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE: usize;

    /// The maximal size of values that can be stored.
    const MAX_VALUE_SIZE: usize;

    /// The batch type.
    type SimpBatch: SimplifiedBatch + Serialize + DeserializeOwned + Default;

    /// Writes the simplified `batch` in the database. After the writing, the
    /// simplified batch must be empty.
    async fn write_simplified_batch(&self, simp_batch: &mut Self::SimpBatch) -> Result<(), E>;
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

/// A KeyValueStore built from a SimplifiedKeyValueStore by using journaling
/// for bypassing the limits on the batch size
#[derive(Clone)]
pub struct JournalingKeyValueStore<K> {
    /// The inner client that is called by the LRU cache one
    pub client: K,
}

#[async_trait]
impl<K> DeletePrefixExpander for &JournalingKeyValueStore<K>
where
    K: DirectKeyValueStore + Send + Sync,
{
    type Error = K::Error;
    async fn expand_delete_prefix(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, Self::Error> {
        let mut vector_list = Vec::new();
        for key in self
            .client
            .find_keys_by_prefix(key_prefix)
            .await?
            .iterator()
        {
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
        self.client.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, K::Error> {
        self.client.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, K::Error> {
        self.client.contains_key(key).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, K::Error> {
        self.client.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, K::Error> {
        self.client.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, K::Error> {
        self.client.find_key_values_by_prefix(key_prefix).await
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
        let mut simp_batch = K::SimpBatch::from_batch(self, batch).await?;
        if Self::is_fastpath_feasible(&simp_batch) {
            self.client.write_simplified_batch(&mut simp_batch).await
        } else {
            let header = self.write_journal(simp_batch, base_key).await?;
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
            let simp_batch = self.client.read_value::<K::SimpBatch>(&key).await?;
            if let Some(mut simp_batch) = simp_batch {
                simp_batch.add_delete(key); // Delete the journal entry
                header.block_count -= 1;
                let key = get_journaling_key(base_key, KeyTag::Journal as u8, 0)?;
                if header.block_count > 0 {
                    let value = bcs::to_bytes(&header)?;
                    simp_batch.add_insert(key, value);
                } else {
                    simp_batch.add_delete(key);
                }
                self.client.write_simplified_batch(&mut simp_batch).await?;
            } else {
                return Err(JournalConsistencyError::FailureToRetrieveJournalBlock.into());
            }
        }
        Ok(())
    }

    /// Writes blocks to the database and resolves them later.
    async fn write_journal(
        &self,
        simplified_batch: K::SimpBatch,
        base_key: &[u8],
    ) -> Result<JournalHeader, K::Error> {
        let mut iter = simplified_batch.into_iter();
        let mut value_size = 0;
        let mut transact_size = 0;
        let mut simp_batch = K::SimpBatch::default();
        let mut block_count = 0;
        let mut transacts = K::SimpBatch::default();
        loop {
            let result = simp_batch.try_append(&mut iter, &mut value_size)?;
            if !result {
                break;
            }
            let (value_flush, transact_flush) = if (iter.remaining_len() == 0)
                || simp_batch.len() == K::MAX_TRANSACT_WRITE_ITEM_SIZE - 2
            {
                (true, true)
            } else {
                let next_value_size = simp_batch.next_value_size(value_size, &mut iter)?;
                let next_transact_size = transact_size + next_value_size;
                let value_flush = if next_transact_size > K::MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE {
                    true
                } else {
                    next_value_size > K::MAX_VALUE_SIZE
                };
                let transact_flush = next_transact_size > K::MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE;
                (value_flush, transact_flush)
            };
            if value_flush {
                value_size += simp_batch.overhead_size();
                let value = simp_batch.to_bytes()?;
                assert_eq!(value.len(), value_size);
                let key = get_journaling_key(base_key, KeyTag::Entry as u8, block_count)?;
                transacts.add_insert(key, value);
                block_count += 1;
                transact_size += value_size;
                value_size = 0;
            }
            if transact_flush {
                self.client.write_simplified_batch(&mut transacts).await?;
                transact_size = 0;
            }
        }
        let header = JournalHeader { block_count };
        if block_count > 0 {
            let key = get_journaling_key(base_key, KeyTag::Journal as u8, 0)?;
            let value = bcs::to_bytes(&header)?;
            let mut sing_oper = K::SimpBatch::default();
            sing_oper.add_insert(key, value);
            self.client.write_simplified_batch(&mut sing_oper).await?;
        }
        Ok(header)
    }

    fn is_fastpath_feasible(simp_batch: &K::SimpBatch) -> bool {
        if simp_batch.len() > K::MAX_TRANSACT_WRITE_ITEM_SIZE {
            return false;
        }
        simp_batch.bytes() <= K::MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE
    }
}

impl<K> JournalingKeyValueStore<K> {
    /// Creates a new store from a simplified one.
    pub fn new(store: K) -> Self {
        Self { client: store }
    }
}
