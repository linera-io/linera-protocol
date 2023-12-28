// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A simplified key value store from which we can implement a key-value store

use async_trait::async_trait;
use crate::common::from_bytes_opt;
use serde::{de::DeserializeOwned, Serialize};
use crate::common::KeyValueIterable;
use crate::batch::Batch;
use crate::batch::SimplifiedBatch;
use crate::common::KeyIterable;
use std::fmt::Debug;

/// Low-level, asynchronous key-value operations with 
#[async_trait]
pub trait SimplifiedKeyValueStore {
    /// The maximal number of items in a simplified transaction
    const MAX_TRANSACT_WRITE_ITEM_SIZE: usize;

    /// The maximal number of bytes of a simplified transaction
    const MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE: usize;

    /// The maximal size of values that can be stored.
    const MAX_VALUE_SIZE: usize;

    /// The maximal size of keys that can be stored.
    const MAX_KEY_SIZE: usize;

    /// The error type.
    type SimpBatch: SimplifiedBatch + Serialize + Deserialize;

    /// The error type.
    type Error: Debug;

    /// Returns type for key search operations.
    type Keys: KeyIterable<Self::Error>;

    /// Returns type for key-value search operations.
    type KeyValues: KeyValueIterable<Self::Error>;

    /// Retrieve the number of stream queries.
    fn max_stream_queries(&self) -> usize;

    /// Retrieves a `Vec<u8>` from the database using the provided `key`.
    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Test whether a key exists in the database
    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error>;

    /// Retrieves multiple `Vec<u8>` from the database using the provided `keys`.
    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error>;

    /// Finds the `key` matching the prefix. The prefix is not included in the returned keys.
    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error>;

    /// Finds the `(key,value)` pairs matching the prefix. The prefix is not included in the returned keys.
    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error>;

    /// Writes the simplified `batch` in the database
    async fn write_simplified_batch(&self, batch: Batch) -> Result<(), Self::Error>;

    /// Reads a single `key` and deserializes the result if present.
    async fn read_value<V: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<V>, Self::Error>
    where
        Self::Error: From<bcs::Error>,
    {
        from_bytes_opt(&self.read_value_bytes(key).await?)
    }

    /// Reads multiple `keys` and deserializes the results if present.
    async fn read_multi_values<V: DeserializeOwned + Send>(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<V>>, Self::Error>
    where
        Self::Error: From<bcs::Error>,
    {
        let mut values = Vec::with_capacity(keys.len());
        for entry in self.read_multi_values_bytes(keys).await? {
            values.push(from_bytes_opt(&entry)?);
        }
        Ok(values)
    }
}

/// The header that contains the current state of the journal.
#[derive(Serialize, Deserialize)]
struct JournalHeader {
    block_count: u32,
}

#[derive(Clone)]
pub struct StoreFromSimplifiedStore<K> {
    /// The inner client that is called by the LRU cache one
    pub client: K,
}

#[async_trait]
impl<K> KeyValueStore for StoreFromSimplifiedStore<K>
where
    K: SimplifiedKeyValueStore + Send + Sync,
{
    /// The reqding constants do not change
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;
    const MAX_KEY_SIZE: usize = K::MAX_KEY_SIZE;
    /// The basic types do not change
    type Error = K::Error;
    type Keys = K::Keys;
    type KeyValues = K::KeyValues;

    /// The read stuff does not change
    fn max_stream_queries(&self) -> usize {
        self.client.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.client.read_value_bytes(key).await
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error> {
        self.cleint.contains_key(key).await
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.client.read_multi_values_bytes(keys).await
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        self.client.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        self.client.find_key_values_by_prefix(key_prefix).await
    }

    async fn write_batch(&self, batch: Batch, base_key: &[u8]) -> Result<(), DynamoDbContextError> {
        let simp_batch = K::SimpBatch::from_batch(self, batch).await?;
	if K::is_fastpath_feasible(simp_batch) {
            self.write_simplified_batch(simp_batch).await
        } else {
            let header = self.write_journal(simp_batch, base_key).await?;
            self.coherently_resolve_journal(header, base_key).await
        }
    }

    async fn clear_journal(&self, base_key: &[u8]) -> Result<(), DynamoDbContextError> {
	let key = get_journaling_key(base_key, KeyTag::Journal as u8, 0)?;
        let value = self.read_value::<JournalHeader>(&key).await?;
        if let Some(header) = value {
            self.coherently_resolve_journal(header, base_key).await?;
        }
        Ok(())
    }
}

impl<K> StoreFromSimplifiedStore<K>
where
    K: SimplifiedKeyValueStore + Send + Sync,
{
    /// Resolves the database by using the header that has been retrieved
    async fn coherently_resolve_journal(
        &self,
        header: mut JournalHeader,
        base_key: &[u8],
    ) -> Result<(), Self::Error> {
        loop {
            if header.block_count == 0 {
                break;
            }
            let key = get_journaling_key(base_key, KeyTag::Entry as u8, self.block_count - 1)?;
            let mut value = store.read_value::<K::SimpBatch>(&key).await?;
            if let Some(value) = value {
                value.add_delete(key); // Delete the journal entry
                header.block_count -= 1;
                let key = get_journaling_key(base_key, KeyTag::Journal as u8, 0)?;
                if header.block_count > 0 {
                    let value = bcs::to_bytes(header)?;
                    value.add_insert(key, value);
                } else {
                    value.add_delete(key);
                }
                self.write_simplified_batch(value).await?;
            } else {
                return Err(DynamoDbContextError::DatabaseRecoveryFailed);
            }
        }
        Ok(())
    }

    /// Writes blocks to the database and resolves them later.
    pub async fn write_journal(
        &self,
        simplified_batch: K::SimpBatch,
        base_key: &[u8],
    ) -> Result<JournalHeader, DynamoDbContextError> {
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
                let next_value_size = iter.next_value_size(value_size, &simp_batch)?;
                let next_transact_size = transact_size + next_value_size;
                let value_flush = if next_transact_size > K::MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE {
                    true
                } else {
                    next_value_size > K::RAW_MAX_VALUE_SIZE
                };
                let transact_flush = next_transact_size > K::MAX_TRANSACT_WRITE_ITEM_TOTAL_SIZE;
                (value_flush, transact_flush)
            };
            if value_flush {
                value_size += simp_batch.overhead_size();
                let value = simp_batch.to_bytes()?;
                assert_eq!(value.len(), value_size);
                let key = get_journaling_key(base_key, KeyTag::Entry as u8, block_count)?;
                transacts.add_insert(key,value);
                block_count += 1;
                transact_size += value_size;
                value_size = 0;
            }
            if transact_flush {
                self.write_simplified_batch(transacts).await?;
                transact_size = 0;
            }
        }
        let header = JournalHeader { block_count };
        if block_count > 0 {
            let key = get_journaling_key(base_key, KeyTag::Journal as u8, 0)?;
            let value = bcs::to_bytes(&header)?;
            let mut sing_oper = K::SimpBatch::default();
            sing_oper.add_insert(key, value).await?;
            self.write_simplified_batch(sing_oper).await?;
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
