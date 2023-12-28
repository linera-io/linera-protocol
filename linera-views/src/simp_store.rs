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
    type SimpBatch: SimplifiedBatch;

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
