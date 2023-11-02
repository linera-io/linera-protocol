// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types to interface with the system API available to application views.

use super::view_system_api as wit;
use crate::util::yield_once;
use async_trait::async_trait;
use linera_views::{
    batch::{Batch, WriteOperation},
    common::{ContextFromDb, KeyValueStoreClient},
    views::ViewError,
};

/// We need to have a maximum key size that handles all possible underlying
/// sizes. The constraint so far is DynamoDb which has a key length of 1024.
/// That key length is decreased by 4 due to the use of a value splitting.
/// Then the KeyValueStorClient needs to handle some base_key and so we
/// reduce to 900. Depending on the size, the error can occur in system_api
/// or in the KeyValueStoreView.
const MAX_KEY_SIZE: usize = 900;

/// A type to interface with the key value storage provided to applications.
#[derive(Default, Clone)]
pub struct KeyValueStore;

impl KeyValueStore {
    async fn find_keys_by_prefix_load(&self, key_prefix: &[u8]) -> Vec<Vec<u8>> {
        let promise = wit::FindKeys::new(key_prefix);
        yield_once().await;
        promise.wait()
    }

    async fn find_key_values_by_prefix_load(&self, key_prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let promise = wit::FindKeyValues::new(key_prefix);
        yield_once().await;
        promise.wait()
    }
}

#[async_trait]
impl KeyValueStoreClient for KeyValueStore {
    // The KeyValueStoreClient of the system_api does not have limits
    // on the size of its values.
    const MAX_VALUE_SIZE: usize = usize::MAX;
    type Error = ViewError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_key_size(&self) -> usize {
        MAX_KEY_SIZE
    }

    fn max_stream_queries(&self) -> usize {
        1
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        if key.len() > MAX_KEY_SIZE {
            return Err(ViewError::KeyTooLong);
        }
        let promise = wit::ReadValueBytes::new(key);
        yield_once().await;
        Ok(promise.wait())
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let mut results = Vec::new();
        for key in keys {
            if key.len() > MAX_KEY_SIZE {
                return Err(ViewError::KeyTooLong);
            }
            let value = self.read_value_bytes(&key).await?;
            results.push(value);
        }
        Ok(results)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, ViewError> {
        if key_prefix.len() > MAX_KEY_SIZE {
            return Err(ViewError::KeyTooLong);
        }
        let keys = self.find_keys_by_prefix_load(key_prefix).await;
        Ok(keys)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ViewError> {
        if key_prefix.len() > MAX_KEY_SIZE {
            return Err(ViewError::KeyTooLong);
        }
        let key_values = self.find_key_values_by_prefix_load(key_prefix).await;
        Ok(key_values)
    }

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), ViewError> {
        let mut list_oper = Vec::new();
        for op in &batch.operations {
            match op {
                WriteOperation::Delete { key } => {
                    if key.len() > MAX_KEY_SIZE {
                        return Err(ViewError::KeyTooLong);
                    }
                    list_oper.push(wit::WriteOperation::Delete(key));
                }
                WriteOperation::Put { key, value } => {
                    if key.len() > MAX_KEY_SIZE {
                        return Err(ViewError::KeyTooLong);
                    }
                    list_oper.push(wit::WriteOperation::Put((key, value)))
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    if key_prefix.len() > MAX_KEY_SIZE {
                        return Err(ViewError::KeyTooLong);
                    }
                    list_oper.push(wit::WriteOperation::Deleteprefix(key_prefix))
                }
            }
        }
        wit::write_batch(&list_oper);
        Ok(())
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Implementation of [`linera_views::common::Context`] to be used for data storage
/// by Linera applications.
pub type ViewStorageContext = ContextFromDb<(), KeyValueStore>;
