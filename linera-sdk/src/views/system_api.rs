// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types to interface with the system API available to application views.

use super::view_system_api as wit;
use async_trait::async_trait;
use futures::future;
use linera_views::{
    batch::{Batch, WriteOperation},
    common::{ContextFromDb, KeyValueStoreClient},
    views::ViewError,
};

/// A type to interface with the key value storage provided to applications.
#[derive(Default, Clone)]
pub struct KeyValueStore;

impl KeyValueStore {
    async fn find_keys_by_prefix_load(&self, key_prefix: &[u8]) -> Vec<Vec<u8>> {
        let future = wit::FindKeys::new(key_prefix);
        future::poll_fn(|_context| future.poll().into()).await
    }

    async fn find_key_values_by_prefix_load(&self, key_prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let future = wit::FindKeyValues::new(key_prefix);
        future::poll_fn(|_context| future.poll().into()).await
    }
}

#[async_trait]
impl KeyValueStoreClient for KeyValueStore {
    type Error = ViewError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let future = wit::ReadKeyBytes::new(key);
        Ok(future::poll_fn(|_context| future.poll().into()).await)
    }

    async fn read_multi_key_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let mut results = Vec::new();
        for key in keys {
            let value = self.read_key_bytes(&key).await?;
            results.push(value);
        }
        Ok(results)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, ViewError> {
        let keys = self.find_keys_by_prefix_load(key_prefix).await;
        Ok(keys)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ViewError> {
        let key_values = self.find_key_values_by_prefix_load(key_prefix).await;
        Ok(key_values)
    }

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), ViewError> {
        let mut list_oper = Vec::new();
        for op in &batch.operations {
            match op {
                WriteOperation::Delete { key } => {
                    list_oper.push(wit::WriteOperation::Delete(key));
                }
                WriteOperation::Put { key, value } => {
                    list_oper.push(wit::WriteOperation::Put((key, value)))
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    list_oper.push(wit::WriteOperation::Deleteprefix(key_prefix))
                }
            }
        }
        let future = wit::WriteBatch::new(&list_oper);
        let () = future::poll_fn(|_context| future.poll().into()).await;
        Ok(())
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Implementation of [`linera_views::common::Context`] that uses the [`KeyValueStore`] to
/// allow views to store data in the storage layer provided to Linera applications.
pub type ViewStorageContext = ContextFromDb<(), KeyValueStore>;
