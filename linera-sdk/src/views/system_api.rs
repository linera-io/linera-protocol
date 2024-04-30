// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types to interface with the system API available to application views.

use linera_base::ensure;
use linera_views::{
    batch::{Batch, WriteOperation},
    common::{ContextFromStore, ReadableKeyValueStore, WritableKeyValueStore},
    views::ViewError,
};

use crate::{service::wit::view_system_api as wit, util::yield_once};

/// We need to have a maximum key size that handles all possible underlying
/// sizes. The constraint so far is DynamoDb which has a key length of 1024.
/// That key length is decreased by 4 due to the use of a value splitting.
/// Then the [`KeyValueStore`] needs to handle some base_key and so we
/// reduce to 900. Depending on the size, the error can occur in system_api
/// or in the `KeyValueStoreView`.
const MAX_KEY_SIZE: usize = 900;

/// A type to interface with the key value storage provided to applications.
#[derive(Default, Clone)]
pub struct KeyValueStore;

impl KeyValueStore {
    async fn find_keys_by_prefix_load(&self, key_prefix: &[u8]) -> Vec<Vec<u8>> {
        let promise = wit::find_keys_new(key_prefix);
        yield_once().await;
        wit::find_keys_wait(promise)
    }

    async fn find_key_values_by_prefix_load(&self, key_prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let promise = wit::find_key_values_new(key_prefix);
        yield_once().await;
        wit::find_key_values_wait(promise)
    }
}

impl ReadableKeyValueStore<ViewError> for KeyValueStore {
    // The KeyValueStore of the system_api does not have limits
    // on the size of its values.
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    fn max_stream_queries(&self) -> usize {
        1
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, ViewError> {
        ensure!(key.len() <= Self::MAX_KEY_SIZE, ViewError::KeyTooLong);
        let promise = wit::contains_key_new(key);
        yield_once().await;
        Ok(wit::contains_key_wait(promise))
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ViewError> {
        for key in &keys {
            ensure!(key.len() <= Self::MAX_KEY_SIZE, ViewError::KeyTooLong);
        }
        let promise = wit::read_multi_values_bytes_new(&keys);
        yield_once().await;
        Ok(wit::read_multi_values_bytes_wait(promise))
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        ensure!(key.len() <= Self::MAX_KEY_SIZE, ViewError::KeyTooLong);
        let promise = wit::read_value_bytes_new(key);
        yield_once().await;
        Ok(wit::read_value_bytes_wait(promise))
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, ViewError> {
        ensure!(
            key_prefix.len() <= Self::MAX_KEY_SIZE,
            ViewError::KeyTooLong
        );
        let keys = self.find_keys_by_prefix_load(key_prefix).await;
        Ok(keys)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ViewError> {
        ensure!(
            key_prefix.len() <= Self::MAX_KEY_SIZE,
            ViewError::KeyTooLong
        );
        let key_values = self.find_key_values_by_prefix_load(key_prefix).await;
        Ok(key_values)
    }
}

impl WritableKeyValueStore<ViewError> for KeyValueStore {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), ViewError> {
        let batch_operations = batch
            .operations
            .into_iter()
            .map(WriteOperation::into)
            .collect::<Vec<_>>();

        wit::write_batch(&batch_operations);

        Ok(())
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), ViewError> {
        Ok(())
    }
}

impl linera_views::common::KeyValueStore for KeyValueStore {
    type Error = ViewError;
}

/// Implementation of [`linera_views::common::Context`] to be used for data storage
/// by Linera applications.
pub type ViewStorageContext = ContextFromStore<(), KeyValueStore>;
