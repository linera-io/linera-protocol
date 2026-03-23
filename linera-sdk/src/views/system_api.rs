// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types to interface with the system API available to application views.

#[cfg(with_testing)]
use std::sync::Arc;

use linera_base::ensure;
use linera_views::{
    batch::Batch,
    store::{
        KeyInterval, KeyIntervalStart, ReadableKeyValueStore, WithError, WritableKeyValueStore,
    },
};
use thiserror::Error;

#[cfg(with_testing)]
use super::mock_key_value_store::MockKeyValueStore;
use crate::{
    contract::wit::{
        base_runtime_api::{self as contract_wit},
        contract_runtime_api::{self, WriteOperation},
    },
    service::wit::base_runtime_api as service_wit,
};

type IntervalKeys = (Vec<Vec<u8>>, bool);
type IntervalKeyValues = (Vec<(Vec<u8>, Vec<u8>)>, bool);

/// We need to have a maximum key size that handles all possible underlying
/// sizes. The constraint so far is DynamoDB which has a key length of 1024.
/// That key length is decreased by 4 due to the use of a value splitting.
/// Then the [`KeyValueStore`] needs to handle some base key and so we
/// reduce to 900. Depending on the size, the error can occur in `system_api`
/// or in the `KeyValueStoreView`.
const MAX_KEY_SIZE: usize = 900;

/// A type to interface with the key value storage provided to applications.
#[derive(Clone)]
pub struct KeyValueStore {
    wit_api: WitInterface,
}

#[cfg_attr(with_testing, allow(dead_code))]
impl KeyValueStore {
    /// Returns a [`KeyValueStore`] that uses the contract WIT interface.
    pub(crate) fn for_contracts() -> Self {
        KeyValueStore {
            wit_api: WitInterface::Contract,
        }
    }

    /// Returns a [`KeyValueStore`] that uses the service WIT interface.
    pub(crate) fn for_services() -> Self {
        KeyValueStore {
            wit_api: WitInterface::Service,
        }
    }

    /// Returns a new [`KeyValueStore`] that just keeps the storage contents in memory.
    #[cfg(with_testing)]
    pub fn mock() -> Self {
        KeyValueStore {
            wit_api: WitInterface::Mock {
                store: Arc::new(MockKeyValueStore::default()),
                read_only: true,
            },
        }
    }

    /// Returns a mocked [`KeyValueStore`] that shares the memory storage with this instance but
    /// allows write operations.
    #[cfg(with_testing)]
    pub fn to_mut(&self) -> Self {
        let WitInterface::Mock { store, .. } = &self.wit_api else {
            panic!("Real `KeyValueStore` should not be used in unit tests");
        };

        KeyValueStore {
            wit_api: WitInterface::Mock {
                store: store.clone(),
                read_only: false,
            },
        }
    }
}

impl WithError for KeyValueStore {
    type Error = KeyValueStoreError;
}

/// The error type for [`KeyValueStore`] operations.
#[derive(Error, Debug)]
pub enum KeyValueStoreError {
    /// Key too long
    #[error("Key too long")]
    KeyTooLong,

    /// BCS serialization error.
    #[error(transparent)]
    BcsError(#[from] bcs::Error),
}

impl linera_views::store::KeyValueStoreError for KeyValueStoreError {
    const BACKEND: &'static str = "key_value_store";
}

impl ReadableKeyValueStore for KeyValueStore {
    // The KeyValueStore of the system_api does not have limits
    // on the size of its values.
    const MAX_KEY_SIZE: usize = MAX_KEY_SIZE;

    fn max_stream_queries(&self) -> usize {
        1
    }

    fn root_key(&self) -> Result<Vec<u8>, KeyValueStoreError> {
        Ok(Vec::new())
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, KeyValueStoreError> {
        ensure!(
            key.len() <= Self::MAX_KEY_SIZE,
            KeyValueStoreError::KeyTooLong
        );
        let promise = self.wit_api.contains_key_new(key);
        Ok(self.wit_api.contains_key_wait(promise))
    }

    async fn contains_keys(&self, keys: &[Vec<u8>]) -> Result<Vec<bool>, KeyValueStoreError> {
        for key in keys {
            ensure!(
                key.len() <= Self::MAX_KEY_SIZE,
                KeyValueStoreError::KeyTooLong
            );
        }
        let promise = self.wit_api.contains_keys_new(keys);
        Ok(self.wit_api.contains_keys_wait(promise))
    }

    async fn read_multi_values_bytes(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, KeyValueStoreError> {
        for key in keys {
            ensure!(
                key.len() <= Self::MAX_KEY_SIZE,
                KeyValueStoreError::KeyTooLong
            );
        }
        let promise = self.wit_api.read_multi_values_bytes_new(keys);
        Ok(self.wit_api.read_multi_values_bytes_wait(promise))
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KeyValueStoreError> {
        ensure!(
            key.len() <= Self::MAX_KEY_SIZE,
            KeyValueStoreError::KeyTooLong
        );
        let promise = self.wit_api.read_value_bytes_new(key);
        Ok(self.wit_api.read_value_bytes_wait(promise))
    }

    async fn find_keys_in_interval(
        &self,
        key_interval: KeyInterval,
    ) -> Result<(Vec<Vec<u8>>, bool), KeyValueStoreError> {
        for key in [key_interval.start_bound(), key_interval.end_bound()] {
            match key {
                std::ops::Bound::Included(key) | std::ops::Bound::Excluded(key) => ensure!(
                    key.len() <= Self::MAX_KEY_SIZE,
                    KeyValueStoreError::KeyTooLong
                ),
                std::ops::Bound::Unbounded => {}
            }
        }
        let promise = self.wit_api.find_keys_in_interval_new(&key_interval);
        Ok(self.wit_api.find_keys_in_interval_wait(promise))
    }

    async fn find_key_values_in_interval(
        &self,
        key_interval: KeyInterval,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, bool), KeyValueStoreError> {
        for key in [key_interval.start_bound(), key_interval.end_bound()] {
            match key {
                std::ops::Bound::Included(key) | std::ops::Bound::Excluded(key) => ensure!(
                    key.len() <= Self::MAX_KEY_SIZE,
                    KeyValueStoreError::KeyTooLong
                ),
                std::ops::Bound::Unbounded => {}
            }
        }
        let promise = self.wit_api.find_key_values_in_interval_new(&key_interval);
        Ok(self.wit_api.find_key_values_in_interval_wait(promise))
    }
}

impl WritableKeyValueStore for KeyValueStore {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch) -> Result<(), KeyValueStoreError> {
        self.wit_api.write_batch(batch);
        Ok(())
    }

    async fn clear_journal(&self) -> Result<(), KeyValueStoreError> {
        Ok(())
    }
}

/// Which system API should be used to interface with the storage.
#[derive(Clone)]
#[cfg_attr(with_testing, allow(dead_code))]
enum WitInterface {
    /// The contract system API.
    Contract,
    /// The service system API.
    Service,
    #[cfg(with_testing)]
    /// A mock system API.
    Mock {
        store: Arc<MockKeyValueStore>,
        read_only: bool,
    },
}

impl WitInterface {
    fn contract_key_interval_query(key_interval: &KeyInterval) -> contract_wit::KeyIntervalQuery {
        contract_wit::KeyIntervalQuery {
            start: match &key_interval.start {
                KeyIntervalStart::Included(key) | KeyIntervalStart::Excluded(key) => key.clone(),
            },
            start_inclusive: matches!(&key_interval.start, KeyIntervalStart::Included(_)),
            end: match &key_interval.end {
                std::ops::Bound::Included(key) | std::ops::Bound::Excluded(key) => {
                    Some(key.clone())
                }
                std::ops::Bound::Unbounded => None,
            },
            end_inclusive: matches!(&key_interval.end, std::ops::Bound::Included(_)),
            limit: key_interval
                .limit
                .and_then(|limit| u32::try_from(limit).ok()),
        }
    }

    fn service_key_interval_query(key_interval: &KeyInterval) -> service_wit::KeyIntervalQuery {
        service_wit::KeyIntervalQuery {
            start: match &key_interval.start {
                KeyIntervalStart::Included(key) | KeyIntervalStart::Excluded(key) => key.clone(),
            },
            start_inclusive: matches!(&key_interval.start, KeyIntervalStart::Included(_)),
            end: match &key_interval.end {
                std::ops::Bound::Included(key) | std::ops::Bound::Excluded(key) => {
                    Some(key.clone())
                }
                std::ops::Bound::Unbounded => None,
            },
            end_inclusive: matches!(&key_interval.end, std::ops::Bound::Included(_)),
            limit: key_interval
                .limit
                .and_then(|limit| u32::try_from(limit).ok()),
        }
    }

    /// Creates a promise for testing if a key exist in the key-value store
    fn contains_key_new(&self, key: &[u8]) -> u32 {
        match self {
            WitInterface::Contract => contract_wit::contains_key_new(key),
            WitInterface::Service => service_wit::contains_key_new(key),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.contains_key_new(key),
        }
    }

    /// Resolves a promise for testing if a key exist in the key-value store
    fn contains_key_wait(&self, promise: u32) -> bool {
        match self {
            WitInterface::Contract => contract_wit::contains_key_wait(promise),
            WitInterface::Service => service_wit::contains_key_wait(promise),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.contains_key_wait(promise),
        }
    }

    /// Creates a promise for testing if multiple keys exist in the key-value store
    fn contains_keys_new(&self, keys: &[Vec<u8>]) -> u32 {
        match self {
            WitInterface::Contract => contract_wit::contains_keys_new(keys),
            WitInterface::Service => service_wit::contains_keys_new(keys),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.contains_keys_new(keys),
        }
    }

    /// Resolves a promise for testing if multiple keys exist in the key-value store
    fn contains_keys_wait(&self, promise: u32) -> Vec<bool> {
        match self {
            WitInterface::Contract => contract_wit::contains_keys_wait(promise),
            WitInterface::Service => service_wit::contains_keys_wait(promise),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.contains_keys_wait(promise),
        }
    }

    /// Creates a promise for reading multiple keys in the key-value store
    fn read_multi_values_bytes_new(&self, keys: &[Vec<u8>]) -> u32 {
        match self {
            WitInterface::Contract => contract_wit::read_multi_values_bytes_new(keys),
            WitInterface::Service => service_wit::read_multi_values_bytes_new(keys),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.read_multi_values_bytes_new(keys),
        }
    }

    /// Resolves a promise for reading multiple keys in the key-value store
    fn read_multi_values_bytes_wait(&self, promise: u32) -> Vec<Option<Vec<u8>>> {
        match self {
            WitInterface::Contract => contract_wit::read_multi_values_bytes_wait(promise),
            WitInterface::Service => service_wit::read_multi_values_bytes_wait(promise),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.read_multi_values_bytes_wait(promise),
        }
    }

    /// Creates a promise for reading a key in the key-value store
    fn read_value_bytes_new(&self, key: &[u8]) -> u32 {
        match self {
            WitInterface::Contract => contract_wit::read_value_bytes_new(key),
            WitInterface::Service => service_wit::read_value_bytes_new(key),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.read_value_bytes_new(key),
        }
    }

    /// Resolves a promise for reading a key in the key-value store
    fn read_value_bytes_wait(&self, promise: u32) -> Option<Vec<u8>> {
        match self {
            WitInterface::Contract => contract_wit::read_value_bytes_wait(promise),
            WitInterface::Service => service_wit::read_value_bytes_wait(promise),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.read_value_bytes_wait(promise),
        }
    }

    /// Creates a promise for finding keys in an interval in the key-value store.
    fn find_keys_in_interval_new(&self, key_interval: &KeyInterval) -> u32 {
        match self {
            WitInterface::Contract => contract_wit::find_keys_in_interval_new(
                &Self::contract_key_interval_query(key_interval),
            ),
            WitInterface::Service => service_wit::find_keys_in_interval_new(
                &Self::service_key_interval_query(key_interval),
            ),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => {
                store.find_keys_in_interval_new(key_interval.clone())
            }
        }
    }

    /// Resolves a promise for finding keys in an interval in the key-value store.
    fn find_keys_in_interval_wait(&self, promise: u32) -> IntervalKeys {
        match self {
            WitInterface::Contract => {
                let result = contract_wit::find_keys_in_interval_wait(promise);
                (result.keys, result.is_finished)
            }
            WitInterface::Service => {
                let result = service_wit::find_keys_in_interval_wait(promise);
                (result.keys, result.is_finished)
            }
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.find_keys_in_interval_wait(promise),
        }
    }

    /// Creates a promise for finding key-values in an interval in the key-value store.
    fn find_key_values_in_interval_new(&self, key_interval: &KeyInterval) -> u32 {
        match self {
            WitInterface::Contract => contract_wit::find_key_values_in_interval_new(
                &Self::contract_key_interval_query(key_interval),
            ),
            WitInterface::Service => service_wit::find_key_values_in_interval_new(
                &Self::service_key_interval_query(key_interval),
            ),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => {
                store.find_key_values_in_interval_new(key_interval.clone())
            }
        }
    }

    /// Resolves a promise for finding key-values in an interval in the key-value store.
    fn find_key_values_in_interval_wait(&self, promise: u32) -> IntervalKeyValues {
        match self {
            WitInterface::Contract => {
                let result = contract_wit::find_key_values_in_interval_wait(promise);
                (result.key_values, result.is_finished)
            }
            WitInterface::Service => {
                let result = service_wit::find_key_values_in_interval_wait(promise);
                (result.key_values, result.is_finished)
            }
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.find_key_values_in_interval_wait(promise),
        }
    }

    /// Calls the `write_batch` WIT function.
    fn write_batch(&self, batch: Batch) {
        match self {
            WitInterface::Contract => {
                let batch_operations = batch
                    .operations
                    .into_iter()
                    .map(WriteOperation::from)
                    .collect::<Vec<_>>();

                contract_runtime_api::write_batch(&batch_operations);
            }
            WitInterface::Service => panic!("Attempt to modify storage from a service"),
            #[cfg(with_testing)]
            WitInterface::Mock {
                store,
                read_only: false,
            } => {
                store.write_batch(batch);
            }
            #[cfg(with_testing)]
            WitInterface::Mock {
                read_only: true, ..
            } => {
                panic!("Attempt to modify storage from a service")
            }
        }
    }
}

/// Implementation of [`linera_views::context::Context`] to be used for data storage
/// by Linera applications.
pub type ViewStorageContext = linera_views::context::ViewContext<(), KeyValueStore>;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_key_value_store_mock() -> anyhow::Result<()> {
        // Create a mock key-value store for testing
        let store = KeyValueStore::mock();
        let mock_store = store.to_mut();

        // Check if key exists
        let is_key_existing = mock_store.contains_key(b"foo").await?;
        assert!(!is_key_existing);

        // Check if keys exist
        let is_keys_existing = mock_store
            .contains_keys(&[b"foo".to_vec(), b"bar".to_vec()])
            .await?;
        assert!(!is_keys_existing[0]);
        assert!(!is_keys_existing[1]);

        // Read and write values
        let mut batch = Batch::new();
        batch.put_key_value(b"foo".to_vec(), &32_u128)?;
        batch.put_key_value(b"bar".to_vec(), &42_u128)?;
        mock_store.write_batch(batch).await?;

        let is_key_existing = mock_store.contains_key(b"foo").await?;
        assert!(is_key_existing);

        let value = mock_store.read_value(b"foo").await?;
        assert_eq!(value, Some(32_u128));

        let value = mock_store.read_value(b"bar").await?;
        assert_eq!(value, Some(42_u128));

        Ok(())
    }
}
