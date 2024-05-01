// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types to interface with the system API available to application views.

#[cfg(with_testing)]
use std::sync::Arc;

use linera_base::ensure;
use linera_views::{
    batch::Batch,
    common::{ContextFromStore, ReadableKeyValueStore, WritableKeyValueStore},
    views::ViewError,
};

#[cfg(with_testing)]
use super::mock_key_value_store::MockKeyValueStore;
use crate::{
    contract::wit::view_system_api::{self as contract_wit, WriteOperation},
    service::wit::view_system_api as service_wit,
    util::yield_once,
};

/// We need to have a maximum key size that handles all possible underlying
/// sizes. The constraint so far is DynamoDb which has a key length of 1024.
/// That key length is decreased by 4 due to the use of a value splitting.
/// Then the [`KeyValueStore`] needs to handle some base_key and so we
/// reduce to 900. Depending on the size, the error can occur in system_api
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
        let promise = self.wit_api.contains_key_new(key);
        yield_once().await;
        Ok(self.wit_api.contains_key_wait(promise))
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ViewError> {
        for key in &keys {
            ensure!(key.len() <= Self::MAX_KEY_SIZE, ViewError::KeyTooLong);
        }
        let promise = self.wit_api.read_multi_values_bytes_new(&keys);
        yield_once().await;
        Ok(self.wit_api.read_multi_values_bytes_wait(promise))
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        ensure!(key.len() <= Self::MAX_KEY_SIZE, ViewError::KeyTooLong);
        let promise = self.wit_api.read_value_bytes_new(key);
        yield_once().await;
        Ok(self.wit_api.read_value_bytes_wait(promise))
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, ViewError> {
        ensure!(
            key_prefix.len() <= Self::MAX_KEY_SIZE,
            ViewError::KeyTooLong
        );
        let promise = self.wit_api.find_keys_new(key_prefix);
        yield_once().await;
        Ok(self.wit_api.find_keys_wait(promise))
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ViewError> {
        ensure!(
            key_prefix.len() <= Self::MAX_KEY_SIZE,
            ViewError::KeyTooLong
        );
        let promise = self.wit_api.find_key_values_new(key_prefix);
        yield_once().await;
        Ok(self.wit_api.find_key_values_wait(promise))
    }
}

impl WritableKeyValueStore<ViewError> for KeyValueStore {
    const MAX_VALUE_SIZE: usize = usize::MAX;

    async fn write_batch(&self, batch: Batch, _base_key: &[u8]) -> Result<(), ViewError> {
        self.wit_api.write_batch(batch);
        Ok(())
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), ViewError> {
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
    /// Calls the `contains_key_new` WIT function.
    fn contains_key_new(&self, key: &[u8]) -> u32 {
        match self {
            WitInterface::Contract => contract_wit::contains_key_new(key),
            WitInterface::Service => service_wit::contains_key_new(key),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.contains_key_new(key),
        }
    }

    /// Calls the `contains_key_wait` WIT function.
    fn contains_key_wait(&self, promise: u32) -> bool {
        match self {
            WitInterface::Contract => contract_wit::contains_key_wait(promise),
            WitInterface::Service => service_wit::contains_key_wait(promise),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.contains_key_wait(promise),
        }
    }

    /// Calls the `read_multi_values_bytes_new` WIT function.
    fn read_multi_values_bytes_new(&self, keys: &[Vec<u8>]) -> u32 {
        match self {
            WitInterface::Contract => contract_wit::read_multi_values_bytes_new(keys),
            WitInterface::Service => service_wit::read_multi_values_bytes_new(keys),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.read_multi_values_bytes_new(keys),
        }
    }

    /// Calls the `read_multi_values_bytes_wait` WIT function.
    fn read_multi_values_bytes_wait(&self, promise: u32) -> Vec<Option<Vec<u8>>> {
        match self {
            WitInterface::Contract => contract_wit::read_multi_values_bytes_wait(promise),
            WitInterface::Service => service_wit::read_multi_values_bytes_wait(promise),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.read_multi_values_bytes_wait(promise),
        }
    }

    /// Calls the `read_value_bytes_new` WIT function.
    fn read_value_bytes_new(&self, key: &[u8]) -> u32 {
        match self {
            WitInterface::Contract => contract_wit::read_value_bytes_new(key),
            WitInterface::Service => service_wit::read_value_bytes_new(key),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.read_value_bytes_new(key),
        }
    }

    /// Calls the `read_value_bytes_wait` WIT function.
    fn read_value_bytes_wait(&self, promise: u32) -> Option<Vec<u8>> {
        match self {
            WitInterface::Contract => contract_wit::read_value_bytes_wait(promise),
            WitInterface::Service => service_wit::read_value_bytes_wait(promise),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.read_value_bytes_wait(promise),
        }
    }

    /// Calls the `find_keys_new` WIT function.
    fn find_keys_new(&self, key_prefix: &[u8]) -> u32 {
        match self {
            WitInterface::Contract => contract_wit::find_keys_new(key_prefix),
            WitInterface::Service => service_wit::find_keys_new(key_prefix),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.find_keys_new(key_prefix),
        }
    }

    /// Calls the `find_keys_wait` WIT function.
    fn find_keys_wait(&self, promise: u32) -> Vec<Vec<u8>> {
        match self {
            WitInterface::Contract => contract_wit::find_keys_wait(promise),
            WitInterface::Service => service_wit::find_keys_wait(promise),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.find_keys_wait(promise),
        }
    }

    /// Calls the `find_key_values_new` WIT function.
    fn find_key_values_new(&self, key_prefix: &[u8]) -> u32 {
        match self {
            WitInterface::Contract => contract_wit::find_key_values_new(key_prefix),
            WitInterface::Service => service_wit::find_key_values_new(key_prefix),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.find_key_values_new(key_prefix),
        }
    }

    /// Calls the `find_key_values_wait` WIT function.
    fn find_key_values_wait(&self, promise: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self {
            WitInterface::Contract => contract_wit::find_key_values_wait(promise),
            WitInterface::Service => service_wit::find_key_values_wait(promise),
            #[cfg(with_testing)]
            WitInterface::Mock { store, .. } => store.find_key_values_wait(promise),
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

                contract_wit::write_batch(&batch_operations);
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

impl linera_views::common::KeyValueStore for KeyValueStore {
    type Error = ViewError;
}

/// Implementation of [`linera_views::common::Context`] to be used for data storage
/// by Linera applications.
pub type ViewStorageContext = ContextFromStore<(), KeyValueStore>;

impl From<KeyValueStore> for ViewStorageContext {
    fn from(store: KeyValueStore) -> Self {
        ContextFromStore {
            store,
            base_key: vec![],
            extra: (),
        }
    }
}
