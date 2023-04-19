// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types to interface with the system API available to application contracts.

use super::queryable_system as system;
use async_trait::async_trait;
use futures::future;
use linera_base::{
    data_types::{Balance, Timestamp},
    identifiers::{ApplicationId, ChainId},
};
use linera_views::{
    batch::Batch,
    common::{ContextFromDb, KeyValueStoreClient},
    views::{View, ViewError},
};
use serde::de::DeserializeOwned;
use std::{fmt, future::Future, task::Poll};

/// Load the contract state, without locking it for writes.
pub async fn load<State>() -> State
where
    State: Default + DeserializeOwned,
{
    let future = system::Load::new();
    load_using(future::poll_fn(|_context| future.poll().into())).await
}

/// Helper function to load the contract state or create a new one if it doesn't exist.
async fn load_using<State>(future: impl Future<Output = Result<Vec<u8>, String>>) -> State
where
    State: Default + DeserializeOwned,
{
    let bytes = future.await.expect("Failed to load contract state");
    if bytes.is_empty() {
        State::default()
    } else {
        bcs::from_bytes(&bytes).expect("Invalid contract state")
    }
}

/// A type to interface in a read-only manner with the key value storage provided to applications.
#[derive(Default, Clone)]
pub struct ReadOnlyKeyValueStore;

impl ReadOnlyKeyValueStore {
    async fn find_keys_by_prefix_load(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, ViewError> {
        let future = system::FindKeys::new(key_prefix);
        future::poll_fn(|_context| future.poll().into()).await
    }

    async fn find_key_values_by_prefix_load(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
        let future = system::FindKeyValues::new(key_prefix);
        future::poll_fn(|_context| future.poll().into()).await
    }
}

#[async_trait]
impl KeyValueStoreClient for ReadOnlyKeyValueStore {
    type Error = ViewError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        let future = system::ReadKeyBytes::new(key);
        future::poll_fn(|_context| future.poll().into()).await
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
        let keys = self.find_keys_by_prefix_load(key_prefix).await?;
        Ok(keys)
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, ViewError> {
        let key_values = self.find_key_values_by_prefix_load(key_prefix).await?;
        Ok(key_values)
    }

    async fn write_batch(&self, _batch: Batch, _base_key: &[u8]) -> Result<(), ViewError> {
        unreachable!();
    }

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), Self::Error> {
        unreachable!();
    }
}

/// Implementation of [`linera_views::common::Context`] that uses the [`ReadOnlyKeyValueStore`] to
/// allow views to read data from the storage layer provided to Linera applications.
pub type ReadOnlyViewStorageContext = ContextFromDb<(), ReadOnlyKeyValueStore>;

/// Load the service state, without locking it for writes.
pub async fn lock_and_load_view<State: View<ReadOnlyViewStorageContext>>() -> State {
    let future = system::Lock::new();
    future::poll_fn(|_context| -> Poll<Result<(), ViewError>> { future.poll().into() })
        .await
        .expect("Failed to lock contract state");
    load_view_using::<State>().await
}

/// Load the service state, without locking it for writes.
pub async fn unlock_view() {
    let future = system::Unlock::new();
    future::poll_fn(|_context| future.poll().into()).await;
}

/// Helper function to load the service state or create a new one if it doesn't exist.
pub async fn load_view_using<State: View<ReadOnlyViewStorageContext>>() -> State {
    let context = ReadOnlyViewStorageContext::default();
    State::load(context)
        .await
        .expect("Failed to load contract state")
}

/// Retrieve the current chain ID.
pub fn current_chain_id() -> ChainId {
    ChainId(system::chain_id().into())
}

/// Retrieve the current application ID.
pub fn current_application_id() -> ApplicationId {
    system::application_id().into()
}

/// Retrieve the current application parameters.
pub fn current_application_parameters() -> Vec<u8> {
    system::application_parameters()
}

/// Retrieve the current system balance.
pub fn current_system_balance() -> Balance {
    system::read_system_balance().into()
}

/// Retrieves the current system time.
pub fn current_system_time() -> Timestamp {
    system::read_system_timestamp().into()
}

/// Queries another application.
pub async fn query_application(
    application: ApplicationId,
    argument: &[u8],
) -> Result<Vec<u8>, String> {
    let future = system::TryQueryApplication::new(application.into(), argument);

    future::poll_fn(|_context| future.poll().into()).await
}

/// Requests the host to log a message.
///
/// Useful for debugging locally, but may be ignored by validators.
pub fn log(message: &fmt::Arguments<'_>, level: log::Level) {
    system::log(&message.to_string(), level.into());
}
