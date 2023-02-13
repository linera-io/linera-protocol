// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::queryable_system as system;
use crate::{ApplicationId, ChainId, SystemBalance, Timestamp};
use async_trait::async_trait;
use futures::future;
use linera_views::{
    common::{Batch, ContextFromDb, KeyValueOperations},
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

#[derive(Default, Clone)]
pub struct ReadableWasmClient;

impl ReadableWasmClient {
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
impl KeyValueOperations for ReadableWasmClient {
    type Error = ViewError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ViewError> {
        let future = system::ReadKeyBytes::new(key);
        future::poll_fn(|_context| future.poll().into()).await
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

    async fn write_batch(&self, _batch: Batch) -> Result<(), ViewError> {
        Ok(())
    }
}

pub type ReadableWasmContext = ContextFromDb<(), ReadableWasmClient>;

pub trait ReadableWasmContextExt {
    fn new() -> Self;
}

impl ReadableWasmContextExt for ReadableWasmContext {
    fn new() -> Self {
        Self {
            db: ReadableWasmClient::default(),
            base_key: Vec::new(),
            extra: (),
        }
    }
}

/// Load the service state, without locking it for writes.
pub async fn lock_and_load_view<State: View<ReadableWasmContext>>() -> State {
    let future = system::Lock::new();
    future::poll_fn(|_context| -> Poll<Result<(), ViewError>> { future.poll().into() })
        .await
        .expect("Failed to lock contract state");
    load_view_using::<State>().await
}

/// Load the service state, without locking it for writes.
pub async fn unlock_view<State: View<ReadableWasmContext>>(_state: State) {
    let future = system::Unlock::new();
    future::poll_fn(|_context| future.poll().into()).await;
}

/// Helper function to load the service state or create a new one if it doesn't exist.
pub async fn load_view_using<State: View<ReadableWasmContext>>() -> State {
    let context = ReadableWasmContext::new();
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

/// Retrieve the current system balance.
pub fn current_system_balance() -> SystemBalance {
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
