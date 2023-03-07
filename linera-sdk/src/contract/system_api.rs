// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::writable_system as system;
use crate::{ApplicationId, ChainId, SessionId, SystemBalance, Timestamp};
use async_trait::async_trait;
use futures::future;
use linera_views::{
    common::{Batch, ContextFromDb, KeyValueStoreClient, WriteOperation},
    views::{RootView, View, ViewError},
};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt, task::Poll};

/// Loads the contract state, without locking it for writes.
pub async fn load<State>() -> State
where
    State: Default + DeserializeOwned,
{
    let future = system::Load::new();
    let state_bytes = future::poll_fn(|_context| future.poll().into()).await;
    deserialize_state(state_bytes)
}

/// Loads the contract state and locks it for writes.
pub async fn load_and_lock<State>() -> Option<State>
where
    State: Default + DeserializeOwned,
{
    let future = system::LoadAndLock::new();
    let state_bytes =
        future::poll_fn(|_context| Poll::<Option<Vec<u8>>>::from(future.poll())).await?;
    Some(deserialize_state(state_bytes))
}

/// Deserializes the contract state or creates a new one if the `bytes` vector is empty.
fn deserialize_state<State>(bytes: Vec<u8>) -> State
where
    State: Default + DeserializeOwned,
{
    if bytes.is_empty() {
        State::default()
    } else {
        bcs::from_bytes(&bytes).expect("Invalid contract state")
    }
}

/// Save the contract state and unlock it.
pub async fn store_and_unlock<State>(state: State)
where
    State: Serialize,
{
    system::store_and_unlock(&bcs::to_bytes(&state).expect("State serialization failed"));
}

#[derive(Default, Clone)]
pub struct WasmClient;

impl WasmClient {
    async fn find_keys_by_prefix_load(&self, key_prefix: &[u8]) -> Vec<Vec<u8>> {
        let future = system::FindKeys::new(key_prefix);
        future::poll_fn(|_context| future.poll().into()).await
    }

    async fn find_key_values_by_prefix_load(&self, key_prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let future = system::FindKeyValues::new(key_prefix);
        future::poll_fn(|_context| future.poll().into()).await
    }
}

#[async_trait]
impl KeyValueStoreClient for WasmClient {
    type Error = ViewError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let future = system::ReadKeyBytes::new(key);
        Ok(future::poll_fn(|_context| future.poll().into()).await)
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

    async fn write_batch(&self, batch: Batch) -> Result<(), ViewError> {
        let mut list_oper = Vec::new();
        for op in &batch.operations {
            match op {
                WriteOperation::Delete { key } => {
                    list_oper.push(system::WriteOperation::Delete(key));
                }
                WriteOperation::Put { key, value } => {
                    list_oper.push(system::WriteOperation::Put((key, value)))
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    list_oper.push(system::WriteOperation::Deleteprefix(key_prefix))
                }
            }
        }
        let future = system::WriteBatch::new(&list_oper);
        let () = future::poll_fn(|_context| future.poll().into()).await;
        Ok(())
    }
}

pub type WasmContext = ContextFromDb<(), WasmClient>;

pub trait WasmContextExt {
    fn new() -> Self;
}

impl WasmContextExt for WasmContext {
    fn new() -> Self {
        Self {
            db: WasmClient::default(),
            base_key: Vec::new(),
            extra: (),
        }
    }
}

/// Load the contract state and lock it for writes.
pub async fn load_and_lock_view<State: View<WasmContext>>() -> Option<State> {
    let future = system::Lock::new();
    if future::poll_fn(|_context| future.poll().into()).await {
        Some(load_view_using::<State>().await)
    } else {
        None
    }
}

/// Helper function to load the contract state or create a new one if it doesn't exist.
pub async fn load_view_using<State: View<WasmContext>>() -> State {
    let context = WasmContext::new();
    let r = State::load(context).await;
    r.expect("Failed to load contract state")
}

/// Save the contract state and unlock it.
pub async fn store_and_unlock_view<State: RootView<WasmContext>>(mut state: State) {
    state.save().await.expect("save operation failed");
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
pub fn current_system_balance() -> SystemBalance {
    system::read_system_balance().into()
}

/// Retrieves the current system time.
pub fn current_system_time() -> Timestamp {
    system::read_system_timestamp().into()
}

/// Calls another application.
pub fn call_application(
    authenticated: bool,
    application: ApplicationId,
    argument: &[u8],
    forwarded_sessions: Vec<SessionId>,
) -> (Vec<u8>, Vec<SessionId>) {
    let forwarded_sessions: Vec<_> = forwarded_sessions
        .into_iter()
        .map(system::SessionId::from)
        .collect();

    system::try_call_application(
        authenticated,
        application.into(),
        argument,
        &forwarded_sessions,
    )
    .into()
}

/// Calls another application's session.
pub async fn call_session(
    authenticated: bool,
    session: SessionId,
    argument: &[u8],
    forwarded_sessions: Vec<SessionId>,
) -> (Vec<u8>, Vec<SessionId>) {
    let forwarded_sessions: Vec<_> = forwarded_sessions
        .into_iter()
        .map(system::SessionId::from)
        .collect();

    let future =
        system::TryCallSession::new(authenticated, session.into(), argument, &forwarded_sessions);

    future::poll_fn(|_context| future.poll().into()).await
}

/// Requests the host to log a message.
///
/// Useful for debugging locally, but may be ignored by validators.
pub fn log(message: &fmt::Arguments<'_>, level: log::Level) {
    system::log(&message.to_string(), level.into());
}
