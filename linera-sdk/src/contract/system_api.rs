// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types to interface with the system API available to application contracts.

use super::writable_system as system;
use async_trait::async_trait;
use futures::future;
use linera_base::{
    data_types::{Balance, Timestamp},
    identifiers::{ApplicationId, ChainId, SessionId},
};
use linera_views::{
    batch::{Batch, WriteOperation},
    common::{ContextFromDb, KeyValueStoreClient},
    views::{RootView, View, ViewError},
};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt;

/// Loads the contract state, without locking it for writes.
pub fn load<State>() -> State
where
    State: Default + DeserializeOwned,
{
    let state_bytes = system::load();
    deserialize_state(state_bytes)
}

/// Loads the contract state and locks it for writes.
pub fn load_and_lock<State>() -> Option<State>
where
    State: Default + DeserializeOwned,
{
    let state_bytes = system::load_and_lock()?;
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

/// A type to interface with the key value storage provided to applications.
#[derive(Default, Clone)]
pub struct KeyValueStore;

impl KeyValueStore {
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
impl KeyValueStoreClient for KeyValueStore {
    type Error = ViewError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let future = system::ReadKeyBytes::new(key);
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

    async fn clear_journal(&self, _base_key: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Implementation of [`linera_views::common::Context`] that uses the [`KeyValueStore`] to
/// allow views to store data in the storage layer provided to Linera applications.
pub type ViewStorageContext = ContextFromDb<(), KeyValueStore>;

/// Load the contract state and lock it for writes.
pub async fn load_and_lock_view<State: View<ViewStorageContext>>() -> Option<State> {
    let future = system::Lock::new();
    if future::poll_fn(|_context| future.poll().into()).await {
        Some(load_view_using::<State>().await)
    } else {
        None
    }
}

/// Helper function to load the contract state or create a new one if it doesn't exist.
pub async fn load_view_using<State: View<ViewStorageContext>>() -> State {
    let context = ViewStorageContext::default();
    let r = State::load(context).await;
    r.expect("Failed to load contract state")
}

/// Save the contract state and unlock it.
pub async fn store_and_unlock_view<State: RootView<ViewStorageContext>>(mut state: State) {
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
pub fn current_system_balance() -> Balance {
    system::read_system_balance().into()
}

/// Retrieves the current system time.
pub fn current_system_time() -> Timestamp {
    system::read_system_timestamp().into()
}

/// Calls another application without persisting the current application's state.
///
/// Use the `call_application` method generated by the [`linera-sdk::contract`] macro in order to
/// guarantee the state is up-to-date in reentrant calls.
pub fn call_application_without_persisting_state(
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

/// Calls another application's session without persisting the current application's state.
///
/// Use the `call_session` method generated by the [`linera-sdk::contract`] macro in order to
/// guarantee the state is up-to-date in reentrant calls.
pub fn call_session_without_persisting_state(
    authenticated: bool,
    session: SessionId,
    argument: &[u8],
    forwarded_sessions: Vec<SessionId>,
) -> (Vec<u8>, Vec<SessionId>) {
    let forwarded_sessions: Vec<_> = forwarded_sessions
        .into_iter()
        .map(system::SessionId::from)
        .collect();

    system::try_call_session(authenticated, session.into(), argument, &forwarded_sessions).into()
}

/// Requests the host to log a message.
///
/// Useful for debugging locally, but may be ignored by validators.
pub fn log(message: &fmt::Arguments<'_>, level: log::Level) {
    system::log(&message.to_string(), level.into());
}
