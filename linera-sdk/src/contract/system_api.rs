// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::writable_system as system;
use crate::{ApplicationId, ChainId, SessionId, SystemBalance, Timestamp};
use async_trait::async_trait;
use futures::future;
use linera_views::{
    common::{Batch, ContextFromDb, KeyValueOperations, WriteOperation},
    views::{ContainerView, View, ViewError},
};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt, future::Future};

#[derive(Default, Clone)]
pub struct HostContractWasmClient;

impl HostContractWasmClient {
    async fn find_keys_by_prefix_load(&self, key_prefix: &[u8]) -> Result<Vec<Vec<u8>>, ViewError> {
        let future = system::ViewFindKeys::new(key_prefix);
        future::poll_fn(|_context| future.poll().into()).await
    }

    async fn find_key_values_by_prefix_load(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ViewError> {
        let future = system::ViewFindKeyValues::new(key_prefix);
        future::poll_fn(|_context| future.poll().into()).await
    }
}

#[async_trait]
impl KeyValueOperations for HostContractWasmClient {
    type Error = ViewError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let future = system::ViewReadKeyBytes::new(key);
        let r = future::poll_fn(|_context| future.poll().into()).await;
        r
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

    async fn write_batch(&self, batch: Batch) -> Result<(), ViewError> {
        let mut list_oper = Vec::new();
        for op in &batch.operations {
            match op {
                WriteOperation::Delete { key } => {
                    list_oper.push(system::ViewWriteOperation::Delete(key));
                }
                WriteOperation::Put { key, value } => {
                    list_oper.push(system::ViewWriteOperation::Put((key, value)))
                }
                WriteOperation::DeletePrefix { key_prefix } => {
                    list_oper.push(system::ViewWriteOperation::Deleteprefix(key_prefix))
                }
            }
        }
        let future = system::ViewWriteBatch::new(&list_oper);
        future::poll_fn(|_context| future.poll().into()).await
    }
}

pub type HostContractWasmContext = ContextFromDb<(), HostContractWasmClient>;

pub trait DefaultContractContext {
    fn new() -> Self;
}

impl DefaultContractContext for HostContractWasmContext {
    fn new() -> Self {
        Self {
            db: HostContractWasmClient::default(),
            base_key: Vec::new(),
            extra: (),
        }
    }
}

/// Load the contract state and lock it for writes.
pub async fn view_lock<State: View<HostContractWasmContext>>() -> State {
    let future = system::ViewLock::new();
    future::poll_fn(|_context| {
        let r2: std::task::Poll<Result<(), ViewError>> = future.poll().into();
        r2
    })
    .await
    .expect("error happens in the into operation");
    view_load_using::<State>().await
}

/// Helper function to load the contract state or create a new one if it doesn't exist.
pub async fn view_load_using<State: View<HostContractWasmContext>>() -> State {
    let context = HostContractWasmContext::new();
    let r = State::load(context).await;
    r.expect("Failed to load contract state")
}

/// Save the contract state and unlock it.
pub async fn view_store_and_unlock<State: ContainerView<HostContractWasmContext>>(mut state: State) {
    state.save().await.expect("save operation failed");
}

#[derive(Default, Clone)]
pub struct WasmClient;

impl WasmClient {
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
impl KeyValueOperations for WasmClient {
    type Error = ViewError;
    type Keys = Vec<Vec<u8>>;
    type KeyValues = Vec<(Vec<u8>, Vec<u8>)>;

    async fn read_key_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let future = system::ReadKeyBytes::new(key);
        let r = future::poll_fn(|_context| future.poll().into()).await;
        r
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
        future::poll_fn(|_context| future.poll().into()).await
    }
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

/// Calls another application.
pub async fn call_application(
    authenticated: bool,
    application: ApplicationId,
    argument: &[u8],
    forwarded_sessions: Vec<SessionId>,
) -> Result<(Vec<u8>, Vec<SessionId>), String> {
    let forwarded_sessions: Vec<_> = forwarded_sessions
        .into_iter()
        .map(system::SessionId::from)
        .collect();

    let future = system::TryCallApplication::new(
        authenticated,
        application.into(),
        argument,
        &forwarded_sessions,
    );

    future::poll_fn(|_context| future.poll().into()).await
}

/// Calls another application's session.
pub async fn call_session(
    authenticated: bool,
    session: SessionId,
    argument: &[u8],
    forwarded_sessions: Vec<SessionId>,
) -> Result<(Vec<u8>, Vec<SessionId>), String> {
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
