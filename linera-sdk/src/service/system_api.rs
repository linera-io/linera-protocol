// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types to interface with the system API available to application services.

use super::service_system_api as wit;
use crate::views::ViewStorageContext;
use futures::future;
use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::{ApplicationId, ChainId},
};
use linera_views::views::{View, ViewError};
use serde::de::DeserializeOwned;
use std::{fmt, future::Future, task::Poll};

/// Loads the application state, without locking it for writes.
pub async fn load<State>() -> State
where
    State: Default + DeserializeOwned,
{
    let future = wit::Load::new();
    load_using(future::poll_fn(|_context| future.poll().into())).await
}

/// Helper function to load the application state or create a new one if it doesn't exist.
async fn load_using<State>(future: impl Future<Output = Result<Vec<u8>, String>>) -> State
where
    State: Default + DeserializeOwned,
{
    let bytes = future.await.expect("Failed to load application state");
    if bytes.is_empty() {
        State::default()
    } else {
        bcs::from_bytes(&bytes).expect("Invalid application state")
    }
}

/// Loads the service state, without locking it for writes.
pub async fn lock_and_load_view<State: View<ViewStorageContext>>() -> State {
    let future = wit::Lock::new();
    future::poll_fn(|_context| -> Poll<Result<(), ViewError>> { future.poll().into() })
        .await
        .expect("Failed to lock application state");
    load_view_using::<State>().await
}

/// Loads the service state, without locking it for writes.
pub async fn unlock_view() {
    let future = wit::Unlock::new();
    future::poll_fn(|_context| future.poll().into()).await;
}

/// Helper function to load the service state or create a new one if it doesn't exist.
pub async fn load_view_using<State: View<ViewStorageContext>>() -> State {
    let context = ViewStorageContext::default();
    State::load(context)
        .await
        .expect("Failed to load application state")
}

/// Retrieves the current chain ID.
pub fn current_chain_id() -> ChainId {
    ChainId(wit::chain_id().into())
}

/// Retrieves the current application ID.
pub fn current_application_id() -> ApplicationId {
    wit::application_id().into()
}

/// Retrieves the current application parameters.
pub fn current_application_parameters() -> Vec<u8> {
    wit::application_parameters()
}

/// Retrieves the current system balance.
pub fn current_system_balance() -> Amount {
    wit::read_system_balance().into()
}

/// Retrieves the current system time.
pub fn current_system_time() -> Timestamp {
    wit::read_system_timestamp().into()
}

/// Queries another application.
pub async fn query_application(
    application: ApplicationId,
    argument: &[u8],
) -> Result<Vec<u8>, String> {
    let future = wit::TryQueryApplication::new(application.into(), argument);

    future::poll_fn(|_context| future.poll().into()).await
}

/// Requests the host to log a message.
///
/// Useful for debugging locally, but may be ignored by validators.
pub fn log(message: &fmt::Arguments<'_>, level: log::Level) {
    wit::log(&message.to_string(), level.into());
}
