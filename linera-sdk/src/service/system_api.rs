// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types to interface with the system API available to application services.

use super::service_system_api as wit;
use crate::views::ViewStorageContext;
use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::{ApplicationId, ChainId},
};
use linera_views::views::View;
use serde::de::DeserializeOwned;
use std::fmt;

/// Loads the application state, without locking it for writes.
pub(crate) async fn load<State>() -> State
where
    State: Default + DeserializeOwned,
{
    let promise = wit::Load::new();
    let bytes = promise.wait().expect("Failed to load application state");
    if bytes.is_empty() {
        State::default()
    } else {
        bcs::from_bytes(&bytes).expect("Invalid application state")
    }
}

/// Loads the application state (and locks it for writes).
pub(crate) async fn lock_and_load_view<State: View<ViewStorageContext>>() -> State {
    let promise = wit::Lock::new();
    promise.wait().expect("Failed to lock application state");
    load_view_using::<State>().await
}

/// Unlocks the service state previously loaded.
pub(crate) async fn unlock_view() {
    let promise = wit::Unlock::new();
    promise.wait().expect("Failed to unlock application state");
}

/// Helper function to load the service state or create a new one if it doesn't exist.
pub(crate) async fn load_view_using<State: View<ViewStorageContext>>() -> State {
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
pub(crate) fn current_application_parameters() -> Vec<u8> {
    wit::application_parameters()
}

/// Retrieves the current system balance.
pub fn current_system_balance() -> Amount {
    wit::read_system_balance().into()
}

/// Retrieves the current system time, i.e. the timestamp of the latest block in this chain.
pub fn current_system_time() -> Timestamp {
    wit::read_system_timestamp().into()
}

/// Queries another application.
pub(crate) async fn query_application(
    application: ApplicationId,
    argument: &[u8],
) -> Result<Vec<u8>, String> {
    let promise = wit::TryQueryApplication::new(application.into(), argument);
    promise.wait()
}

/// Requests the host to log a message.
///
/// Useful for debugging locally, but may be ignored by validators.
pub fn log(message: &fmt::Arguments<'_>, level: log::Level) {
    wit::log(&message.to_string(), level.into());
}
