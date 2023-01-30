// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::queryable_system as system;
use crate::{ApplicationId, ChainId, SystemBalance, Timestamp};
use futures::future;
use serde::de::DeserializeOwned;
use std::{fmt, future::Future};

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

/// Requests the host to log a message.
///
/// Useful for debugging locally, but may be ignored by validators.
pub fn log(message: &fmt::Arguments<'_>, level: log::Level) {
    system::log(&message.to_string(), level.into());
}
