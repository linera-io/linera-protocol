// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types that interface with the system API available to application services but
//! that shouldn't be used by applications directly.

use super::super::service_system_api as wit;
use crate::{util::yield_once, views::ViewStorageContext};
use linera_base::identifiers::ApplicationId;
use linera_views::view::View;
use serde::de::DeserializeOwned;

/// Loads the application state, without locking it for writes.
pub async fn load<State>() -> State
where
    State: Default + DeserializeOwned,
{
    let promise = wit::Load::new();
    yield_once().await;
    let bytes = promise.wait().expect("Failed to load application state");
    if bytes.is_empty() {
        State::default()
    } else {
        bcs::from_bytes(&bytes).expect("Invalid application state")
    }
}

/// Loads the application state (and locks it for writes).
pub async fn lock_and_load_view<State: View<ViewStorageContext>>() -> State {
    let promise = wit::Lock::new();
    yield_once().await;
    promise.wait().expect("Failed to lock application state");
    load_view_using::<State>().await
}

/// Unlocks the service state previously loaded.
pub async fn unlock_view() {
    let promise = wit::Unlock::new();
    yield_once().await;
    promise.wait().expect("Failed to unlock application state");
}

/// Helper function to load the service state or create a new one if it doesn't exist.
pub async fn load_view_using<State: View<ViewStorageContext>>() -> State {
    let context = ViewStorageContext::default();
    State::load(context)
        .await
        .expect("Failed to load application state")
}

/// Retrieves the current application parameters.
pub fn current_application_parameters() -> Vec<u8> {
    wit::application_parameters()
}

/// Queries another application.
pub async fn query_application(
    application: ApplicationId,
    argument: &[u8],
) -> Result<Vec<u8>, String> {
    let promise = wit::TryQueryApplication::new(application.into(), argument);
    yield_once().await;
    promise.wait()
}
