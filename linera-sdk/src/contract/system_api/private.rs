// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types that interface with the system API available to application contracts but
//! that shouldn't be used by applications directly.

use super::super::wit_system_api as wit;
use crate::views::ViewStorageContext;
use linera_base::identifiers::ApplicationId;
use linera_views::views::{RootView, View};

/// Retrieves the current application parameters.
pub fn current_application_parameters() -> Vec<u8> {
    wit::application_parameters()
}

/// Loads the application state or create a new one if it doesn't exist.
pub async fn load_view<State: View<ViewStorageContext>>() -> State {
    let context = ViewStorageContext::default();
    let r = State::load(context).await;
    r.expect("Failed to load application state")
}

/// Saves the application state.
pub async fn store_view<State: RootView<ViewStorageContext>>(state: &mut State) {
    state.save().await.expect("save operation failed");
}

/// Calls another application.
pub fn call_application(
    authenticated: bool,
    application: ApplicationId,
    argument: &[u8],
) -> Vec<u8> {
    wit::try_call_application(authenticated, application.into(), argument)
}
