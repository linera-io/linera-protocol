// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types that interface with the system API available to application services but
//! that shouldn't be used by applications directly.

use super::super::service_system_api as wit;
use crate::views::ViewStorageContext;
use linera_base::identifiers::ApplicationId;
use linera_views::views::View;

/// Helper function to load the service state or create a new one if it doesn't exist.
pub async fn load_view<State: View<ViewStorageContext>>() -> State {
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
pub fn query_application(application: ApplicationId, argument: &[u8]) -> Result<Vec<u8>, String> {
    wit::try_query_application(application.into(), argument)
}
