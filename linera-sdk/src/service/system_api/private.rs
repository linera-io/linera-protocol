// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types that interface with the system API available to application services but
//! that shouldn't be used by applications directly.

use super::super::service_system_api as wit;
use linera_base::identifiers::ApplicationId;

/// Retrieves the current application parameters.
pub fn current_application_parameters() -> Vec<u8> {
    wit::application_parameters()
}

/// Queries another application.
pub fn query_application(application: ApplicationId, argument: &[u8]) -> Vec<u8> {
    wit::try_query_application(application.into(), argument)
}
