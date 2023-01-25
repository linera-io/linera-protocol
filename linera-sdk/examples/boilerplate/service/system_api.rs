// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::super::ApplicationState;
use linera_sdk::service::system_api;

impl ApplicationState {
    /// Load the service state, without locking it for writes.
    pub async fn load() -> Self {
        system_api::load().await
    }
}
