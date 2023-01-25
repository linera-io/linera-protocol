// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::super::ApplicationState;
use linera_sdk::contract::system_api;

#[allow(dead_code)]
impl ApplicationState {
    /// Load the contract state, without locking it for writes.
    pub async fn load() -> Self {
        system_api::load().await
    }

    /// Load the contract state and lock it for writes.
    pub async fn load_and_lock() -> Self {
        system_api::load_and_lock().await
    }

    /// Save the contract state and unlock it.
    pub async fn store_and_unlock(self) {
        system_api::store_and_unlock(self).await
    }
}
