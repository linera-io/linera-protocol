// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{super::ApplicationState, queryable_system as system};
use futures::future;
use linera_sdk::{ApplicationId, ChainId, SystemBalance, Timestamp};
use std::future::Future;

impl ApplicationState {
    /// Load the service state, without locking it for writes.
    pub async fn load() -> Self {
        let future = system::Load::new();
        Self::load_using(future::poll_fn(|_context| future.poll().into())).await
    }

    /// Helper function to load the service state or create a new one if it doesn't exist.
    pub async fn load_using(future: impl Future<Output = Result<Vec<u8>, String>>) -> Self {
        let bytes = future.await.expect("Failed to load service state");
        if bytes.is_empty() {
            Self::default()
        } else {
            bcs::from_bytes(&bytes).expect("Invalid service state")
        }
    }

    /// Retrieve the current chain ID.
    #[allow(dead_code)]
    pub fn current_chain_id() -> ChainId {
        ChainId(system::chain_id().into())
    }

    /// Retrieve the current application ID.
    #[allow(dead_code)]
    pub fn current_application_id() -> ApplicationId {
        system::application_id().into()
    }

    /// Retrieve the current system balance.
    #[allow(dead_code)]
    pub fn current_system_balance() -> SystemBalance {
        system::read_system_balance().into()
    }

    /// Retrieve the current system time.
    #[allow(dead_code)]
    pub fn current_system_time() -> Timestamp {
        system::read_system_time().into()
    }
}
