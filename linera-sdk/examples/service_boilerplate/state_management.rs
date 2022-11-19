// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{super::ServiceState, system};
use futures::future;
use std::future::Future;

impl ServiceState {
    /// Load the service state, without locking it for writes.
    pub async fn load() -> Self {
        let future = system::Load::new();
        Self::load_using(future::poll_fn(|_context| future.poll().into())).await
    }

    /// Load the service state and lock it for writes.
    pub async fn load_and_lock() -> Self {
        let future = system::LoadAndLock::new();
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

    /// Save the service state and unlock it.
    pub async fn store_and_unlock(self) {
        system::store_and_unlock(&bcs::to_bytes(&self).expect("State serialization failed"));
    }
}
