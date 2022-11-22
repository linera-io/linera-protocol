// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(target_arch = "wasm32")]

use async_trait::async_trait;
use linera_sdk::{QueryContext, Service};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The application state.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct Counter {
    value: u128,
}

#[async_trait]
impl Service for Counter {
    type Error = Error;

    async fn query_application(
        &self,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        match argument {
            &[] => Ok(bcs::to_bytes(&self.value).expect("Serialization should not fail")),
            _ => Err(Error::InvalidQuery),
        }
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid query argument; Counter application only supports a single (empty) query.
    #[error("Invalid query argument; Counter application only supports a single (empty) query")]
    InvalidQuery,
}

/// Alias to the application type, so that the boilerplate module can reference it.
type ApplicationState = Counter;

mod service_boilerplate;
