// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(target_arch = "wasm32")]

mod state;
mod types;

use self::state::{ApplicationState, FungibleToken};
use async_trait::async_trait;
use linera_sdk::{QueryContext, Service};
use thiserror::Error;

linera_sdk::service!(FungibleToken);

#[async_trait]
impl Service for FungibleToken {
    type Error = Error;

    async fn query_application(
        &self,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        let account = bcs::from_bytes(argument)?;

        Ok(
            bcs::to_bytes(&self.balance(&account))
                .expect("Serialization of `u128` should not fail"),
        )
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid account query.
    #[error("Invalid account specified in query parameter")]
    InvalidAccount(#[from] bcs::Error),
}
