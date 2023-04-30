// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::FungibleToken;
use async_trait::async_trait;
use linera_sdk::{QueryContext, Service, SimpleStateStorage};
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(FungibleToken);

#[async_trait]
impl Service for FungibleToken {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        let account = bcs::from_bytes(argument)?;
        let bytes = bcs::to_bytes(&self.balance(&account))
            .expect("Serialization of amounts should not fail");
        Ok(bytes)
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid account query.
    #[error("Invalid account specified in query parameter")]
    InvalidAccount(#[from] bcs::Error),
}
