// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::FungibleToken;
use linera_views::common::Context;

use async_trait::async_trait;
use linera_sdk::{
    service::system_api::ReadableWasmContext, QueryContext, Service, ViewStateStorage,
};
use linera_views::views::ViewError;
use thiserror::Error;

/// Alias to the application type, so that the boilerplate module can reference it.
pub type ReadableFungibleToken = FungibleToken<ReadableWasmContext>;
linera_sdk::service!(ReadableFungibleToken);

#[async_trait]
impl<C: Context + Send + Sync + Clone + 'static> Service for FungibleToken<C>
where
    ViewError: From<<C as linera_views::common::Context>::Error>,
{
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn query_application(
        &self,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        let account = bcs::from_bytes(argument)?;
        let balance = self.balance(&account).await;

        Ok(bcs::to_bytes(&balance).expect("Serialization of `u128` should not fail"))
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid account query.
    #[error("Invalid account specified in query parameter")]
    InvalidAccount(#[from] bcs::Error),
}
