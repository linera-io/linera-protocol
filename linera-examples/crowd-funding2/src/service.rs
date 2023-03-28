// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_trait::async_trait;
use crowd_funding::Query;
use linera_sdk::{
    base::Amount, service::system_api::ReadableWasmContext, QueryContext, Service, ViewStateStorage,
};
use state::CrowdFunding;
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(CrowdFunding<ReadableWasmContext>);

#[async_trait]
impl Service for CrowdFunding<ReadableWasmContext> {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        let query = bcs::from_bytes(argument)?;

        let response = match query {
            Query::Status => bcs::to_bytes(&self.status.get()),
            Query::Pledged => bcs::to_bytes(&self.pledged().await),
            Query::Target => bcs::to_bytes(&self.parameters().target),
            Query::Deadline => bcs::to_bytes(&self.parameters().deadline),
            Query::Owner => bcs::to_bytes(&self.parameters().owner),
        }?;

        Ok(response)
    }
}

impl CrowdFunding<ReadableWasmContext> {
    /// Returns the total amount of tokens pledged to this campaign.
    pub async fn pledged(&self) -> Amount {
        let mut total = Amount::zero();
        self.pledges
            .for_each_index_value(|_, value| {
                total.saturating_add_assign(value);
                Ok(())
            })
            .await
            .expect("view iteration should not fail");
        total
    }
}

/// An error that can occur during the service execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid account query.
    #[error("Invalid account specified in query parameter")]
    InvalidQuery(#[from] bcs::Error),
}
