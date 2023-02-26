// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use async_trait::async_trait;
use crowd_funding::Query;
use linera_sdk::{Amount, QueryContext, Service, SimpleStateStorage};
use state::CrowdFunding;
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(CrowdFunding);

#[async_trait]
impl Service for CrowdFunding {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        let query = bcs::from_bytes(argument)?;

        let response = match query {
            Query::Status => bcs::to_bytes(&self.status),
            Query::Pledged => bcs::to_bytes(&self.pledged()),
            Query::Target => bcs::to_bytes(&self.parameters().target),
            Query::Deadline => bcs::to_bytes(&self.parameters().deadline),
            Query::Owner => bcs::to_bytes(&self.parameters().owner),
        }?;

        Ok(response)
    }
}

impl CrowdFunding {
    /// Returns the total amount of tokens pledged to this campaign.
    fn pledged(&self) -> Amount {
        self.pledges.values().sum()
    }
}

/// An error that can occur during the service execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid account query.
    #[error("Invalid account specified in query parameter")]
    InvalidQuery(#[from] bcs::Error),
}
