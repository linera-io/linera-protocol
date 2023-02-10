// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::SimpleCrowdFunding;
use async_trait::async_trait;
use linera_sdk::{QueryContext, Service, SimpleStateStorage};
use serde::Deserialize;
use thiserror::Error;

linera_sdk::service!(SimpleCrowdFunding);

#[async_trait]
impl Service for SimpleCrowdFunding {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn query_application(
        &self,
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

impl SimpleCrowdFunding {
    /// Returns the total amount of tokens pledged to this campaign.
    fn pledged(&self) -> u128 {
        self.pledges.values().sum()
    }
}

/// Queries that can be made to the [`SimpleCrowdFunding`] application service.
#[derive(Clone, Copy, Debug, Deserialize)]
pub enum Query {
    /// The current [`Status`] of the simple_crowd-funding campaign.
    Status,
    /// The total amount pledged to the simple_crowd-funding campaign.
    Pledged,
    /// The simple_crowd-funding campaign's target.
    Target,
    /// The simple_crowd-funding campaign's deadline.
    Deadline,
    /// The recipient of the pledged amount.
    Owner,
}

/// An error that can occur during the service execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid account query.
    #[error("Invalid account specified in query parameter")]
    InvalidQuery(#[from] bcs::Error),
}
