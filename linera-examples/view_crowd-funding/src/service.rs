// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::ViewCrowdFunding;
use async_trait::async_trait;
use linera_views::common::Context;
use view_fungible::AccountOwner;
use linera_sdk::{
    service::system_api::HostServiceWasmContext,
    QueryContext, Service, ViewStateStorage,
};
use linera_views::views::ViewError;
use serde::Deserialize;
use thiserror::Error;

/// Alias to the application type, so that the boilerplate module can reference it.
pub type ApplicationState = ViewCrowdFunding<HostServiceWasmContext>;

//linera_sdk::service!(ApplicationState);

#[async_trait]
impl<C: Context + Send + Sync + Clone + 'static> Service for ViewCrowdFunding<C>
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

impl<C: Context + Send + Sync + Clone> ViewCrowdFunding<C>
where
    ViewError: From<<C as linera_views::common::Context>::Error>,
{
    /// Returns the total amount of tokens pledged to this campaign.
    pub async fn pledged(&self) -> u128 {
        let mut total_pledge = 0;
        self.pledges
            .for_each_index_value(
                |_index: AccountOwner, value: u128| -> Result<(), ViewError> {
                    total_pledge += value;
                    Ok(())
                },
            )
            .await
            .expect("for_each_raw_index_value failed");
        total_pledge
    }
}

/// Queries that can be made to the [`ViewCrowdFunding`] application service.
#[derive(Clone, Copy, Debug, Deserialize)]
pub enum Query {
    /// The current [`Status`] of the view_crowd-funding campaign.
    Status,
    /// The total amount pledged to the view_crowd-funding campaign.
    Pledged,
    /// The view_crowd-funding campaign's target.
    Target,
    /// The view_crowd-funding campaign's deadline.
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
