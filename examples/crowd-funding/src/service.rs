// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use crowd_funding::Operation;
use linera_sdk::{
    base::{ApplicationId, WithServiceAbi},
    graphql::GraphQLMutationRoot,
    Service, ServiceRuntime,
};
use state::CrowdFunding;
use thiserror::Error;

pub struct CrowdFundingService {
    state: Arc<CrowdFunding>,
}

linera_sdk::service!(CrowdFundingService);

impl WithServiceAbi for CrowdFundingService {
    type Abi = crowd_funding::CrowdFundingAbi;
}

impl Service for CrowdFundingService {
    type Error = Error;
    type State = CrowdFunding;
    type Parameters = ApplicationId<fungible::FungibleTokenAbi>;

    async fn new(state: Self::State, _runtime: ServiceRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(CrowdFundingService {
            state: Arc::new(state),
        })
    }

    async fn handle_query(&self, request: Request) -> Result<Response, Self::Error> {
        let schema = Schema::build(
            self.state.clone(),
            Operation::mutation_root(),
            EmptySubscription,
        )
        .finish();
        let response = schema.execute(request).await;
        Ok(response)
    }
}

/// An error that can occur during the service execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid query argument in crowd-funding app: could not deserialize GraphQL request.
    #[error("Invalid query argument in crowd-funding app: could not deserialize GraphQL request.")]
    InvalidQuery(#[from] serde_json::Error),
}
