// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use async_trait::async_trait;
use crowd_funding::Operation;

use linera_sdk::{
    base::WithServiceAbi, graphql::GraphQLMutationRoot, Service, ServiceRuntime, ViewStateStorage,
};
use state::CrowdFunding;
use std::sync::Arc;
use thiserror::Error;

pub struct CrowdFundingService {
    state: Arc<CrowdFunding>,
}

linera_sdk::service!(CrowdFundingService);

impl WithServiceAbi for CrowdFundingService {
    type Abi = crowd_funding::CrowdFundingAbi;
}

#[async_trait]
impl Service for CrowdFundingService {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;
    type State = CrowdFunding;

    async fn new(state: Self::State) -> Result<Self, Self::Error> {
        Ok(CrowdFundingService {
            state: Arc::new(state),
        })
    }

    async fn handle_query(
        &self,
        _runtime: &ServiceRuntime,
        request: Request,
    ) -> Result<Response, Self::Error> {
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
