// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use async_trait::async_trait;
use crowd_funding::Operation;

use linera_sdk::{
    base::WithServiceAbi, graphql::GraphQLMutationRoot, QueryContext, Service, ViewStateStorage,
};
use state::CrowdFunding;
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(CrowdFunding);

impl WithServiceAbi for CrowdFunding {
    type Abi = crowd_funding::CrowdFundingAbi;
}

#[async_trait]
impl Service for CrowdFunding {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        request: Request,
    ) -> Result<Response, Self::Error> {
        let schema =
            Schema::build(self.clone(), Operation::mutation_root(), EmptySubscription).finish();
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
