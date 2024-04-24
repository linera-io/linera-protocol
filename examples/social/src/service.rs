// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use linera_sdk::{
    base::WithServiceAbi, graphql::GraphQLMutationRoot, views::ViewError, Service, ServiceRuntime,
};
use social::Operation;
use state::Social;
use thiserror::Error;

pub struct SocialService {
    state: Arc<Social>,
}

linera_sdk::service!(SocialService);

impl WithServiceAbi for SocialService {
    type Abi = social::SocialAbi;
}

impl Service for SocialService {
    type Error = Error;
    type State = Social;
    type Parameters = ();

    async fn new(state: Self::State, _runtime: ServiceRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(SocialService {
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
    /// Invalid query.
    #[error("Invalid query")]
    InvalidQuery(#[from] serde_json::Error),

    /// Serialization error.
    #[error(transparent)]
    Serialization(#[from] bcs::Error),

    /// View error.
    #[error(transparent)]
    View(#[from] ViewError),
}
