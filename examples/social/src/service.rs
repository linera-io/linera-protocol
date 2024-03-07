// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use async_trait::async_trait;
use linera_sdk::{
    base::WithServiceAbi, graphql::GraphQLMutationRoot, views::ViewError, Service, ServiceRuntime,
    ViewStateStorage,
};
use social::Operation;
use state::Social;
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(Social);

impl WithServiceAbi for Social {
    type Abi = social::SocialAbi;
}

#[async_trait]
impl Service for Social {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn handle_query(
        self: Arc<Self>,
        _runtime: &ServiceRuntime,
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
