// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::FungibleToken;
use async_graphql::{ComplexObject, EmptySubscription, Request, Response, Schema};
use fungible::Operation;
use linera_sdk::{
    base::WithServiceAbi, graphql::GraphQLMutationRoot, Service, ServiceRuntime, ViewStateStorage,
};
use std::sync::Arc;
use thiserror::Error;

pub struct FungibleTokenService {
    state: Arc<FungibleToken>,
}

linera_sdk::service!(FungibleTokenService);

impl WithServiceAbi for FungibleTokenService {
    type Abi = fungible::FungibleTokenAbi;
}

impl Service for FungibleTokenService {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;
    type State = FungibleToken;

    async fn new(state: Self::State, _runtime: ServiceRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(FungibleTokenService {
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

// Implements additional fields not derived from struct members of FungibleToken.
#[ComplexObject]
impl FungibleToken {
    async fn ticker_symbol(&self) -> Result<String, async_graphql::Error> {
        Ok(FungibleTokenService::parameters()?.ticker_symbol)
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid query argument; could not deserialize GraphQL request.
    #[error(
        "Invalid query argument; Fungible application only supports JSON encoded GraphQL queries"
    )]
    InvalidQuery(#[from] serde_json::Error),
}
