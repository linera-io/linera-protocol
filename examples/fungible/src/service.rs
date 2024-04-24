// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::{Arc, Mutex};

use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use fungible::{Operation, Parameters};
use linera_sdk::{
    base::{AccountOwner, Amount, WithServiceAbi},
    graphql::GraphQLMutationRoot,
    views::MapView,
    Service, ServiceRuntime,
};
use thiserror::Error;

use self::state::FungibleToken;

#[derive(Clone)]
pub struct FungibleTokenService {
    state: Arc<FungibleToken>,
    runtime: Arc<Mutex<ServiceRuntime<Self>>>,
}

linera_sdk::service!(FungibleTokenService);

impl WithServiceAbi for FungibleTokenService {
    type Abi = fungible::FungibleTokenAbi;
}

impl Service for FungibleTokenService {
    type Error = Error;
    type State = FungibleToken;
    type Parameters = Parameters;

    async fn new(state: Self::State, runtime: ServiceRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(FungibleTokenService {
            state: Arc::new(state),
            runtime: Arc::new(Mutex::new(runtime)),
        })
    }

    async fn handle_query(&self, request: Request) -> Result<Response, Self::Error> {
        let schema =
            Schema::build(self.clone(), Operation::mutation_root(), EmptySubscription).finish();
        let response = schema.execute(request).await;
        Ok(response)
    }
}

#[Object]
impl FungibleTokenService {
    async fn accounts(&self) -> &MapView<AccountOwner, Amount> {
        &self.state.accounts
    }

    async fn ticker_symbol(&self) -> Result<String, async_graphql::Error> {
        let runtime = self
            .runtime
            .try_lock()
            .expect("Services only run in a single-thread");
        Ok(runtime.application_parameters().ticker_symbol)
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
