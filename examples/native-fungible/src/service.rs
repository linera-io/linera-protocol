// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::NativeFungibleToken;
use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use async_trait::async_trait;
use fungible::Operation;
use linera_sdk::{
    base::{AccountOwner, Amount, WithServiceAbi},
    graphql::GraphQLMutationRoot,
    service::system_api,
    QueryContext, Service, ViewStateStorage,
};
use native_fungible::TICKER_SYMBOL;
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(NativeFungibleToken);

impl WithServiceAbi for NativeFungibleToken {
    type Abi = fungible::FungibleTokenAbi;
}

#[async_trait]
impl Service for NativeFungibleToken {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn handle_query(
        self: Arc<Self>,
        _context: &QueryContext,
        request: Request,
    ) -> Result<Response, Self::Error> {
        let schema =
            Schema::build(QueryRoot, Operation::mutation_root(), EmptySubscription).finish();
        let response = schema.execute(request).await;
        Ok(response)
    }
}

struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn balance(&self, owner: AccountOwner) -> Result<Amount, async_graphql::Error> {
        let owner = match owner {
            AccountOwner::User(owner) => owner,
            AccountOwner::Application(_) => panic!("Applications not supported yet!"),
        };
        Ok(system_api::current_owner_balance(owner))
    }

    async fn ticker_symbol(&self) -> Result<String, async_graphql::Error> {
        Ok(String::from(TICKER_SYMBOL))
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid query argument; could not deserialize GraphQL request.
    #[error(
        "Invalid query argument; Native Fungible application only supports JSON encoded GraphQL queries"
    )]
    InvalidQuery(#[from] serde_json::Error),
}
