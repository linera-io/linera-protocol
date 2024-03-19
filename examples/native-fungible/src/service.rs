// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::NativeFungibleToken;
use async_graphql::{
    ComplexObject, EmptySubscription, Object, Request, Response, Schema, SimpleObject,
};
use fungible::Operation;
use linera_sdk::{
    base::{AccountOwner, Amount, WithServiceAbi},
    graphql::GraphQLMutationRoot,
    service::system_api,
    Service, ServiceRuntime, ViewStateStorage,
};
use native_fungible::TICKER_SYMBOL;
use std::sync::Arc;
use thiserror::Error;

pub struct NativeFungibleTokenService {
    state: Arc<NativeFungibleToken>,
}

linera_sdk::service!(NativeFungibleTokenService);

impl WithServiceAbi for NativeFungibleTokenService {
    type Abi = fungible::FungibleTokenAbi;
}

impl Service for NativeFungibleTokenService {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;
    type State = NativeFungibleToken;

    async fn new(state: Self::State) -> Result<Self, Self::Error> {
        Ok(NativeFungibleTokenService {
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

#[derive(SimpleObject)]
struct AccountEntry {
    value: Amount,
}

#[derive(Default)]
struct Accounts;

#[Object]
impl Accounts {
    // Define a field that lets you query by key
    async fn entry(&self, key: AccountOwner) -> Result<AccountEntry, Error> {
        let owner = match key {
            AccountOwner::User(owner) => owner,
            AccountOwner::Application(_) => return Err(Error::ApplicationsNotSupported),
        };

        let balance = system_api::current_owner_balance(owner);
        Ok(AccountEntry { value: balance })
    }
}

// Implements additional fields not derived from struct members of FungibleToken.
#[ComplexObject]
impl NativeFungibleToken {
    async fn ticker_symbol(&self) -> Result<String, async_graphql::Error> {
        Ok(String::from(TICKER_SYMBOL))
    }

    async fn accounts(&self) -> Result<Accounts, async_graphql::Error> {
        Ok(Accounts)
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

    #[error("Applications not supported yet")]
    ApplicationsNotSupported,
}
