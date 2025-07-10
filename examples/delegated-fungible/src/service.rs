// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use delegated_fungible::{Operation, OwnerSpender, Parameters};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{AccountOwner, Amount, WithServiceAbi},
    views::{MapView, View},
    Service, ServiceRuntime,
};

use self::state::DelegatedFungibleTokenState;

#[derive(Clone)]
pub struct DelegatedFungibleTokenService {
    state: Arc<DelegatedFungibleTokenState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(DelegatedFungibleTokenService);

impl WithServiceAbi for DelegatedFungibleTokenService {
    type Abi = delegated_fungible::DelegatedFungibleTokenAbi;
}

impl Service for DelegatedFungibleTokenService {
    type Parameters = Parameters;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = DelegatedFungibleTokenState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        DelegatedFungibleTokenService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            self.clone(),
            Operation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
}

#[Object]
impl DelegatedFungibleTokenService {
    async fn accounts(&self) -> &MapView<AccountOwner, Amount> {
        &self.state.accounts
    }

    async fn allowances(&self) -> &MapView<OwnerSpender, Amount> {
        &self.state.allowances
    }

    async fn ticker_symbol(&self) -> Result<String, async_graphql::Error> {
        Ok(self.runtime.application_parameters().ticker_symbol)
    }
}
