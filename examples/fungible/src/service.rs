// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use fungible::{OwnerSpender, Parameters};
use linera_sdk::{
    abis::fungible::FungibleOperation,
    graphql::GraphQLMutationRoot as _,
    linera_base_types::{AccountOwner, Amount, WithServiceAbi},
    views::{MapView, View},
    Service, ServiceRuntime,
};

use self::state::FungibleTokenState;

#[derive(Clone)]
pub struct FungibleTokenService {
    state: Arc<FungibleTokenState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(FungibleTokenService);

impl WithServiceAbi for FungibleTokenService {
    type Abi = fungible::FungibleTokenAbi;
}

impl Service for FungibleTokenService {
    type Parameters = Parameters;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = FungibleTokenState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        FungibleTokenService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            self.clone(),
            FungibleOperation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
}

#[Object]
impl FungibleTokenService {
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
