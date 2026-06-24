// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use fungible::{state::FungibleTokenState, OwnerSpender, Parameters};
use linera_sdk::{
    abis::fungible::FungibleOperation,
    graphql::GraphQLMutationRoot,
    linera_base_types::{AccountOwner, Amount, WithServiceAbi},
    views::{MapView, View},
    Service, ServiceRuntime,
};

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
        self.schema().execute(request).await
    }
}

impl FungibleTokenService {
    /// Builds the GraphQL schema served by [`Self::handle_query`].
    fn schema(
        &self,
    ) -> Schema<
        Self,
        <FungibleOperation as GraphQLMutationRoot<FungibleTokenService>>::MutationRoot,
        EmptySubscription,
    > {
        Schema::build(
            self.clone(),
            FungibleOperation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
        .finish()
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use linera_sdk::{util::BlockingWait, views::View, ServiceRuntime};

    use super::*;

    #[test]
    fn schema_sdl() {
        let runtime = ServiceRuntime::<FungibleTokenService>::new();
        let state = FungibleTokenState::load(runtime.root_view_storage_context())
            .blocking_wait()
            .expect("Failed to read from mock key value store");

        let service = FungibleTokenService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        };

        insta::assert_snapshot!(service.schema().sdl());
    }
}
