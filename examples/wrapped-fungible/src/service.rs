// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use fungible::OwnerSpender;
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{AccountOwner, WithServiceAbi, U128},
    views::{MapView, View},
    Service, ServiceRuntime,
};
use wrapped_fungible::{WrappedFungibleOperation, WrappedFungibleTokenAbi, WrappedParameters};

use crate::state::WrappedFungibleTokenState;

#[derive(Clone)]
pub struct WrappedFungibleTokenService {
    state: Arc<WrappedFungibleTokenState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(WrappedFungibleTokenService);

impl WithServiceAbi for WrappedFungibleTokenService {
    type Abi = WrappedFungibleTokenAbi;
}

impl Service for WrappedFungibleTokenService {
    type Parameters = WrappedParameters;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = WrappedFungibleTokenState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        WrappedFungibleTokenService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        self.schema().execute(request).await
    }
}

impl WrappedFungibleTokenService {
    /// Builds the GraphQL schema served by [`Self::handle_query`].
    fn schema(
        &self,
    ) -> Schema<
        Self,
        <WrappedFungibleOperation as GraphQLMutationRoot<WrappedFungibleTokenService>>::MutationRoot,
        EmptySubscription,
    >{
        Schema::build(
            self.clone(),
            WrappedFungibleOperation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
        .finish()
    }
}

#[Object]
impl WrappedFungibleTokenService {
    async fn accounts(&self) -> &MapView<AccountOwner, U128> {
        &self.state.accounts
    }

    async fn allowances(&self) -> &MapView<OwnerSpender, U128> {
        &self.state.allowances
    }

    async fn ticker_symbol(&self) -> Result<String, async_graphql::Error> {
        let params: WrappedParameters = self.runtime.application_parameters();
        Ok(params.ticker_symbol)
    }

    /// The number of decimal places used by the source ERC-20.
    async fn decimals(&self) -> u8 {
        let params: WrappedParameters = self.runtime.application_parameters();
        params.decimals
    }

    /// The ERC-20 token address on the source EVM chain (hex-encoded).
    async fn evm_token_address(&self) -> String {
        let params: WrappedParameters = self.runtime.application_parameters();
        format!("0x{}", hex::encode(params.evm_token_address))
    }

    /// The EVM chain ID of the source chain.
    async fn evm_source_chain_id(&self) -> u64 {
        let params: WrappedParameters = self.runtime.application_parameters();
        params.evm_source_chain_id
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use linera_sdk::{util::BlockingWait, views::View, ServiceRuntime};

    use super::*;

    #[test]
    fn schema_sdl() {
        let runtime = ServiceRuntime::<WrappedFungibleTokenService>::new();
        let state = WrappedFungibleTokenState::load(runtime.root_view_storage_context())
            .blocking_wait()
            .expect("Failed to read from mock key value store");

        let service = WrappedFungibleTokenService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        };

        insta::assert_snapshot!(service.schema().sdl());
    }
}
