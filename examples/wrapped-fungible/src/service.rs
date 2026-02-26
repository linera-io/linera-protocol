// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use fungible::{state::FungibleTokenState, OwnerSpender};
use linera_sdk::{
    abis::fungible::FungibleOperation,
    graphql::GraphQLMutationRoot,
    linera_base_types::{AccountOwner, Amount, WithServiceAbi},
    views::{MapView, View},
    Service, ServiceRuntime,
};
use wrapped_fungible::{WrappedFungibleTokenAbi, WrappedParameters};

#[derive(Clone)]
pub struct WrappedFungibleTokenService {
    state: Arc<FungibleTokenState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(WrappedFungibleTokenService);

impl WithServiceAbi for WrappedFungibleTokenService {
    type Abi = WrappedFungibleTokenAbi;
}

impl Service for WrappedFungibleTokenService {
    type Parameters = WrappedParameters;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = FungibleTokenState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        WrappedFungibleTokenService {
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
impl WrappedFungibleTokenService {
    async fn accounts(&self) -> &MapView<AccountOwner, Amount> {
        &self.state.accounts
    }

    async fn allowances(&self) -> &MapView<OwnerSpender, Amount> {
        &self.state.allowances
    }

    async fn ticker_symbol(&self) -> Result<String, async_graphql::Error> {
        let params: WrappedParameters = self.runtime.application_parameters();
        Ok(params.ticker_symbol)
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
