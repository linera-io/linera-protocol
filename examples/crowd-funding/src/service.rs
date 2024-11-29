// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use crowd_funding::Operation;
use linera_sdk::{
    base::{ApplicationId, WithServiceAbi},
    graphql::GraphQLMutationRoot,
    views::View,
    Service, ServiceRuntime,
};
use state::CrowdFundingState;

pub struct CrowdFundingService {
    state: Arc<CrowdFundingState>,
}

linera_sdk::service!(CrowdFundingService);

impl WithServiceAbi for CrowdFundingService {
    type Abi = crowd_funding::CrowdFundingAbi;
}

impl Service for CrowdFundingService {
    type Parameters = ApplicationId<fungible::FungibleTokenAbi>;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = CrowdFundingState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        CrowdFundingService {
            state: Arc::new(state),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            self.state.clone(),
            Operation::mutation_root(),
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
}
