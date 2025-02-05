// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use amm::{Operation, Parameters};
use async_graphql::{EmptySubscription, Request, Response, Schema};
use linera_sdk::{
    base::WithServiceAbi, graphql::GraphQLMutationRoot, views::View, Service, ServiceRuntime,
};

use self::state::AmmState;

pub struct AmmService {
    state: Arc<AmmState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(AmmService);

impl WithServiceAbi for AmmService {
    type Abi = amm::AmmAbi;
}

impl Service for AmmService {
    type Parameters = Parameters;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = AmmState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        AmmService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            self.state.clone(),
            Operation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
}
