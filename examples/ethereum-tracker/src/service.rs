// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use ethereum_tracker::Operation;
use linera_sdk::{
    base::WithServiceAbi, graphql::GraphQLMutationRoot, views::View, Service, ServiceRuntime,
};

use self::state::EthereumTrackerState;

#[derive(Clone)]
pub struct EthereumTrackerService {
    state: Arc<EthereumTrackerState>,
}

linera_sdk::service!(EthereumTrackerService);

impl WithServiceAbi for EthereumTrackerService {
    type Abi = ethereum_tracker::EthereumTrackerAbi;
}

impl Service for EthereumTrackerService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = EthereumTrackerState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        EthereumTrackerService {
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
