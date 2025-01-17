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

#[derive(Clone, async_graphql::SimpleObject)]
pub struct EthereumTrackerService {
    #[graphql(flatten)]
    state: Arc<EthereumTrackerState>,
    #[graphql(skip)]
    runtime: Arc<ServiceRuntime<Self>>,
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
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            Query {
                service: self.clone(),
            },
            Operation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
}

/// The service handler for GraphQL queries.
#[derive(async_graphql::SimpleObject)]
pub struct Query {
    #[graphql(flatten)]
    service: EthereumTrackerService,
}
