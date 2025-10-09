// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use graph_ql_queries::{GraphQlQueriesAbi, GraphQlQueriesOperation};
use linera_sdk::{linera_base_types::WithServiceAbi, views::View, Service, ServiceRuntime};
use linera_sdk::graphql::{GraphQLMutationRoot as _};



use self::state::GraphQlQueriesState;

pub struct GraphQlQueriesService {
    state: Arc<GraphQlQueriesState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(GraphQlQueriesService);

impl WithServiceAbi for GraphQlQueriesService {
    type Abi = GraphQlQueriesAbi;
}

impl Service for GraphQlQueriesService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = GraphQlQueriesState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        GraphQlQueriesService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            self.state.clone(),
            GraphQlQueriesOperation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
            .finish();
        schema.execute(request).await
    }
}
