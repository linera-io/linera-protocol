// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

#[allow(unused)]
mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use linera_sdk::{
    base::WithServiceAbi, graphql::GraphQLMutationRoot, views::View, Service, ServiceRuntime,
};
use rfq::Operation;

use self::state::RfqState;

pub struct RfqService {
    state: Arc<RfqState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(RfqService);

impl WithServiceAbi for RfqService {
    type Abi = rfq::RfqAbi;
}

impl Service for RfqService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = RfqState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        RfqService {
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
