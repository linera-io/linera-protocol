// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use linera_sdk::{
    graphql::GraphQLMutationRoot, linera_base_types::WithServiceAbi, views::View, Service,
    ServiceRuntime,
};
use social::Operation;
use state::SocialState;

pub struct SocialService {
    state: Arc<SocialState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(SocialService);

impl WithServiceAbi for SocialService {
    type Abi = social::SocialAbi;
}

impl Service for SocialService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = SocialState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        SocialService {
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
