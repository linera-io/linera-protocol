// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use gol_challenge::Operation;
use linera_sdk::{
    graphql::GraphQLMutationRoot, linera_base_types::WithServiceAbi, views::View, Service,
    ServiceRuntime,
};

use self::state::GolChallengeState;

#[derive(Clone)]
pub struct GolChallengeService {
    runtime: Arc<ServiceRuntime<GolChallengeService>>,
    state: Arc<GolChallengeState>,
}

linera_sdk::service!(GolChallengeService);

impl WithServiceAbi for GolChallengeService {
    type Abi = gol_challenge::GolChallengeAbi;
}

impl Service for GolChallengeService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = GolChallengeState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        GolChallengeService {
            runtime: Arc::new(runtime),
            state: Arc::new(state),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            self.state.clone(),
            Operation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
        .data(self.runtime.clone())
        .finish();
        schema.execute(request).await
    }
}
