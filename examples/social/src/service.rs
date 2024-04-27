// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use linera_sdk::{base::WithServiceAbi, graphql::GraphQLMutationRoot, Service, ServiceRuntime};
use social::Operation;
use state::Social;

pub struct SocialService {
    state: Arc<Social>,
}

linera_sdk::service!(SocialService);

impl WithServiceAbi for SocialService {
    type Abi = social::SocialAbi;
}

impl Service for SocialService {
    type State = Social;
    type Parameters = ();

    async fn new(state: Self::State, _runtime: ServiceRuntime<Self>) -> Self {
        SocialService {
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
