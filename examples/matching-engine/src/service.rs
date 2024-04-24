// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use linera_sdk::{base::WithServiceAbi, graphql::GraphQLMutationRoot, Service, ServiceRuntime};
use matching_engine::{Operation, Parameters};

use crate::state::{MatchingEngine, MatchingEngineError};

pub struct MatchingEngineService {
    state: Arc<MatchingEngine>,
}

linera_sdk::service!(MatchingEngineService);

impl WithServiceAbi for MatchingEngineService {
    type Abi = matching_engine::MatchingEngineAbi;
}

impl Service for MatchingEngineService {
    type Error = MatchingEngineError;
    type State = MatchingEngine;
    type Parameters = Parameters;

    async fn new(state: Self::State, _runtime: ServiceRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(MatchingEngineService {
            state: Arc::new(state),
        })
    }

    async fn handle_query(&self, request: Request) -> Result<Response, Self::Error> {
        let schema = Schema::build(
            self.state.clone(),
            Operation::mutation_root(),
            EmptySubscription,
        )
        .finish();
        let response = schema.execute(request).await;
        Ok(response)
    }
}
