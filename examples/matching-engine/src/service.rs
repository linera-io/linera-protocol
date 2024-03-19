// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use crate::state::{MatchingEngine, MatchingEngineError};
use async_graphql::{EmptySubscription, Request, Response, Schema};
use async_trait::async_trait;
use linera_sdk::{
    base::WithServiceAbi, graphql::GraphQLMutationRoot, Service, ServiceRuntime, ViewStateStorage,
};
use matching_engine::Operation;
use std::sync::Arc;

pub struct MatchingEngineService {
    state: Arc<MatchingEngine>,
}

linera_sdk::service!(MatchingEngineService);

impl WithServiceAbi for MatchingEngineService {
    type Abi = matching_engine::MatchingEngineAbi;
}

#[async_trait]
impl Service for MatchingEngineService {
    type Error = MatchingEngineError;
    type Storage = ViewStateStorage<Self>;
    type State = MatchingEngine;

    async fn new(state: Self::State) -> Result<Self, Self::Error> {
        Ok(MatchingEngineService {
            state: Arc::new(state),
        })
    }

    async fn handle_query(
        &self,
        _runtime: &ServiceRuntime,
        request: Request,
    ) -> Result<Response, Self::Error> {
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
