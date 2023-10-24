// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use crate::state::{MatchingEngine, MatchingEngineError};
use async_graphql::{EmptySubscription, Request, Response, Schema};
use async_trait::async_trait;
use linera_sdk::{
    base::WithServiceAbi, graphql::GraphQLMutationRoot, QueryContext, Service, ViewStateStorage,
};
use matching_engine::Operation;
use std::sync::Arc;

linera_sdk::service!(MatchingEngine);

impl WithServiceAbi for MatchingEngine {
    type Abi = matching_engine::MatchingEngineAbi;
}

#[async_trait]
impl Service for MatchingEngine {
    type Error = MatchingEngineError;
    type Storage = ViewStateStorage<Self>;

    async fn handle_query(
        self: Arc<Self>,
        _context: &QueryContext,
        request: Request,
    ) -> Result<Response, Self::Error> {
        let schema =
            Schema::build(self.clone(), Operation::mutation_root(), EmptySubscription).finish();
        let response = schema.execute(request).await;
        Ok(response)
    }
}
