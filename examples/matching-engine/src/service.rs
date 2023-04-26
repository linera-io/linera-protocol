// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use crate::state::{MatchingEngine, MatchingEngineError};
use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use async_trait::async_trait;
use linera_sdk::{base::WithServiceAbi, QueryContext, Service, ViewStateStorage};
use matching_engine::{Operation, Order};
use std::sync::Arc;

linera_sdk::service!(MatchingEngine);

impl WithServiceAbi for MatchingEngine {
    type Abi = matching_engine::MatchingEngineAbi;
}

#[async_trait]
impl Service for MatchingEngine {
    type Error = MatchingEngineError;
    type Storage = ViewStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        request: Request,
    ) -> Result<Response, Self::Error> {
        let schema = Schema::build(self.clone(), MutationRoot, EmptySubscription).finish();
        let response = schema.execute(request).await;
        Ok(response)
    }
}

struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn order(&self, order: Order) -> Vec<u8> {
        bcs::to_bytes(&Operation::ExecuteOrder { order }).unwrap()
    }
}
