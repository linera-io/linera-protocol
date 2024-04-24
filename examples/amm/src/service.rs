// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use amm::{AmmError, Operation, Parameters};
use async_graphql::{EmptySubscription, Request, Response, Schema};
use linera_sdk::{base::WithServiceAbi, graphql::GraphQLMutationRoot, Service, ServiceRuntime};

use self::state::Amm;

pub struct AmmService {
    state: Arc<Amm>,
}

linera_sdk::service!(AmmService);

impl WithServiceAbi for AmmService {
    type Abi = amm::AmmAbi;
}

impl Service for AmmService {
    type Error = AmmError;
    type State = Amm;
    type Parameters = Parameters;

    async fn new(state: Self::State, _runtime: ServiceRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(AmmService {
            state: Arc::new(state),
        })
    }

    async fn handle_query(&self, request: Request) -> Result<Response, AmmError> {
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
