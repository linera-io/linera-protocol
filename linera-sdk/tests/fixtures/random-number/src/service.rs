// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{
    linera_base_types::WithServiceAbi,
    views::View,
    Service, ServiceRuntime,
};
use random_number::{Query, QueryResponse, RandomNumberAbi};

use self::state::RandomNumberState;

pub struct RandomNumberService {
    state: RandomNumberState,
    runtime: ServiceRuntime<Self>,
}

linera_sdk::service!(RandomNumberService);

impl WithServiceAbi for RandomNumberService {
    type Abi = RandomNumberAbi;
}

impl Service for RandomNumberService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = RandomNumberState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        RandomNumberService { state, runtime }
    }

    async fn handle_query(&self, query: Query) -> QueryResponse {
        match query {
            Query::GetSeeds => QueryResponse::Seeds {
                seed1: *self.state.seed1.get(),
                seed2: *self.state.seed2.get(),
            },
            Query::ServiceRandom => {
                let seed1 = self.runtime.random_number();
                let seed2 = self.runtime.random_number();
                QueryResponse::ServiceSeeds { seed1, seed2 }
            }
        }
    }
}
