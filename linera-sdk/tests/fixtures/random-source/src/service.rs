// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{linera_base_types::WithServiceAbi, views::View, Service, ServiceRuntime};
use random_source::{Query, QueryResponse, RandomSourceAbi};

use self::state::RandomSourceState;

pub struct RandomSourceService {
    state: RandomSourceState,
}

linera_sdk::service!(RandomSourceService);

impl WithServiceAbi for RandomSourceService {
    type Abi = RandomSourceAbi;
}

impl Service for RandomSourceService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = RandomSourceState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        RandomSourceService { state }
    }

    async fn handle_query(&self, query: Query) -> QueryResponse {
        match query {
            Query::GetSamples => QueryResponse::Samples {
                seed: *self.state.seed.get(),
                sample1: *self.state.sample1.get(),
                sample2: *self.state.sample2.get(),
            },
        }
    }
}
