// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{linera_base_types::WithServiceAbi, views::View, Service, ServiceRuntime};
use track_instantiation::{Query, TrackInstantiationAbi};

use self::state::TrackInstantiationState;

pub struct TrackInstantiationService {
    state: TrackInstantiationState,
}

linera_sdk::service!(TrackInstantiationService);

impl WithServiceAbi for TrackInstantiationService {
    type Abi = TrackInstantiationAbi;
}

impl Service for TrackInstantiationService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = TrackInstantiationState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        TrackInstantiationService { state }
    }

    async fn handle_query(&self, query: Query) -> u64 {
        match query {
            Query::GetCount => *self.state.stats.get(),
        }
    }
}
