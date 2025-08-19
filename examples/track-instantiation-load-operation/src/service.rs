// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use linera_sdk::{linera_base_types::WithServiceAbi, views::View, Service, ServiceRuntime};
use track_instantiation_load_operation::{Query, Stats, TrackInstantiationLoadOperationAbi};

use self::state::TrackInstantiationLoadOperationState;

pub struct TrackInstantiationLoadOperationService {
    state: TrackInstantiationLoadOperationState,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(TrackInstantiationLoadOperationService);

impl WithServiceAbi for TrackInstantiationLoadOperationService {
    type Abi = TrackInstantiationLoadOperationAbi;
}

impl Service for TrackInstantiationLoadOperationService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = TrackInstantiationLoadOperationState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        TrackInstantiationLoadOperationService {
            state,
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, query: Query) -> Stats {
        match query {
            Query::GetStats => {
                self.state.stats.get().clone()
            },
            Query::ExecuteOperation => {
                self.runtime.schedule_operation(&());
                self.state.stats.get().clone()
            }
        }
    }
}
