// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use cost_tracking::{CostTrackingAbi, Query, QueryResponse};
use linera_sdk::{linera_base_types::WithServiceAbi, views::View, Service, ServiceRuntime};

use self::state::CostTrackingState;

pub struct CostTrackingService {
    state: CostTrackingState,
}

linera_sdk::service!(CostTrackingService);

impl WithServiceAbi for CostTrackingService {
    type Abi = CostTrackingAbi;
}

impl Service for CostTrackingService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = CostTrackingState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        CostTrackingService { state }
    }

    async fn handle_query(&self, query: Query) -> QueryResponse {
        match query {
            Query::GetLogs => {
                let count = self.state.logs.count();
                let mut logs = Vec::with_capacity(count);
                for i in 0..count {
                    if let Some(entry) = self.state.logs.get(i).await.expect("Failed to get log") {
                        logs.push(entry);
                    }
                }
                QueryResponse::Logs(logs)
            }
            Query::GetLogCount => QueryResponse::LogCount(self.state.logs.count() as u64),
        }
    }
}
