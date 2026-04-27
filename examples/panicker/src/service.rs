// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use linera_sdk::{linera_base_types::WithServiceAbi, views::View, Service, ServiceRuntime};
use panicker::{PanickerOperation, PanickerRequest};

use self::state::PanickerState;

pub struct PanickerService {
    _state: PanickerState,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(PanickerService);

impl WithServiceAbi for PanickerService {
    type Abi = panicker::PanickerAbi;
}

impl Service for PanickerService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = PanickerState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        PanickerService {
            _state: state,
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, _request: PanickerRequest) {
        self.runtime.schedule_operation(&PanickerOperation);
    }
}
