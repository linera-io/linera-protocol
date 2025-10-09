// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use complex_data_contract::{ComplexDataAbi, ComplexDataOperation};
use linera_sdk::{linera_base_types::WithServiceAbi, views::View, Service, ServiceRuntime};
use linera_sdk::graphql::{GraphQLMutationRoot as _};



use self::state::ComplexDataState;

pub struct ComplexDataService {
    state: Arc<ComplexDataState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(ComplexDataService);

impl WithServiceAbi for ComplexDataService {
    type Abi = ComplexDataAbi;
}

impl Service for ComplexDataService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = ComplexDataState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        ComplexDataService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            self.state.clone(),
            ComplexDataOperation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
            .finish();
        schema.execute(request).await
    }
}
