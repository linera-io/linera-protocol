// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use linera_sdk::{
    abis::formats_registry::{FormatsRegistryAbi, Operation},
    linera_base_types::{ModuleId, WithServiceAbi},
    views::View,
    Service, ServiceRuntime,
};

use self::state::FormatsRegistryState;

linera_sdk::service!(FormatsRegistryService);

pub struct FormatsRegistryService {
    state: Arc<FormatsRegistryState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

impl WithServiceAbi for FormatsRegistryService {
    type Abi = FormatsRegistryAbi;
}

impl Service for FormatsRegistryService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = FormatsRegistryState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        FormatsRegistryService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            QueryRoot {
                state: self.state.clone(),
                runtime: self.runtime.clone(),
            },
            MutationRoot {
                runtime: self.runtime.clone(),
            },
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
}

struct QueryRoot {
    state: Arc<FormatsRegistryState>,
    runtime: Arc<ServiceRuntime<FormatsRegistryService>>,
}

#[Object]
impl QueryRoot {
    /// Returns the bytes registered for the given module, or `None` if the
    /// module has no entry yet.
    async fn get(&self, module_id: ModuleId) -> Option<Vec<u8>> {
        let blob_hash = self.state.formats.get(&module_id).await.unwrap()?;
        Some(self.runtime.read_data_blob(blob_hash))
    }
}

struct MutationRoot {
    runtime: Arc<ServiceRuntime<FormatsRegistryService>>,
}

#[Object]
impl MutationRoot {
    async fn write(&self, module_id: ModuleId, value: Vec<u8>) -> [u8; 0] {
        self.runtime
            .schedule_operation(&Operation::Write { module_id, value });
        []
    }
}
