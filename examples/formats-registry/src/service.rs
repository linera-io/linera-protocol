// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{ComplexObject, Context, EmptySubscription, Schema};
use formats_registry::{FormatsRegistryAbi, Operation};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
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

    async fn handle_query(&self, query: Self::Query) -> Self::QueryResponse {
        Schema::build(
            self.state.clone(),
            Operation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
        .data(self.runtime.clone())
        .finish()
        .execute(query)
        .await
    }
}

#[ComplexObject]
impl FormatsRegistryState {
    /// Returns the bytes registered for the given module, or `None` if the module
    /// has no entry yet. Reads the registered hash from state and fetches the
    /// corresponding data blob.
    async fn read(&self, ctx: &Context<'_>, module_id: ModuleId) -> Option<Vec<u8>> {
        let blob_hash = self.formats.get(&module_id).await.expect("storage")?;
        let runtime = ctx
            .data::<Arc<ServiceRuntime<FormatsRegistryService>>>()
            .expect("the runtime is available in the GraphQL context");
        Some(runtime.read_data_blob(blob_hash))
    }
}
