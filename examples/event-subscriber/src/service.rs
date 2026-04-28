// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Response, Schema};
use event_subscriber::Operation;
use linera_sdk::{
    graphql::GraphQLMutationRoot as _, linera_base_types::WithServiceAbi, views::View, Service,
    ServiceRuntime,
};
use state::EventSubscriberState;

pub struct EventSubscriberService {
    state: Arc<EventSubscriberState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(EventSubscriberService);

impl WithServiceAbi for EventSubscriberService {
    type Abi = event_subscriber::EventSubscriberAbi;
}

impl Service for EventSubscriberService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = EventSubscriberState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        EventSubscriberService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            self.state.clone(),
            Operation::mutation_root(self.runtime.clone()),
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
}
