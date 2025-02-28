// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use async_graphql::{
    connection::EmptyFields, EmptyMutation, EmptySubscription, Request, Response, Schema,
};
use how_to_perform_http_requests::Abi;
use linera_sdk::{linera_base_types::WithServiceAbi, Service as _, ServiceRuntime};

pub struct Service {
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(Service);

impl WithServiceAbi for Service {
    type Abi = Abi;
}

impl linera_sdk::Service for Service {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        Service {
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(EmptyFields, EmptyMutation, EmptySubscription).finish();
        schema.execute(request).await
    }
}
