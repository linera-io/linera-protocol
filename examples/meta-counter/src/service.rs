// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use async_graphql::{Request, Response};
use linera_sdk::{
    base::{ApplicationId, WithServiceAbi},
    Service, ServiceRuntime,
};

pub struct MetaCounterService {
    runtime: ServiceRuntime<Self>,
}

linera_sdk::service!(MetaCounterService);

impl WithServiceAbi for MetaCounterService {
    type Abi = meta_counter::MetaCounterAbi;
}

impl Service for MetaCounterService {
    type Parameters = ApplicationId<counter::CounterAbi>;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        MetaCounterService { runtime }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let counter_id = self.runtime.application_parameters();
        self.runtime.query_application(counter_id, &request)
    }
}
