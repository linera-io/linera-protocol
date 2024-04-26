// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use async_graphql::{Request, Response};
use linera_sdk::{
    base::{ApplicationId, WithServiceAbi},
    EmptyState, Service, ServiceRuntime,
};
use thiserror::Error;

pub struct MetaCounterService {
    runtime: ServiceRuntime<Self>,
}

linera_sdk::service!(MetaCounterService);

impl WithServiceAbi for MetaCounterService {
    type Abi = meta_counter::MetaCounterAbi;
}

impl Service for MetaCounterService {
    type Error = Error;
    type State = EmptyState;
    type Parameters = ApplicationId<counter::CounterAbi>;

    async fn new(_state: Self::State, runtime: ServiceRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(MetaCounterService { runtime })
    }

    async fn handle_query(&self, request: Request) -> Result<Response, Self::Error> {
        let counter_id = self.runtime.application_parameters();
        Ok(self.runtime.query_application(counter_id, &request))
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {}
