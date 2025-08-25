// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use counter_no_graphql::CounterRequest;
use create_and_call::{CreateAndCallOperation, CreateAndCallRequest};
use linera_sdk::{linera_base_types::WithServiceAbi, views::View, Service, ServiceRuntime};

use self::state::CreateAndCallState;

pub struct CreateAndCallService {
    state: CreateAndCallState,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(CreateAndCallService);

impl WithServiceAbi for CreateAndCallService {
    type Abi = create_and_call::CreateAndCallAbi;
}

impl Service for CreateAndCallService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = CreateAndCallState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        CreateAndCallService {
            state,
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: CreateAndCallRequest) -> u64 {
        match request {
            CreateAndCallRequest::Query => {
                let application_id = self.state.value.get().expect("An application_id");
                let counter_request = CounterRequest::Query;
                self.runtime
                    .query_application(application_id, &counter_request)
            }
            CreateAndCallRequest::CreateAndCall(bytecode, calldata, initial_value, increment) => {
                let operation = CreateAndCallOperation::CreateAndCall(
                    bytecode,
                    calldata,
                    initial_value,
                    increment,
                );
                self.runtime.schedule_operation(&operation);
                0
            }
        }
    }
}
