// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use alloy_primitives::U256;
use alloy_sol_types::{sol, SolCall};
use call_evm_counter::{CallCounterOperation, CallCounterRequest};
use linera_sdk::{
    abis::evm::EvmAbi,
    linera_base_types::{ApplicationId, EvmQuery, WithServiceAbi},
    Service, ServiceRuntime,
};

pub struct CallCounterService {
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(CallCounterService);

impl WithServiceAbi for CallCounterService {
    type Abi = call_evm_counter::CallCounterAbi;
}

impl Service for CallCounterService {
    type Parameters = ApplicationId<EvmAbi>;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        CallCounterService {
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: CallCounterRequest) -> u64 {
        match request {
            CallCounterRequest::Query => {
                sol! {
                    function get_value();
                }
                let query = get_valueCall {};
                let query = query.abi_encode();
                let query = EvmQuery::Query(query);
                let evm_counter_id = self.runtime.application_parameters();
                let result = self.runtime.query_application(evm_counter_id, &query);
                let arr: [u8; 32] = result.try_into().expect("result should have length 32");
                U256::from_be_bytes(arr).to::<u64>()
            }
            CallCounterRequest::Increment(value) => {
                let operation = CallCounterOperation::Increment(value);
                self.runtime.schedule_operation(&operation);
                0
            }
        }
    }
}
