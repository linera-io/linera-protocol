// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use alloy_primitives::{address, U256};
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

impl CallCounterService {
    fn process_query(&self, query: Vec<u8>) -> u64 {
        let query = EvmQuery::Query(query);
        let evm_counter_id = self.runtime.application_parameters();
        let result = self.runtime.query_application(evm_counter_id, &query);
        let arr: [u8; 32] = result.try_into().expect("result should have length 32");
        U256::from_be_bytes(arr).to::<u64>()
    }
}

impl Service for CallCounterService {
    type Parameters = ApplicationId<EvmAbi>;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        CallCounterService {
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: CallCounterRequest) -> u64 {
        sol! {
            function get_value();
            function call_from_wasm(address remote_address);
        }
        match request {
            CallCounterRequest::Query => {
                let query = get_valueCall {};
                let query = query.abi_encode();
                self.process_query(query)
            }
            CallCounterRequest::TestCallAddress => {
                let remote_address = address!("0000000000000000000000000000000000002000");
                let call_from_wasm = call_from_wasmCall { remote_address };
                let call_from_wasm = call_from_wasm.abi_encode();
                self.process_query(call_from_wasm)
            }
            CallCounterRequest::Increment(value) => {
                let operation = CallCounterOperation::Increment(value);
                self.runtime.schedule_operation(&operation);
                0
            }
            CallCounterRequest::ContractTestCallAddress => {
                let operation = CallCounterOperation::TestCallAddress;
                self.runtime.schedule_operation(&operation);
                0
            }
        }
    }
}
