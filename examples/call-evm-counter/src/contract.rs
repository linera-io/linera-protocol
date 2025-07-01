// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use alloy_primitives::{address, U256};
use alloy_sol_types::{sol, SolCall};
use call_evm_counter::{CallCounterAbi, CallCounterOperation};
use linera_sdk::{
    abis::evm::EvmAbi,
    linera_base_types::{ApplicationId, WithContractAbi},
    Contract, ContractRuntime,
};

pub struct CallCounterContract {
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(CallCounterContract);

impl WithContractAbi for CallCounterContract {
    type Abi = CallCounterAbi;
}

impl CallCounterContract {
    fn process_operation(&mut self, operation: Vec<u8>) -> u64 {
        let evm_counter_id = self.runtime.application_parameters();
        let result = self
            .runtime
            .call_application(true, evm_counter_id, &operation);
        let arr: [u8; 32] = result.try_into().expect("result should have length 32");
        U256::from_be_bytes(arr).to::<u64>()
    }
}

impl Contract for CallCounterContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ApplicationId<EvmAbi>;
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        CallCounterContract { runtime }
    }

    async fn instantiate(&mut self, _value: ()) {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: CallCounterOperation) -> u64 {
        sol! {
            function increment(uint64 input);
            function call_from_wasm(address remote_address);
        }
        match operation {
            CallCounterOperation::Increment(increment) => {
                let operation = incrementCall { input: increment };
                let operation = operation.abi_encode();
                self.process_operation(operation)
            }
            CallCounterOperation::TestCallAddress => {
                let remote_address = address!("0000000000000000000000000000000000000000");
                let operation = call_from_wasmCall { remote_address };
                let operation = operation.abi_encode();
                self.process_operation(operation)
            }
        }
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Counter application doesn't support any cross-chain messages");
    }

    async fn store(self) {}
}
