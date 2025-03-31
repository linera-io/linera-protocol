// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use alloy::primitives::U256;
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

impl Contract for CallCounterContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ApplicationId<EvmAbi>;

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        CallCounterContract { runtime }
    }

    async fn instantiate(&mut self, _value: ()) {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: CallCounterOperation) -> u64 {
        let CallCounterOperation::Increment(increment) = operation;
        sol! {
            function increment(uint64 input);
        }
        let operation = incrementCall { input: increment };
        let operation = operation.abi_encode();
        let evm_counter_id = self.runtime.application_parameters();
        let result = self
            .runtime
            .call_application(true, evm_counter_id, &operation);
        let arr: [u8; 32] = result.try_into().expect("result should have length 32");
        U256::from_be_bytes(arr).to::<u64>()
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Counter application doesn't support any cross-chain messages");
    }

    async fn store(self) {}
}
