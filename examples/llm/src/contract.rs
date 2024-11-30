// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use linera_sdk::{base::WithContractAbi, Contract, ContractRuntime};

pub struct LlmContract;

linera_sdk::contract!(LlmContract);

impl WithContractAbi for LlmContract {
    type Abi = llm::LlmAbi;
}

impl Contract for LlmContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();

    async fn load(_runtime: ContractRuntime<Self>) -> Self {
        LlmContract
    }

    async fn instantiate(&mut self, _value: ()) {}

    async fn execute_operation(&mut self, _operation: ()) -> Self::Response {}

    async fn execute_message(&mut self, _message: ()) {
        panic!("Llm application doesn't support any cross-chain messages");
    }

    async fn store(self) {}
}
