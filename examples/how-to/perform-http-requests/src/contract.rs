// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use how_to_perform_http_requests::Abi;
use linera_sdk::{linera_base_types::WithContractAbi, Contract as _, ContractRuntime};

pub struct Contract {
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(Contract);

impl WithContractAbi for Contract {
    type Abi = Abi;
}

impl linera_sdk::Contract for Contract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = String;

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        Contract { runtime }
    }

    async fn instantiate(&mut self, (): Self::InstantiationArgument) {
        // Check that the global parameters can be deserialized correctly.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, _operation: Self::Operation) -> Self::Response {}

    async fn execute_message(&mut self, (): Self::Message) {
        panic!("This application doesn't support any cross-chain messages");
    }

    async fn store(self) {}
}
