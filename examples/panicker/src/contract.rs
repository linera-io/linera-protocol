// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};
use panicker::{PanickerAbi, PanickerOperation};

use self::state::PanickerState;

pub struct PanickerContract {
    state: PanickerState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(PanickerContract);

impl WithContractAbi for PanickerContract {
    type Abi = PanickerAbi;
}

impl Contract for PanickerContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = PanickerState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        PanickerContract { state, runtime }
    }

    async fn instantiate(&mut self, (): ()) {
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, _operation: PanickerOperation) {
        panic!("Panicker contract panicked on purpose");
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Panicker application doesn't support cross-chain messages");
    }

    async fn store(self) {
        self.state
            .save_and_drop()
            .await
            .expect("Failed to save state");
    }
}
