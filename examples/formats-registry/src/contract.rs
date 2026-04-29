// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use formats_registry::{FormatsRegistryAbi, Operation};
use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};

use self::state::FormatsRegistryState;

linera_sdk::contract!(FormatsRegistryContract);

pub struct FormatsRegistryContract {
    state: FormatsRegistryState,
    runtime: ContractRuntime<Self>,
}

impl WithContractAbi for FormatsRegistryContract {
    type Abi = FormatsRegistryAbi;
}

impl Contract for FormatsRegistryContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = FormatsRegistryState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        FormatsRegistryContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: Operation) {
        match operation {
            Operation::Write { module_id, value } => {
                let existing = self
                    .state
                    .formats
                    .get(&module_id)
                    .await
                    .expect("storage");
                assert!(
                    existing.is_none(),
                    "formats are already registered for this module"
                );
                let blob_hash = self.runtime.create_data_blob(value);
                self.state
                    .formats
                    .insert(&module_id, blob_hash)
                    .expect("storage");
            }
        }
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("formats-registry does not support cross-chain messages");
    }

    async fn store(self) {
        self.state
            .save_and_drop()
            .await
            .expect("Failed to save state");
    }
}
