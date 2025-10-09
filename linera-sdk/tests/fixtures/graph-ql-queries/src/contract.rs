// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use complex_data_contract::{ComplexDataAbi, ComplexDataOperation};
use complex_data_contract::ComplexDataOperation::*;
use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};

use self::state::ComplexDataState;

pub struct ComplexDataContract {
    state: ComplexDataState,
}

linera_sdk::contract!(ComplexDataContract);

impl WithContractAbi for ComplexDataContract {
    type Abi = ComplexDataAbi;
}

impl Contract for ComplexDataContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = ComplexDataState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        ComplexDataContract { state }
    }

    async fn instantiate(&mut self, _value: ()) {
    }

    async fn execute_operation(&mut self, operation: ComplexDataOperation) {
        match operation {
            InsertField4 { key1, key2, value } => {
                let subview = self.state.field4.load_entry_mut(&key1).await.unwrap();
                subview.insert(&key2, value).unwrap();
            },
        }
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Counter application doesn't support any cross-chain messages");
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}
