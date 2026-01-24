// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};
use time_expiry::{TimeExpiryAbi, TimeExpiryOperation};

use self::state::TimeExpiryState;

pub struct TimeExpiryContract {
    state: TimeExpiryState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(TimeExpiryContract);

impl WithContractAbi for TimeExpiryContract {
    type Abi = TimeExpiryAbi;
}

impl Contract for TimeExpiryContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = TimeExpiryState::load(runtime.root_view_storage_context())
            .expect("Failed to load state");
        TimeExpiryContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: TimeExpiryOperation) {
        let TimeExpiryOperation::ExpireAfter(delta) = operation;

        // Get the current block timestamp.
        let block_timestamp = self.runtime.system_time();

        // Calculate the expiry timestamp.
        let expiry_timestamp = block_timestamp.saturating_add(delta);

        // Assert that the validation happens before the expiry timestamp.
        self.runtime.assert_before(expiry_timestamp);
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("TimeExpiry application doesn't support any cross-chain messages");
    }

    async fn store(mut self) {
        self.state.save().expect("Failed to save state");
    }
}
