// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use track_instantiation_load_operation::TrackInstantiationLoadOperationAbi;
use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};

use self::state::TrackInstantiationLoadOperationState;

pub struct TrackInstantiationLoadOperationContract {
    state: TrackInstantiationLoadOperationState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(TrackInstantiationLoadOperationContract);

impl WithContractAbi for TrackInstantiationLoadOperationContract {
    type Abi = TrackInstantiationLoadOperationAbi;
}

impl Contract for TrackInstantiationLoadOperationContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = TrackInstantiationLoadOperationState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");

        TrackInstantiationLoadOperationContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        self.runtime.application_parameters();

        // Send message to creator chain about instantiation
        let creator_chain = self.runtime.application_creator_chain_id();
        self.runtime
            .prepare_message(())
            .with_authentication()
            .send_to(creator_chain);
    }

    async fn execute_operation(&mut self, _operation: ()) -> () {
        panic!("No operation being executed");
    }

    async fn execute_message(&mut self, _message: ()) {
        let count = self.state.stats.get_mut();
        *count += 1;
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}
