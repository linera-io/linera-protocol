// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use track_instantiation_load_operation::{MessageContent, Stats, TrackInstantiationLoadOperationAbi};
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
    type Message = MessageContent;
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(mut runtime: ContractRuntime<Self>) -> Self {
        let state = TrackInstantiationLoadOperationState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");

        // Send message to creator chain about load operation
        let creator_chain = runtime.application_creator_chain_id();
        if creator_chain != runtime.chain_id() {
            runtime
                .prepare_message(MessageContent::IncrementLoad)
                .with_authentication()
                .send_to(creator_chain);
        }

        TrackInstantiationLoadOperationContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        self.runtime.application_parameters();

        // Send message to creator chain about instantiation
        let creator_chain = self.runtime.application_creator_chain_id();
        self.runtime
            .prepare_message(MessageContent::IncrementInstantiation)
            .with_authentication()
            .send_to(creator_chain);
    }

    async fn execute_operation(&mut self, _operation: ()) -> () {
        let mut current_stats = self.state.stats.get().clone();
        current_stats.execute_operation_count += 1;
        self.state.stats.set(current_stats);

        // Send message to creator chain about execute_operation
        let creator_chain = self.runtime.application_creator_chain_id();
        if creator_chain != self.runtime.chain_id() {
            self.runtime
                .prepare_message(MessageContent::IncrementExecuteOperation)
                .with_authentication()
                .send_to(creator_chain);
        }
    }

    async fn execute_message(&mut self, message: MessageContent) {
        let mut current_stats = self.state.stats.get().clone();

        match message {
            MessageContent::IncrementInstantiation => {
                current_stats.instantiation_count += 1;
            }
            MessageContent::IncrementLoad => {
                current_stats.load_count += 1;
            }
            MessageContent::IncrementExecuteOperation => {
                current_stats.execute_operation_count += 1;
            }
        }

        self.state.stats.set(current_stats);
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}
