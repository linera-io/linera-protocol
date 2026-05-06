// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use event_emitter::{EventEmitterAbi, Operation};
use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};
use state::EventEmitterState;

pub struct EventEmitterContract {
    state: EventEmitterState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(EventEmitterContract);

impl WithContractAbi for EventEmitterContract {
    type Abi = EventEmitterAbi;
}

impl Contract for EventEmitterContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = String;

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = EventEmitterState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        EventEmitterContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {}

    async fn execute_operation(&mut self, operation: Operation) -> Self::Response {
        match operation {
            Operation::Emit { stream_name, value } => {
                self.state.emitted_events.push(value.clone());
                self.runtime
                    .emit(stream_name.as_bytes().to_vec().into(), &value);
                None
            }
            Operation::ReadEvent {
                chain_id,
                stream_name,
                index,
            } => {
                let value: String = self.runtime.read_event(
                    chain_id,
                    stream_name.as_bytes().to_vec().into(),
                    index,
                );
                Some(value)
            }
        }
    }

    async fn execute_message(&mut self, _message: ()) {}

    async fn store(self) {
        self.state
            .save_and_drop()
            .await
            .expect("Failed to save state");
    }
}
