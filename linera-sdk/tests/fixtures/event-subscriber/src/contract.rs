// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use event_emitter::EventEmitterAbi;
use event_subscriber::{EventSubscriberAbi, Operation};
use linera_sdk::{
    linera_base_types::{GenericApplicationId, StreamUpdate, WithContractAbi},
    views::{RootView, View},
    Contract, ContractRuntime,
};
use state::EventSubscriberState;

pub struct EventSubscriberContract {
    state: EventSubscriberState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(EventSubscriberContract);

impl WithContractAbi for EventSubscriberContract {
    type Abi = EventSubscriberAbi;
}

impl Contract for EventSubscriberContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = EventSubscriberState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        EventSubscriberContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {}

    async fn execute_operation(&mut self, operation: Operation) -> Self::Response {
        match operation {
            Operation::Subscribe {
                chain_id,
                application_id,
                stream_name,
            } => {
                self.runtime.subscribe_to_events(
                    chain_id,
                    application_id,
                    stream_name.as_bytes().to_vec().into(),
                );
            }
            Operation::Unsubscribe {
                chain_id,
                application_id,
                stream_name,
            } => {
                self.runtime.unsubscribe_from_events(
                    chain_id,
                    application_id,
                    stream_name.as_bytes().to_vec().into(),
                );
            }
        }
    }

    async fn execute_message(&mut self, _message: ()) {}

    async fn process_streams(&mut self, updates: Vec<StreamUpdate>) {
        for update in updates {
            let GenericApplicationId::User(app_id) = update.stream_id.application_id else {
                continue;
            };
            let emitter_app_id = app_id.with_abi::<EventEmitterAbi>();
            let stream_name =
                String::from_utf8(update.stream_id.stream_name.0.clone()).unwrap_or_default();
            for index in update.new_indices() {
                let response = self.runtime.call_application(
                    true,
                    emitter_app_id,
                    &event_emitter::Operation::ReadEvent {
                        chain_id: update.chain_id,
                        stream_name: stream_name.clone(),
                        index,
                    },
                );
                if let Some(value) = response {
                    self.state.received_events.push(value);
                }
            }
        }
    }

    async fn store(self) {
        self.state
            .save_and_drop()
            .await
            .expect("Failed to save state");
    }
}
