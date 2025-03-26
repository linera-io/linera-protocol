// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use linera_sdk::{
    linera_base_types::{ApplicationId, StreamName, WithContractAbi},
    Contract, ContractRuntime, Resources,
};
use meta_counter::{Message, MetaCounterAbi, Operation};

pub struct MetaCounterContract {
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(MetaCounterContract);

impl MetaCounterContract {
    fn counter_id(&mut self) -> ApplicationId<counter::CounterAbi> {
        self.runtime.application_parameters()
    }
}

impl WithContractAbi for MetaCounterContract {
    type Abi = MetaCounterAbi;
}

impl Contract for MetaCounterContract {
    type Message = Message;
    type InstantiationArgument = ();
    type Parameters = ApplicationId<counter::CounterAbi>;

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        MetaCounterContract { runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        // Validate that the application parameters were configured correctly.
        self.counter_id();
        // Send a no-op message to ourselves. This is only for testing contracts that send messages
        // on initialization. Since the value is 0 it does not change the counter value.
        let this_chain = self.runtime.chain_id();
        self.runtime
            .emit(StreamName(b"announcements".to_vec()), 0, b"instantiated");
        self.runtime.send_message(this_chain, Message::Increment(0));
    }

    async fn execute_operation(&mut self, operation: Operation) {
        log::trace!("operation: {:?}", operation);
        let Operation {
            recipient_id,
            authenticated,
            is_tracked,
            query_service,
            fuel_grant,
            message,
        } = operation;

        let mut message = self.runtime.prepare_message(message).with_grant(Resources {
            fuel: fuel_grant,
            ..Resources::default()
        });
        if authenticated {
            message = message.with_authentication();
        }
        if is_tracked {
            message = message.with_tracking();
        }
        if query_service {
            // Make a service query: The result will be logged in the executed block.
            let counter_id = self.counter_id();
            let _ = self
                .runtime
                .query_service(counter_id, "query { value }".into());
        }
        message.send_to(recipient_id);
    }

    async fn execute_message(&mut self, message: Message) {
        let is_bouncing = self
            .runtime
            .message_is_bouncing()
            .expect("Message delivery status has to be available when executing a message");
        if is_bouncing {
            log::trace!("receiving a bouncing message {message:?}");
            return;
        }
        match message {
            Message::Fail => {
                panic!("Message failed intentionally");
            }
            Message::Increment(value) => {
                let counter_id = self.counter_id();
                log::trace!("executing {} via {:?}", value, counter_id);
                self.runtime.call_application(true, counter_id, &value);
            }
        }
    }

    async fn store(self) {}
}
