// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use linera_sdk::{
    base::{ApplicationId, WithContractAbi},
    Contract, ContractRuntime, EmptyState, Resources,
};
use meta_counter::{Message, MetaCounterAbi, Operation};
use thiserror::Error;

pub struct MetaCounterContract {
    state: EmptyState,
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
    type Error = Error;
    type State = EmptyState;
    type Message = Message;
    type InstantiationArgument = ();
    type Parameters = ApplicationId<counter::CounterAbi>;

    async fn new(state: Self::State, runtime: ContractRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(MetaCounterContract { state, runtime })
    }

    async fn instantiate(&mut self, _argument: ()) -> Result<(), Self::Error> {
        // Validate that the application parameters were configured correctly.
        self.counter_id();
        // Send a no-op message to ourselves. This is only for testing contracts that send messages
        // on initialization. Since the value is 0 it does not change the counter value.
        let this_chain = self.runtime.chain_id();
        self.runtime.send_message(this_chain, Message::Increment(0));
        Ok(())
    }

    async fn execute_operation(&mut self, operation: Operation) -> Result<(), Self::Error> {
        log::trace!("operation: {:?}", operation);
        let Operation {
            recipient_id,
            authenticated,
            is_tracked,
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
        message.send_to(recipient_id);

        Ok(())
    }

    async fn execute_message(&mut self, message: Message) -> Result<(), Self::Error> {
        let is_bouncing = self
            .runtime
            .message_is_bouncing()
            .expect("Message delivery status has to be available when executing a message");
        if is_bouncing {
            log::trace!("receiving a bouncing message {message:?}");
            return Ok(());
        }
        match message {
            Message::Fail => {
                log::trace!("failing message {message:?} on purpose");
                Err(Error::MessageFailed)
            }
            Message::Increment(value) => {
                let counter_id = self.counter_id();
                log::trace!("executing {} via {:?}", value, counter_id);
                self.runtime.call_application(true, counter_id, &value);
                Ok(())
            }
        }
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    #[error("MetaCounter application doesn't support any cross-chain messages")]
    MessagesNotSupported,

    #[error("Message failed intentionally")]
    MessageFailed,

    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),
}
