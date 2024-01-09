// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::MetaCounter;
use async_trait::async_trait;
use linera_sdk::{
    base::{ApplicationId, ChainId, SessionId, WithContractAbi},
    ApplicationCallOutcome, CalleeContext, Contract, ExecutionOutcome, MessageContext,
    OperationContext, SessionCallOutcome, SimpleStateStorage,
};
use thiserror::Error;

linera_sdk::contract!(MetaCounter);

impl MetaCounter {
    fn counter_id() -> Result<ApplicationId<counter::CounterAbi>, Error> {
        Self::parameters()
    }
}

impl WithContractAbi for MetaCounter {
    type Abi = meta_counter::MetaCounterAbi;
}

#[async_trait]
impl Contract for MetaCounter {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn initialize(
        &mut self,
        context: &OperationContext,
        _argument: (),
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        // Validate that the application parameters were configured correctly.
        assert!(Self::parameters().is_ok());

        Self::counter_id()?;
        // Send a no-op message to ourselves. This is only for testing contracts that send messages
        // on initialization. Since the value is 0 it does not change the counter value.
        Ok(ExecutionOutcome::default().with_message(context.chain_id, 0))
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        (recipient_id, value): (ChainId, u64),
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        log::trace!("message: {:?}", value);
        if value >= 20000 {
            Ok(ExecutionOutcome::default().with_tracked_message(recipient_id, value))
        } else {
            Ok(ExecutionOutcome::default().with_message(recipient_id, value))
        }
    }

    async fn execute_message(
        &mut self,
        context: &MessageContext,
        value: u64,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        if context.is_bouncing {
            log::trace!("receiving a bouncing message {value}");
            return Ok(ExecutionOutcome::default());
        }
        if value >= 10000 {
            log::trace!("failing message {value} on purpose");
            return Err(Error::ValueIsTooHigh);
        }
        log::trace!("executing {} via {:?}", value, Self::counter_id()?);
        self.call_application(true, Self::counter_id()?, &value, vec![])?;
        Ok(ExecutionOutcome::default())
    }

    async fn handle_application_call(
        &mut self,
        _context: &CalleeContext,
        _call: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<
        ApplicationCallOutcome<Self::Message, Self::Response, Self::SessionState>,
        Self::Error,
    > {
        Err(Error::CallsNotSupported)
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        _state: Self::SessionState,
        _call: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallOutcome<Self::Message, Self::Response, Self::SessionState>, Self::Error>
    {
        Err(Error::SessionsNotSupported)
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    #[error("MetaCounter application doesn't support any cross-chain messages")]
    MessagesNotSupported,

    #[error("MetaCounter application doesn't support any cross-application calls")]
    CallsNotSupported,

    #[error("MetaCounter application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    #[error("Message failed")]
    ValueIsTooHigh,

    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),
}
