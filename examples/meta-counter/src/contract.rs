// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::MetaCounter;
use async_trait::async_trait;
use linera_sdk::{
    base::{ApplicationId, ChainId, SessionId, WithContractAbi},
    ApplicationCallResult, CalleeContext, Contract, ExecutionResult, MessageContext,
    OperationContext, SessionCallResult, SimpleStateStorage,
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
        _context: &OperationContext,
        _argument: (),
    ) -> Result<ExecutionResult<Self::Message>, Self::Error> {
        Self::counter_id()?;
        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        (recipient_id, operation): (ChainId, u64),
    ) -> Result<ExecutionResult<Self::Message>, Self::Error> {
        log::trace!("message: {:?}", operation);
        Ok(ExecutionResult::default().with_message(recipient_id, operation))
    }

    async fn execute_message(
        &mut self,
        _context: &MessageContext,
        message: u64,
    ) -> Result<ExecutionResult<Self::Message>, Self::Error> {
        log::trace!("executing {:?} via {:?}", message, Self::counter_id()?);
        self.call_application(true, Self::counter_id()?, &message, vec![])
            .await?;
        Ok(ExecutionResult::default())
    }

    async fn handle_application_call(
        &mut self,
        _context: &CalleeContext,
        _call: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult<Self::Message, Self::Response, Self::SessionState>, Self::Error>
    {
        Err(Error::CallsNotSupported)
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        _state: Self::SessionState,
        _call: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult<Self::Message, Self::Response, Self::SessionState>, Self::Error>
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

    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),
}
