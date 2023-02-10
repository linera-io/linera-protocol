// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::SimpleMetaCounter;
use async_trait::async_trait;
use linera_sdk::{
    contract::system_api, ApplicationCallResult, CalleeContext, Contract, EffectContext,
    ExecutionResult, OperationContext, Session, SessionCallResult, SessionId, SimpleStateStorage,
};
use thiserror::Error;

linera_sdk::contract!(SimpleMetaCounter);

#[async_trait]
impl Contract for SimpleMetaCounter {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        argument: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        self.counter_id = Some(bcs::from_bytes(argument).map_err(|_| Error::Initialization)?);
        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        operation: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        system_api::call_application(true, self.counter_id.unwrap(), operation, vec![])
            .await
            .map_err(|_| Error::InternalCall)?;
        Ok(ExecutionResult::default())
    }

    async fn execute_effect(
        &mut self,
        _context: &EffectContext,
        _effect: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        Err(Error::EffectsNotSupported)
    }

    async fn call_application(
        &mut self,
        _context: &CalleeContext,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error> {
        Err(Error::CallsNotSupported)
    }

    async fn call_session(
        &mut self,
        _context: &CalleeContext,
        _session: Session,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, Self::Error> {
        Err(Error::SessionsNotSupported)
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    #[error("SimpleMetaCounter application doesn't support any cross-chain effects")]
    EffectsNotSupported,

    #[error("SimpleMetaCounter application doesn't support any cross-application calls")]
    CallsNotSupported,

    #[error("SimpleMetaCounter application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    #[error("Error with the internal call to Counter")]
    InternalCall,

    #[error("Error with the initialization")]
    Initialization,
}
