// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::MetaCounter;
use async_trait::async_trait;
use linera_sdk::{
    contract::system_api, ensure, ApplicationCallResult, ApplicationId, CalleeContext, ChainId,
    Contract, EffectContext, ExecutionResult, OperationContext, Session, SessionCallResult,
    SessionId, SimpleStateStorage,
};
use thiserror::Error;

linera_sdk::contract!(MetaCounter);

impl MetaCounter {
    fn counter_id() -> Result<ApplicationId, Error> {
        let parameters = system_api::current_application_parameters();
        bcs::from_bytes(&parameters).map_err(|_| Error::Parameters)
    }
}

#[async_trait]
impl Contract for MetaCounter {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        argument: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        ensure!(argument.is_empty(), Error::Initialization);
        Self::counter_id()?;
        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        operation: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        let (recipient_id, operation) =
            bcs::from_bytes::<(ChainId, u128)>(operation).map_err(|_| Error::Operation)?;
        log::trace!("effect: {:?}", operation);
        Ok(ExecutionResult::default().with_effect(recipient_id, &operation))
    }

    async fn execute_effect(
        &mut self,
        _context: &EffectContext,
        effect: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        log::trace!("executing {:?} via {:?}", effect, Self::counter_id()?);
        system_api::call_application(true, Self::counter_id()?, effect, vec![])
            .await
            .map_err(Error::InternalCall)?;
        Ok(ExecutionResult::default())
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
    #[error("MetaCounter application doesn't support any cross-chain effects")]
    EffectsNotSupported,

    #[error("MetaCounter application doesn't support any cross-application calls")]
    CallsNotSupported,

    #[error("MetaCounter application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    #[error("Error with the internal call to Counter: {0}")]
    InternalCall(String),

    #[error("Error during the initialization")]
    Initialization,

    #[error("Invalid application parameters")]
    Parameters,

    #[error("Error with the operation")]
    Operation,
}
