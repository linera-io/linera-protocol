// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::MetaCounter;
use async_trait::async_trait;
use linera_sdk::{
    base::{ApplicationId, ChainId, SessionId},
    contract::system_api,
    ApplicationCallResult, CalleeContext, Contract, EffectContext, ExecutionResult,
    OperationContext, Session, SessionCallResult, SimpleStateStorage,
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
    type InitializationArguments = ();
    type Operation = (ChainId, u64);
    type ApplicationCallArguments = ();
    type Effect = u64;
    type SessionCall = ();

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        _argument: (),
    ) -> Result<ExecutionResult, Self::Error> {
        Self::counter_id()?;
        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        (recipient_id, operation): (ChainId, u64),
    ) -> Result<ExecutionResult, Self::Error> {
        log::trace!("effect: {:?}", operation);
        Ok(ExecutionResult::default().with_effect(recipient_id, &operation))
    }

    async fn execute_effect(
        &mut self,
        _context: &EffectContext,
        effect: u64,
    ) -> Result<ExecutionResult, Self::Error> {
        log::trace!("executing {:?} via {:?}", effect, Self::counter_id()?);
        let as_bytes = bcs::to_bytes(&effect)?;
        self.call_application(true, Self::counter_id()?, &as_bytes, vec![])
            .await;
        Ok(ExecutionResult::default())
    }

    async fn handle_application_call(
        &mut self,
        _context: &CalleeContext,
        _argument: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error> {
        Err(Error::CallsNotSupported)
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        _session: Session,
        _argument: (),
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

    #[error("Error during the initialization")]
    Initialization(#[from] serde_json::Error),

    #[error("Error while deserializing BCS bytes")]
    BcsError(#[from] bcs::Error),

    #[error("Invalid application parameters")]
    Parameters,

    #[error("Error with the operation")]
    Operation,
}
