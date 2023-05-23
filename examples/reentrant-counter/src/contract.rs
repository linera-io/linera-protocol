// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::ReentrantCounter;
use async_trait::async_trait;
use linera_sdk::{
    base::{SessionId, WithContractAbi},
    contract::system_api,
    ApplicationCallResult, CalleeContext, Contract, EffectContext, ExecutionResult,
    OperationContext, SessionCallResult, ViewStateStorage,
};
use thiserror::Error;

linera_sdk::contract!(ReentrantCounter);

impl WithContractAbi for ReentrantCounter {
    type Abi = reentrant_counter::ReentrantCounterAbi;
}

#[async_trait]
impl Contract for ReentrantCounter {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        value: u64,
    ) -> Result<ExecutionResult<Self::Effect>, Self::Error> {
        self.value.set(value);
        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        increment: u64,
    ) -> Result<ExecutionResult<Self::Effect>, Self::Error> {
        let first_half = increment / 2;
        let second_half = increment - first_half;

        let value = self.value.get_mut();
        *value += first_half;

        self.call_application::<Self>(
            false,
            system_api::current_application_id(),
            &second_half,
            vec![],
        )
        .await?;

        Ok(ExecutionResult::default())
    }

    async fn execute_effect(
        &mut self,
        _context: &EffectContext,
        _effect: (),
    ) -> Result<ExecutionResult<Self::Effect>, Self::Error> {
        Err(Error::EffectsNotSupported)
    }

    async fn handle_application_call(
        &mut self,
        _context: &CalleeContext,
        increment: u64,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult<Self::Effect, Self::Response, Self::SessionState>, Self::Error>
    {
        let value = self.value.get_mut();
        *value += increment;
        Ok(ApplicationCallResult {
            value: *value,
            ..ApplicationCallResult::default()
        })
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        _state: Self::SessionState,
        _call: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult<Self::Effect, Self::Response, Self::SessionState>, Self::Error>
    {
        Err(Error::SessionsNotSupported)
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Counter application doesn't support any cross-chain effects.
    #[error("Counter application doesn't support any cross-chain effects")]
    EffectsNotSupported,

    /// Counter application doesn't support any cross-application sessions.
    #[error("Counter application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),
}
