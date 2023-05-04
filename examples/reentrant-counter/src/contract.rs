// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::ReentrantCounter;
use async_trait::async_trait;
use linera_sdk::{
    base::SessionId, contract::system_api, views::ViewStorageContext, ApplicationCallResult,
    CalleeContext, Contract, EffectContext, ExecutionResult, OperationContext, Session,
    SessionCallResult, ViewStateStorage,
};
use thiserror::Error;

linera_sdk::contract!(ReentrantCounter<ViewStorageContext>);

#[async_trait]
impl Contract for ReentrantCounter<ViewStorageContext> {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;
    type InitializationArguments = u128;
    type Operation = u128;
    type ApplicationCallArguments = u128;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        argument: u128,
    ) -> Result<ExecutionResult, Self::Error> {
        self.value.set(argument);
        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        increment: u128,
    ) -> Result<ExecutionResult, Self::Error> {
        let first_half = increment / 2;
        let second_half = increment - first_half;
        let second_half_as_bytes = bcs::to_bytes(&second_half).expect("Failed to serialize `u128`");

        let value = self.value.get_mut();
        *value += first_half;

        self.call_application(
            false,
            system_api::current_application_id(),
            &second_half_as_bytes,
            vec![],
        )
        .await;

        Ok(ExecutionResult::default())
    }

    async fn execute_effect(
        &mut self,
        _context: &EffectContext,
        _effect: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        Err(Error::EffectsNotSupported)
    }

    async fn handle_application_call(
        &mut self,
        _context: &CalleeContext,
        increment: u128,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error> {
        let value = self.value.get_mut();
        *value += increment;
        Ok(ApplicationCallResult {
            value: bcs::to_bytes(&value).expect("Serialization should not fail"),
            ..ApplicationCallResult::default()
        })
    }

    async fn handle_session_call(
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
    /// Counter application doesn't support any cross-chain effects.
    #[error("Counter application doesn't support any cross-chain effects")]
    EffectsNotSupported,

    /// Counter application doesn't support any cross-application sessions.
    #[error("Counter application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    /// Invalid serialized increment value.
    #[error("Invalid serialized increment value")]
    InvalidIncrement(#[from] bcs::Error),

    /// Invalid serialized initialization value.
    #[error("Invalid serialized initialization value")]
    InvalidInitializationArguments(#[from] serde_json::Error),
}
