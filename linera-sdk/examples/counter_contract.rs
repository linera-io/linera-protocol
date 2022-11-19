// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(target_arch = "wasm32")]

use async_trait::async_trait;
use linera_sdk::{
    ApplicationCallResult, CalleeContext, Contract, EffectContext, ExecutionResult,
    OperationContext, Session, SessionCallResult, SessionId,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The application state.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct Counter {
    value: u128,
}

#[async_trait]
impl Contract for Counter {
    type Error = Error;

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        operation: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        let increment: u128 = bcs::from_bytes(operation)?;
        self.value += increment;
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
        argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error> {
        let increment: u128 = bcs::from_bytes(argument)?;
        self.value += increment;
        Ok(ApplicationCallResult {
            value: bcs::to_bytes(&self.value).expect("Serialization should not fail"),
            ..ApplicationCallResult::default()
        })
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
    /// Counter application doesn't support any cross-chain effects.
    #[error("Counter application doesn't support any cross-chain effects")]
    EffectsNotSupported,

    /// Counter application doesn't support any cross-application sessions.
    #[error("Counter application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    /// Invalid serialized increment value.
    #[error("Invalid serialized increment value")]
    InvalidIncrement(#[from] bcs::Error),
}

/// Alias to the contract type, so that the boilerplate module can reference it.
type ContractState = Counter;

mod contract_boilerplate;
