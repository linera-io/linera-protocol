// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(target_arch = "wasm32")]

mod state;

use self::{
    boilerplate::system_api,
    state::{ApplicationState, CrowdFunding},
};
use async_trait::async_trait;
use linera_sdk::{
    ensure, ApplicationCallResult, CalleeContext, Contract, EffectContext, ExecutionResult,
    OperationContext, Session, SessionCallResult, SessionId,
};
use thiserror::Error;

#[async_trait]
impl Contract for CrowdFunding {
    type Error = Error;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        argument: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        self.parameters = Some(bcs::from_bytes(argument).map_err(Error::InvalidParameters)?);

        ensure!(
            self.parameters().deadline > system_api::current_system_time(),
            Error::DeadlineInThePast
        );

        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        operation_bytes: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        todo!();
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
        context: &CalleeContext,
        argument: &[u8],
        sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error> {
        todo!();
    }

    async fn call_session(
        &mut self,
        _context: &CalleeContext,
        _session: Session,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, Self::Error> {
        todo!();
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Crowd-funding application doesn't support any cross-chain effects.
    #[error("Crowd-funding application doesn't support any cross-chain effects")]
    EffectsNotSupported,

    /// Failure to deserialize the initialization parameters.
    #[error("Crowd-funding campaign parameters are invalid")]
    InvalidParameters(bcs::Error),

    /// Crowd-funding campaigns can't start after its deadline.
    #[error("Crowd-funding campaign can not start after its deadline")]
    DeadlineInThePast,
}

#[path = "../boilerplate/contract/mod.rs"]
mod boilerplate;

// Work-around to pretend that `fungible` is an external crate, exposing the Fungible Token
// application's interface.
#[path = "../fungible/interface.rs"]
#[allow(dead_code)]
mod fungible;
