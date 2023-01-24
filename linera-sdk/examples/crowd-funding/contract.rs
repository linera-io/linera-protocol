// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(target_arch = "wasm32")]

mod state;

use self::{
    boilerplate::system_api,
    state::{ApplicationState, CrowdFunding},
};
use async_trait::async_trait;
use fungible::{AccountOwner, SignedTransfer};
use linera_sdk::{
    ensure, ApplicationCallResult, CalleeContext, Contract, EffectContext, ExecutionResult,
    OperationContext, Session, SessionCallResult, SessionId,
};
use serde::{Deserialize, Serialize};
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
        let operation: Operation =
            bcs::from_bytes(operation_bytes).map_err(Error::InvalidOperation)?;

        match operation {
            Operation::Pledge { transfer } => self.signed_pledge(transfer).await?,
        }

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

impl CrowdFunding {
    /// Adds a pledge from a [`SignedTransfer`].
    async fn signed_pledge(&mut self, transfer: SignedTransfer) -> Result<(), Error> {
        let amount = transfer.payload.transfer.amount;
        let source = AccountOwner::Key(transfer.source);

        ensure!(transfer.payload.transfer.amount > 0, Error::EmptyPledge);
        ensure!(
            transfer.payload.token_id == self.parameters().token,
            Error::IncorrectToken
        );
        ensure!(
            transfer.payload.transfer.destination_chain == system_api::current_chain_id(),
            Error::IncorrectDestination
        );
        ensure!(
            transfer.payload.transfer.destination_account
                == AccountOwner::Application(system_api::current_application_id()),
            Error::IncorrectDestination
        );

        self.transfer(fungible::ApplicationCall::Delegated(transfer))
            .await?;

        self.finish_pledge(source, amount)
    }

    /// Marks a pledge in the application state, so that it can be returned if the campaign is
    /// cancelled.
    fn finish_pledge(&mut self, source: AccountOwner, amount: u128) -> Result<(), Error> {
        ensure!(!self.status.is_cancelled(), Error::Cancelled);

        *self.pledges.entry(source).or_insert(0) += amount;

        Ok(())
    }

    /// Calls into the Fungible Token application to execute the `transfer`.
    async fn transfer(&self, transfer: fungible::ApplicationCall) -> Result<(), Error> {
        let transfer_bytes = bcs::to_bytes(&transfer).map_err(Error::InvalidTransfer)?;

        system_api::call_application(true, self.parameters().token, &transfer_bytes, vec![])
            .await
            .map_err(Error::Transfer)?;

        Ok(())
    }
}

/// Operations that can be sent to the application.
#[derive(Deserialize, Serialize)]
pub enum Operation {
    /// Pledge some tokens to the campaign.
    Pledge { transfer: SignedTransfer },
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

    /// Operation bytes does not deserialize into an [`Operation`].
    #[error("Requested operation is invalid")]
    InvalidOperation(bcs::Error),

    /// A pledge can not be empty.
    #[error("Pledge is empty")]
    EmptyPledge,

    /// Pledge used a token that's not the same as the one in the campaign's [`Parameters`].
    #[error("Pledge uses the incorrect token")]
    IncorrectToken,

    /// Pledge used a destination that's not the same as this campaign's [`ApplicationId`].
    #[error("Pledge uses the incorrect destination account")]
    IncorrectDestination,

    /// Fungible Token application did not execute the requested transfer.
    #[error("Failed to transfer tokens: {0}")]
    Transfer(String),

    /// An invalid transfer was constructed.
    #[error("Transfer is invalid because it can't be serialized")]
    InvalidTransfer(bcs::Error),

    /// Can't pledge to or collect pledges from a cancelled campaign.
    #[error("Crowd-funding campaign has been cancelled")]
    Cancelled,
}

#[path = "../boilerplate/contract/mod.rs"]
mod boilerplate;

// Work-around to pretend that `fungible` is an external crate, exposing the Fungible Token
// application's interface.
#[path = "../fungible/interface.rs"]
#[allow(dead_code)]
mod fungible;
