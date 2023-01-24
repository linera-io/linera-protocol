// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(target_arch = "wasm32")]

mod state;

use self::{
    boilerplate::system_api,
    state::{ApplicationState, CrowdFunding, Status},
};
use async_trait::async_trait;
use fungible::{AccountOwner, ApplicationTransfer, SignedTransfer, Transfer};
use linera_sdk::{
    ensure, ApplicationCallResult, CalleeContext, Contract, EffectContext, ExecutionResult,
    OperationContext, Session, SessionCallResult, SessionId,
};
use serde::{Deserialize, Serialize};
use std::mem;
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
            Operation::Collect => self.collect_pledges().await?,
            Operation::Cancel => self.cancel_campaign().await?,
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

    /// Collects all pledges and completes the campaign if the target has been reached.
    async fn collect_pledges(&mut self) -> Result<(), Error> {
        let total = self.balance().await?;

        match self.status {
            Status::Active => {
                ensure!(total >= self.parameters().target, Error::TargetNotReached);
            }
            Status::Complete => (),
            Status::Cancelled => return Err(Error::Cancelled),
        }

        self.send(total, self.parameters().owner).await?;
        self.pledges.clear();
        self.status = Status::Complete;

        Ok(())
    }

    /// Cancels the campaign if the deadline has passed, refunding all pledges.
    async fn cancel_campaign(&mut self) -> Result<(), Error> {
        ensure!(!self.status.is_complete(), Error::Completed);

        ensure!(
            system_api::current_system_time() >= self.parameters().deadline,
            Error::DeadlineNotReached
        );

        for (pledger, amount) in mem::take(&mut self.pledges) {
            self.send(amount, pledger).await?;
        }

        self.send(self.balance().await?, self.parameters().owner)
            .await?;
        self.status = Status::Cancelled;

        Ok(())
    }

    /// Queries the token application to determine the total amount of tokens in custody.
    async fn balance(&self) -> Result<u128, Error> {
        let query_bytes = bcs::to_bytes(&fungible::ApplicationCall::Balance)
            .map_err(Error::InvalidBalanceQuery)?;

        let (response, _sessions) =
            system_api::call_application(true, self.parameters().token, &query_bytes, vec![])
                .await
                .map_err(Error::Balance)?;

        Ok(bcs::from_bytes(&response).map_err(Error::InvalidBalance)?)
    }

    /// Transfers `amount` tokens from the funds in custody to the `destination`.
    async fn send(&self, amount: u128, destination: AccountOwner) -> Result<(), Error> {
        let transfer = ApplicationTransfer::Static(Transfer {
            destination_account: destination,
            destination_chain: system_api::current_chain_id(),
            amount,
        });

        self.transfer(fungible::ApplicationCall::Transfer(transfer))
            .await
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
    /// Collect the pledges after the campaign has reached its target.
    Collect,
    /// Cancel the campaign and refund all pledges after the campaign has reached its deadline.
    Cancel,
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

    /// [`fungible::ApplicationCall::Balance`] could not be serialized.
    #[error("Can't check balance because the query can't be serialized")]
    InvalidBalanceQuery(bcs::Error),

    /// Fungible Token application did not return the campaign's balance.
    #[error("Failed to read application balance: {0}")]
    Balance(String),

    /// Fungible Token application returned an invalid balance.
    #[error("Received an invalid balance from token application")]
    InvalidBalance(bcs::Error),

    /// Can't collect pledges before the campaign target has been reached.
    #[error("Crowd-funding campaign has not reached its target yet")]
    TargetNotReached,

    /// Can't cancel a campaign before its deadline.
    #[error("Crowd-funding campaign has not reached its deadline yet")]
    DeadlineNotReached,

    /// Can't cancel a campaign after it has been completed.
    #[error("Crowd-funding campaign has already been completed")]
    Completed,

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
