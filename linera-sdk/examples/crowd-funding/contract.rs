// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(target_arch = "wasm32")]

mod interface;
mod state;

use self::{
    interface::ApplicationCall,
    state::{CrowdFunding, Status},
};
use async_trait::async_trait;
use fungible::{AccountOwner, ApplicationTransfer, SignedTransfer, Transfer};
use futures::{future, stream, StreamExt, TryFutureExt, TryStreamExt};
use linera_sdk::{
    contract::system_api, ensure, ApplicationCallResult, CalleeContext, Contract, EffectContext,
    ExecutionResult, FromBcsBytes, OperationContext, Session, SessionCallResult, SessionId,
};
use serde::{Deserialize, Serialize};
use std::mem;
use thiserror::Error;

linera_sdk::contract!(CrowdFunding);

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
        let call: ApplicationCall =
            bcs::from_bytes(argument).map_err(Error::InvalidCrossApplicationCall)?;

        match call {
            ApplicationCall::Pledge => self.application_pledge(context, sessions).await?,
            ApplicationCall::DelegatedPledge { transfer } => self.signed_pledge(transfer).await?,
            ApplicationCall::Collect => self.collect_pledges().await?,
            ApplicationCall::Cancel => self.cancel_campaign().await?,
        }

        Ok(ApplicationCallResult::default())
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

        self.finish_pledge(source, amount).await
    }

    /// Adds a pledge sent from an application using token sessions.
    async fn application_pledge(
        &mut self,
        context: &CalleeContext,
        sessions: Vec<SessionId>,
    ) -> Result<(), Error> {
        self.check_session_tokens(&sessions)?;

        let session_balances = Self::query_session_balances(&sessions).await?;
        let amount = session_balances.iter().sum();

        ensure!(amount > 0, Error::EmptyPledge);

        Self::collect_session_tokens(sessions, session_balances).await?;

        let source_application = context
            .authenticated_caller_id
            .ok_or(Error::MissingSourceApplication)?;
        let source = AccountOwner::Application(source_application);

        self.finish_pledge(source, amount).await
    }

    /// Checks that the sessions pledged all use the correct token.
    fn check_session_tokens(&self, sessions: &[SessionId]) -> Result<(), Error> {
        ensure!(
            sessions
                .iter()
                .all(|session_id| session_id.application_id == self.parameters().token),
            Error::IncorrectToken
        );

        Ok(())
    }

    /// Gathers the balances in all the pledged sessions.
    async fn query_session_balances(sessions: &[SessionId]) -> Result<Vec<u128>, Error> {
        let balance_query = bcs::to_bytes(&fungible::SessionCall::Balance)
            .map_err(Error::InvalidSessionBalanceQuery)?;

        stream::iter(sessions)
            .then(|session| {
                system_api::call_session(false, *session, &balance_query, vec![])
                    .map_err(Error::SessionBalance)
            })
            .and_then(|(balance_bytes, _)| {
                future::ready(
                    u128::from_bcs_bytes(&balance_bytes).map_err(Error::InvalidSessionBalance),
                )
            })
            .try_collect()
            .await
    }

    /// Collects all tokens in the sessions and places them in custody of the campaign.
    async fn collect_session_tokens(
        sessions: Vec<SessionId>,
        balances: Vec<u128>,
    ) -> Result<(), Error> {
        let destination_account = AccountOwner::Application(system_api::current_application_id());
        let destination_chain = system_api::current_chain_id();

        stream::iter(sessions.into_iter().zip(balances))
            .map(Ok)
            .try_for_each_concurrent(None, move |(session, balance)| async move {
                let transfer = Transfer {
                    destination_account,
                    destination_chain,
                    amount: balance,
                };
                let transfer_bytes = bcs::to_bytes(&transfer).map_err(Error::InvalidTransfer)?;

                system_api::call_session(false, session, &transfer_bytes, vec![])
                    .map_err(Error::Transfer)
                    .await?;

                Ok(())
            })
            .await
    }

    /// Marks a pledge in the application state, so that it can be returned if the campaign is
    /// cancelled.
    async fn finish_pledge(&mut self, source: AccountOwner, amount: u128) -> Result<(), Error> {
        match self.status {
            Status::Active => {
                *self.pledges.entry(source).or_insert(0) += amount;

                Ok(())
            }
            Status::Complete => self.send(amount, self.parameters().owner).await,
            Status::Cancelled => Err(Error::Cancelled),
        }
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

    /// Crowd-funding application doesn't support any cross-application sessions.
    #[error("Crowd-funding application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    /// Failure to deserialize the initialization parameters.
    #[error("Crowd-funding campaign parameters are invalid")]
    InvalidParameters(bcs::Error),

    /// Crowd-funding campaigns can't start after its deadline.
    #[error("Crowd-funding campaign can not start after its deadline")]
    DeadlineInThePast,

    /// Operation bytes does not deserialize into an [`Operation`].
    #[error("Requested operation is invalid")]
    InvalidOperation(bcs::Error),

    /// Cross-application call argument does not deserialize into an [`ApplicationCall`].
    #[error("Requested cross-application call is invalid")]
    InvalidCrossApplicationCall(bcs::Error),

    /// A pledge can not be empty.
    #[error("Pledge is empty")]
    EmptyPledge,

    /// Pledge used a token that's not the same as the one in the campaign's [`Parameters`].
    #[error("Pledge uses the incorrect token")]
    IncorrectToken,

    /// Pledge used a destination that's not the same as this campaign's [`ApplicationId`].
    #[error("Pledge uses the incorrect destination account")]
    IncorrectDestination,

    /// Cross-application call without a source application ID.
    #[error("Applications must identify themselves to perform transfers")]
    MissingSourceApplication,

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

    /// [`fungible::SessionCall::Balance`] could not be serialized.
    #[error("Can't check session balance because the query can't be serialized")]
    InvalidSessionBalanceQuery(bcs::Error),

    /// Fungible Token application did not return the session's balance.
    #[error("Failed to read session balance: {0}")]
    SessionBalance(String),

    /// Fungible Token application returned an invalid session balance.
    #[error("Received an invalid session balance from token application")]
    InvalidSessionBalance(bcs::Error),

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
