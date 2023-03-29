// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use async_trait::async_trait;
use crowd_funding::{ApplicationCall, Operation};
use fungible::{Account, AccountOwner, Destination};
use linera_sdk::{
    base::{Amount, SessionId},
    contract::{system_api, system_api::WasmContext},
    ensure, ApplicationCallResult, CalleeContext, Contract, EffectContext, ExecutionResult,
    FromBcsBytes, OperationContext, Session, SessionCallResult, ViewStateStorage,
};
use linera_views::views::View;
use state::{CrowdFunding, Status};
use thiserror::Error;

linera_sdk::contract!(CrowdFunding<WasmContext>);

#[async_trait]
impl Contract for CrowdFunding<WasmContext> {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        argument: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        self.parameters.set(Some(
            bcs::from_bytes(argument).map_err(Error::InvalidParameters)?,
        ));

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
            Operation::PledgeWithTransfer { owner, amount } => {
                self.execute_pledge_with_transfer(owner, amount).await?
            }
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

    async fn handle_application_call(
        &mut self,
        _context: &CalleeContext,
        argument: &[u8],
        sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error> {
        let call: ApplicationCall =
            bcs::from_bytes(argument).map_err(Error::InvalidCrossApplicationCall)?;

        match call {
            ApplicationCall::PledgeWithSessions { source } => {
                // In real-life applications, the source could be constrained so that a
                // refund cannot be used as a transfer.
                self.execute_pledge_with_sessions(source, sessions).await?
            }
            ApplicationCall::PledgeWithTransfer { owner, amount } => {
                self.execute_pledge_with_transfer(owner, amount).await?
            }
            ApplicationCall::Collect => self.collect_pledges().await?,
            ApplicationCall::Cancel => self.cancel_campaign().await?,
        }

        Ok(ApplicationCallResult::default())
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

impl CrowdFunding<WasmContext> {
    /// Adds a pledge from a transfer.
    async fn execute_pledge_with_transfer(
        &mut self,
        owner: AccountOwner,
        amount: Amount,
    ) -> Result<(), Error> {
        ensure!(amount > Amount::zero(), Error::EmptyPledge);
        self.receive_from_account(owner, amount).await?;
        self.finish_pledge(owner, amount).await
    }

    /// Adds a pledge sent from an application using token sessions.
    async fn execute_pledge_with_sessions(
        &mut self,
        source: AccountOwner,
        sessions: Vec<SessionId>,
    ) -> Result<(), Error> {
        self.check_session_tokens(&sessions)?;

        let session_balances = self.query_session_balances(&sessions).await?;
        let amount = session_balances.iter().sum();

        ensure!(amount > Amount::zero(), Error::EmptyPledge);

        self.collect_session_tokens(sessions, session_balances)
            .await?;

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
    async fn query_session_balances(
        &mut self,
        sessions: &[SessionId],
    ) -> Result<Vec<Amount>, Error> {
        let balance_query = bcs::to_bytes(&fungible::SessionCall::Balance)
            .map_err(Error::InvalidSessionBalanceQuery)?;

        let mut balances = Vec::with_capacity(sessions.len());
        for session in sessions {
            let (balance_bytes, _) = self
                .call_session(false, *session, &balance_query, vec![])
                .await;
            balances.push(
                Amount::from_bcs_bytes(&balance_bytes).map_err(Error::InvalidSessionBalance)?,
            );
        }
        Ok(balances)
    }

    /// Collects all tokens in the sessions and places them in custody of the campaign.
    async fn collect_session_tokens(
        &mut self,
        sessions: Vec<SessionId>,
        balances: Vec<Amount>,
    ) -> Result<(), Error> {
        for (session, balance) in sessions.into_iter().zip(balances) {
            self.receive_from_session(session, balance).await?;
        }
        Ok(())
    }

    /// Marks a pledge in the application state, so that it can be returned if the campaign is
    /// cancelled.
    async fn finish_pledge(&mut self, source: AccountOwner, amount: Amount) -> Result<(), Error> {
        match self.status.get() {
            Status::Active => {
                self.pledges
                    .get_mut_or_default(&source)
                    .await
                    .expect("view access should not fail")
                    .saturating_add_assign(amount);
                Ok(())
            }
            Status::Complete => self.send_to(amount, self.parameters().owner).await,
            Status::Cancelled => Err(Error::Cancelled),
        }
    }

    /// Collects all pledges and completes the campaign if the target has been reached.
    async fn collect_pledges(&mut self) -> Result<(), Error> {
        let total = self.balance().await?;

        match self.status.get() {
            Status::Active => {
                ensure!(total >= self.parameters().target, Error::TargetNotReached);
            }
            Status::Complete => (),
            Status::Cancelled => return Err(Error::Cancelled),
        }

        self.send_to(total, self.parameters().owner).await?;
        self.pledges.clear();
        self.status.set(Status::Complete);

        Ok(())
    }

    /// Cancels the campaign if the deadline has passed, refunding all pledges.
    async fn cancel_campaign(&mut self) -> Result<(), Error> {
        ensure!(!self.status.get().is_complete(), Error::Completed);

        ensure!(
            system_api::current_system_time() >= self.parameters().deadline,
            Error::DeadlineNotReached
        );

        let mut pledges = Vec::new();
        self.pledges
            .for_each_index_value(|pledger, amount| {
                pledges.push((pledger, amount));
                Ok(())
            })
            .await
            .expect("view iteration should not fail");
        for (pledger, amount) in pledges {
            self.send_to(amount, pledger).await?;
        }

        let balance = self.balance().await?;
        self.send_to(balance, self.parameters().owner).await?;
        self.status.set(Status::Cancelled);

        Ok(())
    }

    /// Queries the token application to determine the total amount of tokens in custody.
    async fn balance(&mut self) -> Result<Amount, Error> {
        let query_bytes =
            bcs::to_bytes(&fungible::SessionCall::Balance).map_err(Error::InvalidBalanceQuery)?;
        let (response, _sessions) = self
            .call_application(true, self.parameters().token, &query_bytes, vec![])
            .await;

        bcs::from_bytes(&response).map_err(Error::InvalidBalance)
    }

    /// Transfers `amount` tokens from the funds in custody to the `destination`.
    async fn send_to(&mut self, amount: Amount, owner: AccountOwner) -> Result<(), Error> {
        let account = Account {
            chain_id: system_api::current_chain_id(),
            owner,
        };
        let destination = Destination::Account(account);
        let transfer = fungible::ApplicationCall::Transfer {
            owner: AccountOwner::Application(system_api::current_application_id()),
            amount,
            destination,
        };
        let transfer_bytes = bcs::to_bytes(&transfer).map_err(Error::InvalidTransfer)?;
        self.call_application(true, self.parameters().token, &transfer_bytes, vec![])
            .await;
        Ok(())
    }

    /// Calls into the Fungible Token application to receive tokens from the given account.
    async fn receive_from_account(
        &mut self,
        owner: AccountOwner,
        amount: Amount,
    ) -> Result<(), Error> {
        let account = Account {
            chain_id: system_api::current_chain_id(),
            owner: AccountOwner::Application(system_api::current_application_id()),
        };
        let destination = Destination::Account(account);
        let transfer = fungible::ApplicationCall::Transfer {
            owner,
            amount,
            destination,
        };
        let transfer_bytes = bcs::to_bytes(&transfer).map_err(Error::InvalidTransfer)?;
        self.call_application(true, self.parameters().token, &transfer_bytes, vec![])
            .await;
        Ok(())
    }

    /// Calls into the Fungible Token application to receive tokens from the given account.
    async fn receive_from_session(
        &mut self,
        session: SessionId,
        amount: Amount,
    ) -> Result<(), Error> {
        let account = Account {
            chain_id: system_api::current_chain_id(),
            owner: AccountOwner::Application(system_api::current_application_id()),
        };
        let destination = Destination::Account(account);
        let transfer = fungible::SessionCall::Transfer {
            amount,
            destination,
        };
        let transfer_bytes = bcs::to_bytes(&transfer).map_err(Error::InvalidTransfer)?;
        self.call_session(false, session, &transfer_bytes, vec![])
            .await;
        Ok(())
    }
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

    /// Crowd-funding campaign can't start after its deadline.
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

    /// An invalid transfer was constructed.
    #[error("Transfer is invalid because it can't be serialized")]
    InvalidTransfer(bcs::Error),

    /// [`fungible::ApplicationCall::Balance`] could not be serialized.
    #[error("Can't check balance because the query can't be serialized")]
    InvalidBalanceQuery(bcs::Error),

    /// Fungible Token application returned an invalid balance.
    #[error("Received an invalid balance from token application")]
    InvalidBalance(bcs::Error),

    /// [`fungible::SessionCall::Balance`] could not be serialized.
    #[error("Can't check session balance because the query can't be serialized")]
    InvalidSessionBalanceQuery(bcs::Error),

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
