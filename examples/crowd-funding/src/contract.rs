// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_trait::async_trait;
use crowd_funding::{ApplicationCall, InitializationArgument, Message, Operation};
use fungible::{Account, AccountOwner, Destination, FungibleTokenAbi};
use linera_sdk::{
    base::{Amount, ApplicationId, SessionId, WithContractAbi},
    contract::system_api,
    ensure, ApplicationCallResult, CalleeContext, Contract, ExecutionResult, MessageContext,
    OperationContext, SessionCallResult, ViewStateStorage,
};
use linera_views::views::View;
use state::{CrowdFunding, Status};
use thiserror::Error;

linera_sdk::contract!(CrowdFunding);

impl WithContractAbi for CrowdFunding {
    type Abi = crowd_funding::CrowdFundingAbi;
}

#[async_trait]
impl Contract for CrowdFunding {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        argument: InitializationArgument,
    ) -> Result<ExecutionResult<Self::Message>, Self::Error> {
        self.initialization_argument.set(Some(argument));

        ensure!(
            self.initialization_argument().deadline > system_api::current_system_time(),
            Error::DeadlineInThePast
        );

        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        context: &OperationContext,
        operation: Operation,
    ) -> Result<ExecutionResult<Self::Message>, Self::Error> {
        let mut result = ExecutionResult::default();

        match operation {
            Operation::PledgeWithTransfer { owner, amount } => {
                if context.chain_id == system_api::current_application_id().creation.chain_id {
                    self.execute_pledge_with_account(owner, amount).await?;
                } else {
                    self.execute_pledge_with_transfer(&mut result, owner, amount)
                        .await?;
                }
            }
            Operation::Collect => self.collect_pledges().await?,
            Operation::Cancel => self.cancel_campaign().await?,
        }

        Ok(result)
    }

    async fn execute_message(
        &mut self,
        context: &MessageContext,
        message: Message,
    ) -> Result<ExecutionResult<Self::Message>, Self::Error> {
        match message {
            Message::PledgeWithAccount { owner, amount } => {
                ensure!(
                    context.chain_id == system_api::current_application_id().creation.chain_id,
                    Error::CampaignChainOnly
                );
                self.execute_pledge_with_account(owner, amount).await?;
            }
        }
        Ok(ExecutionResult::default())
    }

    async fn handle_application_call(
        &mut self,
        context: &CalleeContext,
        call: ApplicationCall,
        sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult<Self::Message, Self::Response, Self::SessionState>, Self::Error>
    {
        let mut result = ApplicationCallResult::default();
        match call {
            ApplicationCall::PledgeWithSessions { source } => {
                // Only sessions on the campaign chain are supported.
                ensure!(
                    context.chain_id == system_api::current_application_id().creation.chain_id,
                    Error::CampaignChainOnly
                );
                // In real-life applications, the source could be constrained so that a
                // refund cannot be used as a transfer.
                self.execute_pledge_with_sessions(source, sessions).await?
            }
            ApplicationCall::PledgeWithTransfer { owner, amount } => {
                self.execute_pledge_with_transfer(&mut result.execution_result, owner, amount)
                    .await?;
            }
            ApplicationCall::Collect => self.collect_pledges().await?,
            ApplicationCall::Cancel => self.cancel_campaign().await?,
        }

        Ok(result)
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        _state: Self::SessionState,
        _call: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult<Self::Message, Self::Response, Self::SessionState>, Self::Error>
    {
        Err(Error::SessionsNotSupported)
    }
}

impl CrowdFunding {
    fn fungible_id() -> Result<ApplicationId<FungibleTokenAbi>, Error> {
        // TODO(#723): We should be able to pull the fungible ID from the
        // `required_application_ids` of the application description.
        Self::parameters()
    }

    /// Adds a pledge from a local account to the remote campaign chain.
    async fn execute_pledge_with_transfer(
        &mut self,
        result: &mut ExecutionResult<Message>,
        owner: AccountOwner,
        amount: Amount,
    ) -> Result<(), Error> {
        ensure!(amount > Amount::zero(), Error::EmptyPledge);
        // The campaign chain.
        let chain_id = system_api::current_application_id().creation.chain_id;
        // First, move the funds to the campaign chain (under the same owner).
        // TODO(#589): Simplify this when the messaging system guarantees atomic delivery
        // of all messages created in the same operation/message.
        let destination = fungible::Destination::Account(Account { chain_id, owner });
        let call = fungible::ApplicationCall::Transfer {
            owner,
            amount,
            destination,
        };
        self.call_application(
            /* authenticated by owner */ true,
            Self::fungible_id()?,
            &call,
            vec![],
        )
        .await?;
        // Second, schedule the attribution of the funds to the (remote) campaign.
        let message = Message::PledgeWithAccount { owner, amount };
        result.messages.push((
            chain_id.into(),
            /* authenticated by owner */ true,
            message,
        ));
        Ok(())
    }

    /// Adds a pledge from a local account to the campaign chain.
    async fn execute_pledge_with_account(
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
        let sessions = self.check_session_tokens(sessions)?;

        let session_balances = self.query_session_balances(&sessions).await?;
        let amount = session_balances.iter().sum();

        ensure!(amount > Amount::zero(), Error::EmptyPledge);

        self.collect_session_tokens(sessions, session_balances)
            .await?;

        self.finish_pledge(source, amount).await
    }

    /// Checks that the sessions pledged all use the correct token. Marks the sessions
    /// with the correct Abi.
    fn check_session_tokens(
        &self,
        sessions: Vec<SessionId>,
    ) -> Result<Vec<SessionId<FungibleTokenAbi>>, Error> {
        let fungible_id = Self::fungible_id()?.forget_abi();
        ensure!(
            sessions
                .iter()
                .all(|session_id| session_id.application_id == fungible_id),
            Error::IncorrectToken
        );
        let sessions = sessions.into_iter().map(|s| s.with_abi()).collect();
        Ok(sessions)
    }

    /// Gathers the balances in all the pledged sessions.
    async fn query_session_balances(
        &mut self,
        sessions: &[SessionId<FungibleTokenAbi>],
    ) -> Result<Vec<Amount>, Error> {
        let mut balances = Vec::with_capacity(sessions.len());
        for session in sessions {
            let (balance, _) = self
                .call_session(false, *session, &fungible::SessionCall::Balance, vec![])
                .await?;
            balances.push(balance);
        }
        Ok(balances)
    }

    /// Collects all tokens in the sessions and places them in custody of the campaign.
    async fn collect_session_tokens(
        &mut self,
        sessions: Vec<SessionId<FungibleTokenAbi>>,
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
            Status::Complete => {
                self.send_to(amount, self.initialization_argument().owner)
                    .await
            }
            Status::Cancelled => Err(Error::Cancelled),
        }
    }

    /// Collects all pledges and completes the campaign if the target has been reached.
    async fn collect_pledges(&mut self) -> Result<(), Error> {
        let total = self.balance().await?;

        match self.status.get() {
            Status::Active => {
                ensure!(
                    total >= self.initialization_argument().target,
                    Error::TargetNotReached
                );
            }
            Status::Complete => (),
            Status::Cancelled => return Err(Error::Cancelled),
        }

        self.send_to(total, self.initialization_argument().owner)
            .await?;
        self.pledges.clear();
        self.status.set(Status::Complete);

        Ok(())
    }

    /// Cancels the campaign if the deadline has passed, refunding all pledges.
    async fn cancel_campaign(&mut self) -> Result<(), Error> {
        ensure!(!self.status.get().is_complete(), Error::Completed);

        // TODO(#728): Remove this.
        #[cfg(not(any(test, feature = "test")))]
        ensure!(
            system_api::current_system_time() >= self.initialization_argument().deadline,
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
        self.send_to(balance, self.initialization_argument().owner)
            .await?;
        self.status.set(Status::Cancelled);

        Ok(())
    }

    /// Queries the token application to determine the total amount of tokens in custody.
    async fn balance(&mut self) -> Result<Amount, Error> {
        let owner = AccountOwner::Application(system_api::current_application_id());
        let (amount, _) = self
            .call_application(
                true,
                Self::fungible_id()?,
                &fungible::ApplicationCall::Balance { owner },
                vec![],
            )
            .await?;
        Ok(amount)
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
        self.call_application(true, Self::fungible_id()?, &transfer, vec![])
            .await?;
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
        self.call_application(true, Self::fungible_id()?, &transfer, vec![])
            .await?;
        Ok(())
    }

    /// Calls into the Fungible Token application to receive tokens from the given account.
    async fn receive_from_session(
        &mut self,
        session: SessionId<FungibleTokenAbi>,
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
        self.call_session(false, session, &transfer, vec![]).await?;
        Ok(())
    }

    fn initialization_argument(&self) -> &InitializationArgument {
        self.initialization_argument
            .get()
            .as_ref()
            .expect("Application is not running on the host chain or was not initialized yet")
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Action can only be executed on the chain that created the crowd-funding campaign
    #[error("Action can only be executed on the chain that created the crowd-funding campaign")]
    CampaignChainOnly,

    /// Crowd-funding application doesn't support any cross-application sessions.
    #[error("Crowd-funding application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    /// Crowd-funding campaign cannot start after its deadline.
    #[error("Crowd-funding campaign cannot start after its deadline")]
    DeadlineInThePast,

    /// A pledge can not be empty.
    #[error("Pledge is empty")]
    EmptyPledge,

    /// Pledge used a token that's not the same as the one in the campaign's [`InitializationArgument`].
    #[error("Pledge uses the incorrect token")]
    IncorrectToken,

    /// Pledge used a destination that's not the same as this campaign's [`ApplicationId`].
    #[error("Pledge uses the incorrect destination account")]
    IncorrectDestination,

    /// Cross-application call without a source application ID.
    #[error("Applications must identify themselves to perform transfers")]
    MissingSourceApplication,

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

    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),
}
