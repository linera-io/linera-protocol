// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::FungibleToken;
use async_trait::async_trait;
use fungible::{
    Account, ApplicationCall, Destination, FungibleResponse, Message, Operation, SessionCall,
};
use linera_sdk::{
    base::{AccountOwner, Amount, ApplicationId, Owner, SessionId, WithContractAbi},
    contract::system_api,
    ApplicationCallOutcome, CalleeContext, Contract, ExecutionOutcome, MessageContext,
    OperationContext, SessionCallOutcome, ViewStateStorage,
};
use std::str::FromStr;
use thiserror::Error;

linera_sdk::contract!(FungibleToken);

impl WithContractAbi for FungibleToken {
    type Abi = fungible::FungibleTokenAbi;
}

#[async_trait]
impl Contract for FungibleToken {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn initialize(
        &mut self,
        context: &OperationContext,
        mut state: Self::InitializationArgument,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        // Validate that the application parameters were configured correctly.
        assert!(Self::parameters().is_ok());

        // If initial accounts are empty, creator gets 1M tokens to act like a faucet.
        if state.accounts.is_empty() {
            if let Some(owner) = context.authenticated_signer {
                state.accounts.insert(
                    AccountOwner::User(owner),
                    Amount::from_str("1000000").unwrap(),
                );
            }
        }
        self.initialize_accounts(state).await;

        Ok(ExecutionOutcome::default())
    }

    async fn execute_operation(
        &mut self,
        context: &OperationContext,
        operation: Self::Operation,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        match operation {
            Operation::Transfer {
                owner,
                amount,
                target_account,
            } => {
                Self::check_account_authentication(None, context.authenticated_signer, owner)?;
                self.debit(owner, amount).await?;
                Ok(self
                    .finish_transfer_to_account(amount, target_account, owner)
                    .await)
            }

            Operation::Claim {
                source_account,
                amount,
                target_account,
            } => {
                Self::check_account_authentication(
                    None,
                    context.authenticated_signer,
                    source_account.owner,
                )?;
                self.claim(source_account, amount, target_account).await
            }
        }
    }

    async fn execute_message(
        &mut self,
        context: &MessageContext,
        message: Message,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        match message {
            Message::Credit {
                amount,
                target,
                source,
            } => {
                let receiver = if context.is_bouncing { source } else { target };
                self.credit(receiver, amount).await;
                Ok(ExecutionOutcome::default())
            }
            Message::Withdraw {
                owner,
                amount,
                target_account,
            } => {
                Self::check_account_authentication(None, context.authenticated_signer, owner)?;
                self.debit(owner, amount).await?;
                Ok(self
                    .finish_transfer_to_account(amount, target_account, owner)
                    .await)
            }
        }
    }

    async fn handle_application_call(
        &mut self,
        context: &CalleeContext,
        call: ApplicationCall,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<
        ApplicationCallOutcome<Self::Message, Self::Response, Self::SessionState>,
        Self::Error,
    > {
        match call {
            ApplicationCall::Balance { owner } => {
                let mut outcome = ApplicationCallOutcome::default();
                let balance = self.balance_or_default(&owner).await;
                outcome.value = FungibleResponse::Balance(balance);
                Ok(outcome)
            }

            ApplicationCall::Transfer {
                owner,
                amount,
                destination,
            } => {
                Self::check_account_authentication(
                    context.authenticated_caller_id,
                    context.authenticated_signer,
                    owner,
                )?;
                self.debit(owner, amount).await?;
                let mut outcome = ApplicationCallOutcome::default();
                match destination {
                    Destination::Account(account) => {
                        outcome.execution_outcome = self
                            .finish_transfer_to_account(amount, account, owner)
                            .await;
                    }
                    Destination::NewSession => {
                        outcome.create_sessions.push(amount);
                    }
                }
                Ok(outcome)
            }

            ApplicationCall::Claim {
                source_account,
                amount,
                target_account,
            } => {
                Self::check_account_authentication(
                    None,
                    context.authenticated_signer,
                    source_account.owner,
                )?;
                let execution_outcome = self.claim(source_account, amount, target_account).await?;
                Ok(ApplicationCallOutcome {
                    execution_outcome,
                    ..Default::default()
                })
            }

            ApplicationCall::TickerSymbol => {
                let mut outcome = ApplicationCallOutcome::default();
                let params = Self::parameters()?;
                outcome.value = FungibleResponse::TickerSymbol(params.ticker_symbol);
                Ok(outcome)
            }
        }
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        state: Self::SessionState,
        request: SessionCall,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallOutcome<Self::Message, Self::Response, Self::SessionState>, Self::Error>
    {
        match request {
            SessionCall::Balance => self.handle_session_balance(state),
            SessionCall::Transfer {
                amount,
                destination,
            } => {
                self.handle_session_transfer(state, amount, destination)
                    .await
            }
        }
    }
}

impl FungibleToken {
    /// Verifies that a transfer is authenticated for this local account.
    fn check_account_authentication(
        authenticated_application_id: Option<ApplicationId>,
        authenticated_signer: Option<Owner>,
        owner: AccountOwner,
    ) -> Result<(), Error> {
        match owner {
            AccountOwner::User(address) if authenticated_signer == Some(address) => Ok(()),
            AccountOwner::Application(id) if authenticated_application_id == Some(id) => Ok(()),
            _ => Err(Error::IncorrectAuthentication),
        }
    }

    /// Handles a session balance request sent by an application.
    fn handle_session_balance(
        &self,
        balance: Amount,
    ) -> Result<SessionCallOutcome<Message, FungibleResponse, Amount>, Error> {
        let application_call_outcome = ApplicationCallOutcome {
            value: FungibleResponse::Balance(balance),
            execution_outcome: ExecutionOutcome::default(),
            create_sessions: vec![],
        };
        let session_call_outcome = SessionCallOutcome {
            inner: application_call_outcome,
            new_state: Some(balance),
        };
        Ok(session_call_outcome)
    }

    /// Handles a transfer from a session.
    async fn handle_session_transfer(
        &mut self,
        mut balance: Amount,
        amount: Amount,
        destination: Destination,
    ) -> Result<SessionCallOutcome<Message, FungibleResponse, Amount>, Error> {
        balance
            .try_sub_assign(amount)
            .map_err(|_| Error::InsufficientSessionBalance)?;

        let updated_session = (balance > Amount::ZERO).then_some(balance);

        let mut outcome = ApplicationCallOutcome::default();
        match destination {
            Destination::Account(account) => {
                outcome.execution_outcome = self
                    .finish_transfer_to_account(amount, account, account.owner)
                    .await;
            }
            Destination::NewSession => {
                outcome.create_sessions.push(amount);
            }
        }

        Ok(SessionCallOutcome {
            inner: outcome,
            new_state: updated_session,
        })
    }

    async fn claim(
        &mut self,
        source_account: Account,
        amount: Amount,
        target_account: Account,
    ) -> Result<ExecutionOutcome<Message>, Error> {
        if source_account.chain_id == system_api::current_chain_id() {
            self.debit(source_account.owner, amount).await?;
            Ok(self
                .finish_transfer_to_account(amount, target_account, source_account.owner)
                .await)
        } else {
            let message = Message::Withdraw {
                owner: source_account.owner,
                amount,
                target_account,
            };
            Ok(ExecutionOutcome::default()
                .with_authenticated_message(source_account.chain_id, message))
        }
    }

    /// Executes the final step of a transfer where the tokens are sent to the destination.
    async fn finish_transfer_to_account(
        &mut self,
        amount: Amount,
        target_account: Account,
        source: AccountOwner,
    ) -> ExecutionOutcome<Message> {
        if target_account.chain_id == system_api::current_chain_id() {
            self.credit(target_account.owner, amount).await;
            ExecutionOutcome::default()
        } else {
            let message = Message::Credit {
                target: target_account.owner,
                amount,
                source,
            };
            ExecutionOutcome::default().with_tracked_message(target_account.chain_id, message)
        }
    }
}

// Dummy ComplexObject implementation, required by the graphql(complex) attribute in state.rs.
#[async_graphql::ComplexObject]
impl FungibleToken {}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Insufficient balance in source account.
    #[error("Source account does not have sufficient balance for transfer")]
    InsufficientBalance(#[from] state::InsufficientBalanceError),

    /// Insufficient balance in session.
    #[error("Session does not have sufficient balance for transfer")]
    InsufficientSessionBalance,

    /// Requested transfer does not have permission on this account.
    #[error("The requested transfer is not correctly authenticated.")]
    IncorrectAuthentication,

    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),
}
