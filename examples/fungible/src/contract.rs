// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::FungibleToken;
use async_trait::async_trait;
use fungible::{Account, ApplicationCall, FungibleResponse, Message, Operation};
use linera_sdk::{
    base::{AccountOwner, Amount, ApplicationId, Owner, SessionId, WithContractAbi},
    contract::system_api,
    ApplicationCallOutcome, Contract, ContractRuntime, ExecutionOutcome, SessionCallOutcome,
    ViewStateStorage,
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
        runtime: &mut ContractRuntime,
        mut state: Self::InitializationArgument,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        // Validate that the application parameters were configured correctly.
        assert!(Self::parameters().is_ok());

        // If initial accounts are empty, creator gets 1M tokens to act like a faucet.
        if state.accounts.is_empty() {
            if let Some(owner) = runtime.authenticated_signer() {
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
        runtime: &mut ContractRuntime,
        operation: Self::Operation,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        match operation {
            Operation::Transfer {
                owner,
                amount,
                target_account,
            } => {
                Self::check_account_authentication(None, runtime.authenticated_signer(), owner)?;
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
                    runtime.authenticated_signer(),
                    source_account.owner,
                )?;
                self.claim(source_account, amount, target_account).await
            }
        }
    }

    async fn execute_message(
        &mut self,
        runtime: &mut ContractRuntime,
        message: Message,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        match message {
            Message::Credit {
                amount,
                target,
                source,
            } => {
                let is_bouncing = runtime
                    .message_is_bouncing()
                    .expect("Message delivery status has to be available when executing a message");
                let receiver = if is_bouncing { source } else { target };
                self.credit(receiver, amount).await;
                Ok(ExecutionOutcome::default())
            }
            Message::Withdraw {
                owner,
                amount,
                target_account,
            } => {
                Self::check_account_authentication(None, runtime.authenticated_signer(), owner)?;
                self.debit(owner, amount).await?;
                Ok(self
                    .finish_transfer_to_account(amount, target_account, owner)
                    .await)
            }
        }
    }

    async fn handle_application_call(
        &mut self,
        runtime: &mut ContractRuntime,
        call: ApplicationCall,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallOutcome<Self::Message, Self::Response>, Self::Error> {
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
                    runtime.authenticated_caller_id(),
                    runtime.authenticated_signer(),
                    owner,
                )?;
                self.debit(owner, amount).await?;
                let execution_outcome = self
                    .finish_transfer_to_account(amount, destination, owner)
                    .await;
                Ok(ApplicationCallOutcome {
                    execution_outcome,
                    ..ApplicationCallOutcome::default()
                })
            }

            ApplicationCall::Claim {
                source_account,
                amount,
                target_account,
            } => {
                Self::check_account_authentication(
                    None,
                    runtime.authenticated_signer(),
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
        _runtime: &mut ContractRuntime,
        _state: Self::SessionState,
        _request: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallOutcome<Self::Message, Self::Response, Self::SessionState>, Self::Error>
    {
        Err(Error::SessionsNotSupported)
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

    /// Requested transfer does not have permission on this account.
    #[error("The requested transfer is not correctly authenticated.")]
    IncorrectAuthentication,

    #[error("Fungible Token application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),
}
