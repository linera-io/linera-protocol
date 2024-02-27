// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::NativeFungibleToken;
use async_trait::async_trait;
use fungible::{ApplicationCall, FungibleResponse, Message, Operation};
use linera_sdk::{
    base::{Account, AccountOwner, Amount, Owner, SessionId, WithContractAbi},
    contract::system_api,
    ApplicationCallOutcome, CalleeContext, Contract, ExecutionOutcome, MessageContext,
    OperationContext, SessionCallOutcome, ViewStateStorage,
};
use native_fungible::TICKER_SYMBOL;
use thiserror::Error;

linera_sdk::contract!(NativeFungibleToken);

impl WithContractAbi for NativeFungibleToken {
    type Abi = fungible::FungibleTokenAbi;
}

#[async_trait]
impl Contract for NativeFungibleToken {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        state: Self::InitializationArgument,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        // Validate that the application parameters were configured correctly.
        assert!(
            Self::parameters().is_ok_and(|param| param.ticker_symbol == "NAT"),
            "Only NAT is accepted as ticker symbol"
        );
        for (owner, amount) in state.accounts {
            let owner = self.normalize_owner(owner);
            let account = Account {
                chain_id: system_api::current_chain_id(),
                owner: Some(owner),
            };
            system_api::transfer(None, account, amount);
        }
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
                Self::check_account_authentication(context.authenticated_signer, owner)?;
                let account_owner = owner;
                let owner = self.normalize_owner(owner);

                let fungible_target_account = target_account;
                let target_account = self.normalize_account(target_account);

                system_api::transfer(Some(owner), target_account, amount);

                Ok(self.get_transfer_outcome(account_owner, fungible_target_account, amount))
            }

            Operation::Claim {
                source_account,
                amount,
                target_account,
            } => {
                Self::check_account_authentication(
                    context.authenticated_signer,
                    source_account.owner,
                )?;

                let fungible_source_account = source_account;
                let fungible_target_account = target_account;

                let source_account = self.normalize_account(source_account);
                let target_account = self.normalize_account(target_account);

                system_api::claim(source_account, target_account, amount);
                Ok(
                    self.get_claim_outcome(
                        fungible_source_account,
                        fungible_target_account,
                        amount,
                    ),
                )
            }
        }
    }

    // TODO(#1721): After message is separated from the Abi, create an empty Notify message
    // to be the only message used here, simple message (no authentication, not tracked)
    async fn execute_message(
        &mut self,
        context: &MessageContext,
        message: Self::Message,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        // Messages for now don't do anything, just pass messages around
        match message {
            Message::Credit {
                amount: _,
                target: _,
                source: _,
            } => {
                // If we ever actually implement this, we need to remember
                // to check if it's a bouncing message like in the fungible app
                Ok(ExecutionOutcome::default())
            }
            Message::Withdraw {
                owner,
                amount,
                target_account,
            } => {
                Self::check_account_authentication(context.authenticated_signer, owner)?;
                Ok(self.get_transfer_outcome(owner, target_account, amount))
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
                let owner = self.normalize_owner(owner);

                let mut outcome = ApplicationCallOutcome::default();
                let balance = system_api::current_owner_balance(owner);
                outcome.value = FungibleResponse::Balance(balance);
                Ok(outcome)
            }

            ApplicationCall::Transfer {
                owner,
                amount,
                destination,
            } => {
                Self::check_account_authentication(context.authenticated_signer, owner)?;
                let account_owner = owner;
                let owner = self.normalize_owner(owner);

                let fungible_target_account = self.destination_to_account(destination);
                let target_account = self.normalize_account(fungible_target_account);

                system_api::transfer(Some(owner), target_account, amount);
                let execution_outcome =
                    self.get_transfer_outcome(account_owner, fungible_target_account, amount);
                Ok(ApplicationCallOutcome {
                    execution_outcome,
                    ..Default::default()
                })
            }

            ApplicationCall::Claim {
                source_account,
                amount,
                target_account,
            } => {
                Self::check_account_authentication(
                    context.authenticated_signer,
                    source_account.owner,
                )?;

                let fungible_source_account = source_account;
                let fungible_target_account = target_account;

                let source_account = self.normalize_account(source_account);
                let target_account = self.normalize_account(target_account);

                system_api::claim(source_account, target_account, amount);
                let execution_outcome = self.get_claim_outcome(
                    fungible_source_account,
                    fungible_target_account,
                    amount,
                );
                Ok(ApplicationCallOutcome {
                    execution_outcome,
                    ..Default::default()
                })
            }

            ApplicationCall::TickerSymbol => {
                let outcome = ApplicationCallOutcome {
                    value: FungibleResponse::TickerSymbol(String::from(TICKER_SYMBOL)),
                    ..Default::default()
                };
                Ok(outcome)
            }
        }
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        _state: Self::SessionState,
        _request: Self::SessionCall,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallOutcome<Self::Message, Self::Response, Self::SessionState>, Self::Error>
    {
        Err(Error::SessionsNotSupported)
    }
}

impl NativeFungibleToken {
    fn get_transfer_outcome(
        &self,
        source: AccountOwner,
        target: fungible::Account,
        amount: Amount,
    ) -> ExecutionOutcome<Message> {
        if target.chain_id == system_api::current_chain_id() {
            ExecutionOutcome::default()
        } else {
            let message = Message::Credit {
                target: target.owner,
                amount,
                source,
            };

            ExecutionOutcome::default().with_message(target.chain_id, message)
        }
    }

    fn get_claim_outcome(
        &self,
        source: fungible::Account,
        target: fungible::Account,
        amount: Amount,
    ) -> ExecutionOutcome<Message> {
        if source.chain_id == system_api::current_chain_id() {
            self.get_transfer_outcome(source.owner, target, amount)
        } else {
            // If different chain, send message that will be ignored so the app gets auto-deployed
            let message = Message::Withdraw {
                owner: source.owner,
                amount,
                target_account: target,
            };
            ExecutionOutcome::default().with_message(source.chain_id, message)
        }
    }

    fn normalize_owner(&self, account_owner: AccountOwner) -> Owner {
        match account_owner {
            AccountOwner::User(owner) => owner,
            AccountOwner::Application(_) => panic!("Applications not supported yet!"),
        }
    }

    fn normalize_account(&self, account: fungible::Account) -> Account {
        let owner = self.normalize_owner(account.owner);
        Account {
            chain_id: account.chain_id,
            owner: Some(owner),
        }
    }

    fn destination_to_account(&self, destination: fungible::Destination) -> fungible::Account {
        match destination {
            fungible::Destination::Account(account) => account,
            fungible::Destination::NewSession => panic!("Sessions not supported yet!"),
        }
    }

    /// Verifies that a transfer is authenticated for this local account.
    fn check_account_authentication(
        authenticated_signer: Option<Owner>,
        owner: AccountOwner,
    ) -> Result<(), Error> {
        match owner {
            AccountOwner::User(address) if authenticated_signer == Some(address) => Ok(()),
            AccountOwner::Application(_) => Err(Error::ApplicationsNotSupported),
            _ => Err(Error::IncorrectAuthentication),
        }
    }
}

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

    #[error("Native Fungible application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    #[error("Applications not supported yet")]
    ApplicationsNotSupported,
}
