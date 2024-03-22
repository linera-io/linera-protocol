// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_trait::async_trait;
use fungible::{ApplicationCall, FungibleResponse, FungibleTokenAbi, Message, Operation};
use linera_sdk::{
    base::{Account, AccountOwner, Amount, Owner, WithContractAbi},
    ensure, ApplicationCallOutcome, Contract, ContractRuntime, ExecutionOutcome, ViewStateStorage,
};
use native_fungible::TICKER_SYMBOL;
use thiserror::Error;

use self::state::NativeFungibleToken;

pub struct NativeFungibleTokenContract {
    state: NativeFungibleToken,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(NativeFungibleTokenContract);

impl WithContractAbi for NativeFungibleTokenContract {
    type Abi = FungibleTokenAbi;
}

#[async_trait]
impl Contract for NativeFungibleTokenContract {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;
    type State = NativeFungibleToken;

    async fn new(
        state: NativeFungibleToken,
        runtime: ContractRuntime<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(NativeFungibleTokenContract { state, runtime })
    }

    fn state_mut(&mut self) -> &mut Self::State {
        &mut self.state
    }

    async fn initialize(
        &mut self,
        state: Self::InitializationArgument,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        // Validate that the application parameters were configured correctly.
        assert!(
            self.runtime.application_parameters().ticker_symbol == "NAT",
            "Only NAT is accepted as ticker symbol"
        );
        for (owner, amount) in state.accounts {
            let owner = self.normalize_owner(owner);
            let account = Account {
                chain_id: self.runtime.chain_id(),
                owner: Some(owner),
            };
            self.runtime.transfer(None, account, amount);
        }
        Ok(ExecutionOutcome::default())
    }

    async fn execute_operation(
        &mut self,
        operation: Self::Operation,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        match operation {
            Operation::Transfer {
                owner,
                amount,
                target_account,
            } => {
                self.check_account_authentication(owner)?;
                let account_owner = owner;
                let owner = self.normalize_owner(owner);

                let fungible_target_account = target_account;
                let target_account = self.normalize_account(target_account);

                self.runtime.transfer(Some(owner), target_account, amount);

                Ok(self.get_transfer_outcome(account_owner, fungible_target_account, amount))
            }

            Operation::Claim {
                source_account,
                amount,
                target_account,
            } => {
                self.check_account_authentication(source_account.owner)?;

                let fungible_source_account = source_account;
                let fungible_target_account = target_account;

                let source_account = self.normalize_account(source_account);
                let target_account = self.normalize_account(target_account);

                self.runtime.claim(source_account, target_account, amount);
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
                self.check_account_authentication(owner)?;
                Ok(self.get_transfer_outcome(owner, target_account, amount))
            }
        }
    }

    async fn handle_application_call(
        &mut self,
        call: ApplicationCall,
    ) -> Result<ApplicationCallOutcome<Self::Message, Self::Response>, Self::Error> {
        match call {
            ApplicationCall::Balance { owner } => {
                let owner = self.normalize_owner(owner);

                let mut outcome = ApplicationCallOutcome::default();
                let balance = self.runtime.owner_balance(owner);
                outcome.value = FungibleResponse::Balance(balance);
                Ok(outcome)
            }

            ApplicationCall::Transfer {
                owner,
                amount,
                destination,
            } => {
                self.check_account_authentication(owner)?;
                let account_owner = owner;
                let owner = self.normalize_owner(owner);

                let target_account = self.normalize_account(destination);

                self.runtime.transfer(Some(owner), target_account, amount);
                let execution_outcome =
                    self.get_transfer_outcome(account_owner, destination, amount);
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
                self.check_account_authentication(source_account.owner)?;

                let fungible_source_account = source_account;
                let fungible_target_account = target_account;

                let source_account = self.normalize_account(source_account);
                let target_account = self.normalize_account(target_account);

                self.runtime.claim(source_account, target_account, amount);
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
}

impl NativeFungibleTokenContract {
    fn get_transfer_outcome(
        &mut self,
        source: AccountOwner,
        target: fungible::Account,
        amount: Amount,
    ) -> ExecutionOutcome<Message> {
        if target.chain_id == self.runtime.chain_id() {
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
        &mut self,
        source: fungible::Account,
        target: fungible::Account,
        amount: Amount,
    ) -> ExecutionOutcome<Message> {
        if source.chain_id == self.runtime.chain_id() {
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

    /// Verifies that a transfer is authenticated for this local account.
    fn check_account_authentication(&mut self, owner: AccountOwner) -> Result<(), Error> {
        match owner {
            AccountOwner::User(address) => {
                ensure!(
                    self.runtime.authenticated_signer() == Some(address),
                    Error::IncorrectAuthentication
                );
                Ok(())
            }
            AccountOwner::Application(_) => Err(Error::ApplicationsNotSupported),
        }
    }
}

// Dummy ComplexObject implementation, required by the graphql(complex) attribute in state.rs.
#[async_graphql::ComplexObject]
impl NativeFungibleToken {}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Insufficient balance in source account.
    #[error("Source account does not have sufficient balance for transfer")]
    InsufficientBalance(#[from] state::InsufficientBalanceError),

    /// Requested transfer does not have permission on this account.
    #[error("The requested transfer is not correctly authenticated.")]
    IncorrectAuthentication,

    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),

    #[error("Applications not supported yet")]
    ApplicationsNotSupported,
}
