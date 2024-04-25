// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use fungible::{FungibleResponse, FungibleTokenAbi, InitialState, Operation, Parameters};
use linera_sdk::{
    base::{Account, AccountOwner, ChainId, Owner, WithContractAbi},
    ensure, Contract, ContractRuntime, EmptyState,
};
use native_fungible::{Message, TICKER_SYMBOL};
use thiserror::Error;

pub struct NativeFungibleTokenContract {
    state: EmptyState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(NativeFungibleTokenContract);

impl WithContractAbi for NativeFungibleTokenContract {
    type Abi = FungibleTokenAbi;
}

impl Contract for NativeFungibleTokenContract {
    type Error = Error;
    type State = EmptyState;
    type Message = Message;
    type Parameters = Parameters;
    type InstantiationArgument = InitialState;

    async fn new(state: EmptyState, runtime: ContractRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(NativeFungibleTokenContract { state, runtime })
    }

    fn state_mut(&mut self) -> &mut Self::State {
        &mut self.state
    }

    async fn instantiate(&mut self, state: Self::InstantiationArgument) -> Result<(), Self::Error> {
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
        Ok(())
    }

    async fn execute_operation(
        &mut self,
        operation: Self::Operation,
    ) -> Result<Self::Response, Self::Error> {
        match operation {
            Operation::Balance { owner } => {
                let owner = self.normalize_owner(owner);

                let balance = self.runtime.owner_balance(owner);
                Ok(FungibleResponse::Balance(balance))
            }

            Operation::TickerSymbol => {
                Ok(FungibleResponse::TickerSymbol(String::from(TICKER_SYMBOL)))
            }

            Operation::Transfer {
                owner,
                amount,
                target_account,
            } => {
                self.check_account_authentication(owner)?;
                let owner = self.normalize_owner(owner);

                let fungible_target_account = target_account;
                let target_account = self.normalize_account(target_account);

                self.runtime.transfer(Some(owner), target_account, amount);

                self.transfer(fungible_target_account.chain_id);
                Ok(FungibleResponse::Ok)
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
                self.claim(
                    fungible_source_account.chain_id,
                    fungible_target_account.chain_id,
                );
                Ok(FungibleResponse::Ok)
            }
        }
    }

    async fn execute_message(&mut self, message: Self::Message) -> Result<(), Self::Error> {
        // Messages for now don't do anything, just pass messages around
        match message {
            Message::Notify => Ok(()),
        }
    }
}

impl NativeFungibleTokenContract {
    fn transfer(&mut self, chain_id: ChainId) {
        if chain_id != self.runtime.chain_id() {
            let message = Message::Notify;
            self.runtime
                .prepare_message(message)
                .with_authentication()
                .send_to(chain_id);
        }
    }

    fn claim(&mut self, source_chain_id: ChainId, target_chain_id: ChainId) {
        if source_chain_id == self.runtime.chain_id() {
            self.transfer(target_chain_id);
        } else {
            // If different chain, send notify message so the app gets auto-deployed
            let message = Message::Notify;
            self.runtime
                .prepare_message(message)
                .with_authentication()
                .send_to(source_chain_id);
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
