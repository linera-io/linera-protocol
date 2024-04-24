// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::str::FromStr;

use async_trait::async_trait;
use fungible::{
    Account, FungibleResponse, FungibleTokenAbi, InitialState, Message, Operation, Parameters,
};
use linera_sdk::{
    base::{AccountOwner, Amount, WithContractAbi},
    ensure, Contract, ContractRuntime, ViewStateStorage,
};
use thiserror::Error;

use self::state::FungibleToken;

pub struct FungibleTokenContract {
    state: FungibleToken,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(FungibleTokenContract);

impl WithContractAbi for FungibleTokenContract {
    type Abi = FungibleTokenAbi;
}

#[async_trait]
impl Contract for FungibleTokenContract {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;
    type State = FungibleToken;
    type Message = Message;
    type Parameters = Parameters;
    type InstantiationArgument = InitialState;

    async fn new(
        state: FungibleToken,
        runtime: ContractRuntime<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(FungibleTokenContract { state, runtime })
    }

    fn state_mut(&mut self) -> &mut Self::State {
        &mut self.state
    }

    async fn instantiate(
        &mut self,
        mut state: Self::InstantiationArgument,
    ) -> Result<(), Self::Error> {
        // Validate that the application parameters were configured correctly.
        let _ = self.runtime.application_parameters();

        // If initial accounts are empty, creator gets 1M tokens to act like a faucet.
        if state.accounts.is_empty() {
            if let Some(owner) = self.runtime.authenticated_signer() {
                state.accounts.insert(
                    AccountOwner::User(owner),
                    Amount::from_str("1000000").unwrap(),
                );
            }
        }
        self.state.initialize_accounts(state).await;

        Ok(())
    }

    async fn execute_operation(
        &mut self,
        operation: Self::Operation,
    ) -> Result<Self::Response, Self::Error> {
        match operation {
            Operation::Balance { owner } => {
                let balance = self.state.balance_or_default(&owner).await;
                Ok(FungibleResponse::Balance(balance))
            }

            Operation::TickerSymbol => {
                let params = self.runtime.application_parameters();
                Ok(FungibleResponse::TickerSymbol(params.ticker_symbol))
            }

            Operation::Transfer {
                owner,
                amount,
                target_account,
            } => {
                self.check_account_authentication(owner)?;
                self.state.debit(owner, amount).await?;
                self.finish_transfer_to_account(amount, target_account, owner)
                    .await;
                Ok(FungibleResponse::Ok)
            }

            Operation::Claim {
                source_account,
                amount,
                target_account,
            } => {
                self.check_account_authentication(source_account.owner)?;
                self.claim(source_account, amount, target_account).await?;
                Ok(FungibleResponse::Ok)
            }
        }
    }

    async fn execute_message(&mut self, message: Message) -> Result<(), Self::Error> {
        match message {
            Message::Credit {
                amount,
                target,
                source,
            } => {
                let is_bouncing = self
                    .runtime
                    .message_is_bouncing()
                    .expect("Message delivery status has to be available when executing a message");
                let receiver = if is_bouncing { source } else { target };
                self.state.credit(receiver, amount).await;
            }
            Message::Withdraw {
                owner,
                amount,
                target_account,
            } => {
                self.check_account_authentication(owner)?;
                self.state.debit(owner, amount).await?;
                self.finish_transfer_to_account(amount, target_account, owner)
                    .await;
            }
        }

        Ok(())
    }
}

impl FungibleTokenContract {
    /// Verifies that a transfer is authenticated for this local account.
    fn check_account_authentication(&mut self, owner: AccountOwner) -> Result<(), Error> {
        match owner {
            AccountOwner::User(address) => {
                ensure!(
                    self.runtime.authenticated_signer() == Some(address),
                    Error::IncorrectAuthentication
                )
            }
            AccountOwner::Application(id) => {
                ensure!(
                    self.runtime.authenticated_caller_id() == Some(id),
                    Error::IncorrectAuthentication
                )
            }
        }

        Ok(())
    }

    async fn claim(
        &mut self,
        source_account: Account,
        amount: Amount,
        target_account: Account,
    ) -> Result<(), Error> {
        if source_account.chain_id == self.runtime.chain_id() {
            self.state.debit(source_account.owner, amount).await?;
            self.finish_transfer_to_account(amount, target_account, source_account.owner)
                .await;
        } else {
            let message = Message::Withdraw {
                owner: source_account.owner,
                amount,
                target_account,
            };
            self.runtime
                .prepare_message(message)
                .with_authentication()
                .send_to(source_account.chain_id);
        }

        Ok(())
    }

    /// Executes the final step of a transfer where the tokens are sent to the destination.
    async fn finish_transfer_to_account(
        &mut self,
        amount: Amount,
        target_account: Account,
        source: AccountOwner,
    ) {
        if target_account.chain_id == self.runtime.chain_id() {
            self.state.credit(target_account.owner, amount).await;
        } else {
            let message = Message::Credit {
                target: target_account.owner,
                amount,
                source,
            };
            self.runtime
                .prepare_message(message)
                .with_authentication()
                .with_tracking()
                .send_to(target_account.chain_id);
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

    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),
}
