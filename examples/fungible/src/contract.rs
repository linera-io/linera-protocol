// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::str::FromStr;

use fungible::{
    Account, FungibleResponse, FungibleTokenAbi, InitialState, Message, Operation, Parameters,
};
use linera_sdk::{
    linera_base_types::{Address, Amount, WithContractAbi},
    views::{RootView, View},
    Contract, ContractRuntime,
};

use self::state::FungibleTokenState;

pub struct FungibleTokenContract {
    state: FungibleTokenState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(FungibleTokenContract);

impl WithContractAbi for FungibleTokenContract {
    type Abi = FungibleTokenAbi;
}

impl Contract for FungibleTokenContract {
    type Message = Message;
    type Parameters = Parameters;
    type InstantiationArgument = InitialState;

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = FungibleTokenState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        FungibleTokenContract { state, runtime }
    }

    async fn instantiate(&mut self, mut state: Self::InstantiationArgument) {
        // Validate that the application parameters were configured correctly.
        let _ = self.runtime.application_parameters();

        // If initial accounts are empty, creator gets 1M tokens to act like a faucet.
        if state.accounts.is_empty() {
            if let Some(owner) = self.runtime.authenticated_signer() {
                state
                    .accounts
                    .insert(owner, Amount::from_str("1000000").unwrap());
            }
        }
        self.state.initialize_accounts(state).await;
    }

    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response {
        match operation {
            Operation::Balance { address } => {
                let balance = self.state.balance_or_default(&address).await;
                FungibleResponse::Balance(balance)
            }

            Operation::TickerSymbol => {
                let params = self.runtime.application_parameters();
                FungibleResponse::TickerSymbol(params.ticker_symbol)
            }

            Operation::Transfer {
                source: address,
                amount,
                target_account,
            } => {
                self.check_account_authentication(address);
                self.state.debit(address, amount).await;
                self.finish_transfer_to_account(amount, target_account, address)
                    .await;
                FungibleResponse::Ok
            }

            Operation::Claim {
                source_account,
                amount,
                target_account,
            } => {
                self.check_account_authentication(source_account.address);
                self.claim(source_account, amount, target_account).await;
                FungibleResponse::Ok
            }
        }
    }

    async fn execute_message(&mut self, message: Message) {
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
                address,
                amount,
                target_account,
            } => {
                self.check_account_authentication(address);
                self.state.debit(address, amount).await;
                self.finish_transfer_to_account(amount, target_account, address)
                    .await;
            }
        }
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl FungibleTokenContract {
    /// Verifies that a transfer is authenticated for this local account.
    fn check_account_authentication(&mut self, address: Address) {
        assert!(
            self.runtime.authenticated_signer() == Some(address)
                || self.runtime.authenticated_caller_id() == Some(address),
            "The requested transfer is not correctly authenticated."
        )
    }

    async fn claim(&mut self, source_account: Account, amount: Amount, target_account: Account) {
        if source_account.chain_id == self.runtime.chain_id() {
            self.state.debit(source_account.address, amount).await;
            self.finish_transfer_to_account(amount, target_account, source_account.address)
                .await;
        } else {
            let message = Message::Withdraw {
                address: source_account.address,
                amount,
                target_account,
            };
            self.runtime
                .prepare_message(message)
                .with_authentication()
                .send_to(source_account.chain_id);
        }
    }

    /// Executes the final step of a transfer where the tokens are sent to the destination.
    async fn finish_transfer_to_account(
        &mut self,
        amount: Amount,
        target_account: Account,
        source: Address,
    ) {
        if target_account.chain_id == self.runtime.chain_id() {
            self.state.credit(target_account.address, amount).await;
        } else {
            let message = Message::Credit {
                target: target_account.address,
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
