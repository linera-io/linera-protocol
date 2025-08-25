// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use linera_sdk::{
    abis::fungible::{
        FungibleResponse, InitialState, NativeFungibleOperation, NativeFungibleTokenAbi, Parameters,
    },
    linera_base_types::{Account, AccountOwner, ChainId, WithContractAbi},
    Contract, ContractRuntime,
};
use native_fungible::{Message, TICKER_SYMBOL};

pub struct NativeFungibleTokenContract {
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(NativeFungibleTokenContract);

impl WithContractAbi for NativeFungibleTokenContract {
    type Abi = NativeFungibleTokenAbi;
}

impl Contract for NativeFungibleTokenContract {
    type Message = Message;
    type Parameters = Parameters;
    type InstantiationArgument = InitialState;
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        NativeFungibleTokenContract { runtime }
    }

    async fn instantiate(&mut self, state: Self::InstantiationArgument) {
        // Validate that the application parameters were configured correctly.
        assert!(
            self.runtime.application_parameters().ticker_symbol == "NAT",
            "Only NAT is accepted as ticker symbol"
        );
        for (owner, amount) in state.accounts {
            let account = Account {
                chain_id: self.runtime.chain_id(),
                owner,
            };
            self.runtime.transfer(AccountOwner::CHAIN, account, amount);
        }
    }

    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response {
        match operation {
            NativeFungibleOperation::Balance { owner } => {
                let balance = self.runtime.owner_balance(owner);
                FungibleResponse::Balance(balance)
            }

            NativeFungibleOperation::TickerSymbol => {
                FungibleResponse::TickerSymbol(String::from(TICKER_SYMBOL))
            }

            NativeFungibleOperation::Transfer {
                owner,
                amount,
                target_account,
            } => {
                self.runtime
                    .check_account_permission(owner)
                    .expect("Permission for Transfer operation");

                let fungible_target_account = target_account;
                let target_account = self.normalize_account(target_account);

                self.runtime.transfer(owner, target_account, amount);

                self.transfer(fungible_target_account.chain_id);
                FungibleResponse::Ok
            }

            NativeFungibleOperation::Claim {
                source_account,
                amount,
                target_account,
            } => {
                self.runtime
                    .check_account_permission(source_account.owner)
                    .expect("Permission for Claim operation");

                let fungible_source_account = source_account;
                let fungible_target_account = target_account;

                let source_account = self.normalize_account(source_account);
                let target_account = self.normalize_account(target_account);

                self.runtime.claim(source_account, target_account, amount);
                self.claim(
                    fungible_source_account.chain_id,
                    fungible_target_account.chain_id,
                );
                FungibleResponse::Ok
            }
        }
    }

    async fn execute_message(&mut self, message: Self::Message) {
        // Messages for now don't do anything, just pass messages around
        match message {
            Message::Notify => (),
        }
    }

    async fn store(self) {}
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

    fn normalize_account(&self, account: Account) -> Account {
        Account {
            chain_id: account.chain_id,
            owner: account.owner,
        }
    }
}
