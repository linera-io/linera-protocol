// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use fungible::{FungibleResponse, FungibleTokenAbi, InitialState, Operation, Parameters};
use linera_sdk::{
    base::{Account, AccountOwner, ChainId, WithContractAbi},
    Contract, ContractRuntime,
};
use native_fungible::{Message, TICKER_SYMBOL};

pub struct NativeFungibleTokenContract {
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(NativeFungibleTokenContract);

impl WithContractAbi for NativeFungibleTokenContract {
    type Abi = FungibleTokenAbi;
}

impl Contract for NativeFungibleTokenContract {
    type Message = Message;
    type Parameters = Parameters;
    type InstantiationArgument = InitialState;

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
                owner: Some(owner),
            };
            self.runtime.transfer(None, account, amount);
        }
    }

    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response {
        match operation {
            Operation::Balance { owner } => {
                let balance = self.runtime.owner_balance(owner);
                FungibleResponse::Balance(balance)
            }

            Operation::TickerSymbol => FungibleResponse::TickerSymbol(String::from(TICKER_SYMBOL)),

            Operation::Transfer {
                owner,
                amount,
                target_account,
            } => {
                self.check_account_authentication(owner);

                let fungible_target_account = target_account;
                let target_account = self.normalize_account(target_account);

                self.runtime.transfer(Some(owner), target_account, amount);

                self.transfer(fungible_target_account.chain_id);
                FungibleResponse::Ok
            }

            Operation::Claim {
                source_account,
                amount,
                target_account,
            } => {
                self.check_account_authentication(source_account.owner);

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

    fn normalize_account(&self, account: fungible::Account) -> Account {
        Account {
            chain_id: account.chain_id,
            owner: Some(account.owner),
        }
    }

    /// Verifies that a transfer is authenticated for this local account.
    fn check_account_authentication(&mut self, owner: AccountOwner) {
        match owner {
            AccountOwner::User(address) => {
                assert_eq!(
                    self.runtime.authenticated_signer(),
                    Some(address),
                    "The requested transfer is not correctly authenticated."
                );
            }
            AccountOwner::Application(_) => panic!("Applications not supported yet"),
        }
    }
}
