// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use fungible::{state::FungibleTokenState, FungibleResponse, InitialState};
use linera_sdk::{
    linera_base_types::{AccountOwner, Amount, StreamName, WithContractAbi},
    views::{RootView, View},
    Contract, ContractRuntime,
};
use wrapped_fungible::{
    Account, BurnEvent, Message, WrappedFungibleOperation, WrappedFungibleTokenAbi,
    WrappedParameters,
};

pub struct WrappedFungibleTokenContract {
    state: FungibleTokenState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(WrappedFungibleTokenContract);

impl WithContractAbi for WrappedFungibleTokenContract {
    type Abi = WrappedFungibleTokenAbi;
}

impl Contract for WrappedFungibleTokenContract {
    type Message = Message;
    type Parameters = WrappedParameters;
    type InstantiationArgument = InitialState;
    type EventValue = BurnEvent;

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = FungibleTokenState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        WrappedFungibleTokenContract { state, runtime }
    }

    async fn instantiate(&mut self, state: Self::InstantiationArgument) {
        self.runtime.application_parameters();
        for (k, v) in state.accounts {
            if v != Amount::ZERO {
                self.state.credit(k, v).await;
            }
        }
    }

    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response {
        match operation {
            WrappedFungibleOperation::Balance { owner } => {
                let balance = self.state.balance_or_default(&owner).await;
                FungibleResponse::Balance(balance)
            }

            WrappedFungibleOperation::TickerSymbol => {
                let params: WrappedParameters = self.runtime.application_parameters();
                FungibleResponse::TickerSymbol(params.ticker_symbol)
            }

            WrappedFungibleOperation::Approve {
                owner,
                spender,
                allowance,
            } => {
                self.runtime
                    .check_account_permission(owner)
                    .expect("Permission for Approve operation");
                self.state.approve(owner, spender, allowance).await;
                FungibleResponse::Ok
            }

            WrappedFungibleOperation::Transfer {
                owner,
                amount,
                target_account,
            } => {
                self.runtime
                    .check_account_permission(owner)
                    .expect("Permission for Transfer operation");
                self.state.debit(owner, amount).await;
                self.finish_transfer_to_account(amount, target_account, owner)
                    .await;
                FungibleResponse::Ok
            }

            WrappedFungibleOperation::TransferFrom {
                owner,
                spender,
                amount,
                target_account,
            } => {
                self.runtime
                    .check_account_permission(spender)
                    .expect("Permission for TransferFrom operation");
                self.state
                    .debit_for_transfer_from(owner, spender, amount)
                    .await;
                self.finish_transfer_to_account(amount, target_account, owner)
                    .await;
                FungibleResponse::Ok
            }

            WrappedFungibleOperation::Claim {
                source_account,
                amount,
                target_account,
            } => {
                self.runtime
                    .check_account_permission(source_account.owner)
                    .expect("Permission for Claim operation");
                self.claim(source_account, amount, target_account).await;
                FungibleResponse::Ok
            }

            WrappedFungibleOperation::Mint {
                target_account,
                amount,
            } => self.execute_mint(target_account, amount).await,
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
                    .expect("Delivery status is available when executing a message");
                let on_mint_chain =
                    self.runtime.chain_id() == self.runtime.application_parameters().mint_chain_id;
                if is_bouncing {
                    self.state.credit(source, amount).await;
                } else if let (true, AccountOwner::Address20(addr)) = (on_mint_chain, target) {
                    self.runtime.emit(
                        StreamName::from("burns"),
                        &BurnEvent {
                            target: addr,
                            amount,
                        },
                    );
                } else {
                    self.state.credit(target, amount).await;
                }
            }
            Message::Withdraw {
                owner,
                amount,
                target_account,
            } => {
                self.runtime
                    .check_account_permission(owner)
                    .expect("Permission for Withdraw message");
                self.state.debit(owner, amount).await;
                self.finish_transfer_to_account(amount, target_account, owner)
                    .await;
            }
        }
    }

    async fn store(self) {
        self.state
            .save_and_drop()
            .await
            .expect("Failed to save state");
    }
}

impl WrappedFungibleTokenContract {
    /// Checks that the authenticated signer is the authorized minter and
    /// that the operation is on the designated mint chain.
    fn require_minter(&mut self) -> AccountOwner {
        let signer = self
            .runtime
            .authenticated_signer()
            .expect("Mint/Burn requires an authenticated signer");
        let params: WrappedParameters = self.runtime.application_parameters();
        assert!(
            signer == params.minter,
            "unauthorized: only the minter can perform this operation"
        );
        assert!(
            self.runtime.chain_id() == params.mint_chain_id,
            "Mint/Burn operations are only allowed on the designated mint chain"
        );
        signer
    }

    /// Mints tokens to a target account (local or remote).
    async fn execute_mint(&mut self, target_account: Account, amount: Amount) -> FungibleResponse {
        let signer = self.require_minter();
        if target_account.chain_id == self.runtime.chain_id() {
            self.state.credit(target_account.owner, amount).await;
        } else {
            self.runtime
                .prepare_message(Message::Credit {
                    target: target_account.owner,
                    amount,
                    source: signer,
                })
                .with_authentication()
                .with_tracking()
                .send_to(target_account.chain_id);
        }
        FungibleResponse::Ok
    }

    async fn claim(&mut self, source_account: Account, amount: Amount, target_account: Account) {
        if source_account.chain_id == self.runtime.chain_id() {
            self.state.debit(source_account.owner, amount).await;
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
    }

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
