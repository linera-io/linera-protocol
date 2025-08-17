// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use fungible::{
    FungibleOperation, FungibleResponse, FungibleTokenAbi, InitialState, Message, Parameters,
};
use linera_sdk::{
    linera_base_types::{Account, AccountOwner, Amount, WithContractAbi},
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
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = FungibleTokenState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        FungibleTokenContract { state, runtime }
    }

    async fn instantiate(&mut self, state: Self::InstantiationArgument) {
        // Validate that the application parameters were configured correctly.
        let _ = self.runtime.application_parameters();

        let mut total_supply = Amount::ZERO;
        for value in state.accounts.values() {
            total_supply.saturating_add_assign(*value);
        }
        if total_supply == Amount::ZERO {
            panic!("The total supply is zero, therefore we cannot instantiate the contract");
        }
        self.state.initialize_accounts(state).await;
    }

    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response {
        match operation {
            FungibleOperation::Balance { owner } => {
                let balance = self.state.balance_or_default(&owner).await;
                FungibleResponse::Balance(balance)
            }

            FungibleOperation::TickerSymbol => {
                let params = self.runtime.application_parameters();
                FungibleResponse::TickerSymbol(params.ticker_symbol)
            }

            FungibleOperation::Approve {
                owner,
                spender,
                allowance,
            } => {
                self.runtime
                    .check_account_permission(owner)
                    .expect("Permission for Transfer operation");
                self.state.approve(owner, spender, allowance).await;
                FungibleResponse::Ok
            }

            FungibleOperation::Transfer {
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

            FungibleOperation::TransferFrom {
                owner,
                spender,
                amount,
                target_account,
            } => {
                self.runtime
                    .check_account_permission(spender)
                    .expect("Permission for Transfer operation");
                self.state
                    .debit_for_transfer_from(owner, spender, amount)
                    .await;
                self.finish_transfer_to_account(amount, target_account, owner)
                    .await;
                FungibleResponse::Ok
            }

            FungibleOperation::Claim {
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

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl FungibleTokenContract {
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
