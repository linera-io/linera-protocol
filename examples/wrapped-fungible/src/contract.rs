// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{
    linera_base_types::{AccountOwner, WithContractAbi, U128},
    views::{RootView, View},
    Contract, ContractRuntime,
};
use wrapped_fungible::{
    Account, FungibleResponse, InitialState, Message, WrappedFungibleOperation,
    WrappedFungibleTokenAbi, WrappedParameters,
};

use crate::state::WrappedFungibleTokenState;

pub struct WrappedFungibleTokenContract {
    state: WrappedFungibleTokenState,
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
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = WrappedFungibleTokenState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        WrappedFungibleTokenContract { state, runtime }
    }

    async fn instantiate(&mut self, state: Self::InstantiationArgument) {
        self.runtime.application_parameters();
        for (k, v) in state.accounts {
            if v.0 != 0 {
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

            WrappedFungibleOperation::MintAndTransfer {
                target_account,
                amount,
            } => {
                self.require_authorized_chain();
                self.execute_mint_and_transfer(target_account, amount).await;
                FungibleResponse::Ok
            }

            WrappedFungibleOperation::Burn { owner, amount } => {
                self.require_authorized_chain();
                self.state.debit(owner, amount).await;
                FungibleResponse::Ok
            }

            WrappedFungibleOperation::RegisterAuthorizedCaller { app_id } => {
                self.runtime
                    .authenticated_owner()
                    .expect("RegisterAuthorizedCaller requires an authenticated signer");
                // The authorized caller is only consulted by `MintAndTransfer`/`Burn`,
                // which run only on the mint chain. Restrict registration to that
                // chain so it cannot be set (even inertly) on user-owned chains.
                let mint_chain_id = self.runtime.application_parameters().mint_chain_id;
                assert!(
                    self.runtime.chain_id() == mint_chain_id,
                    "the authorized caller may only be registered on the mint chain"
                );
                self.state.authorized_caller_id.set(Some(app_id));
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
                    .expect("Delivery status is available when executing a message");
                // A bouncing Credit returns funds to the original `source`
                // (e.g. when a driven burn is rejected and the funding transfer
                // bounces back). Otherwise credit the `target`.
                if is_bouncing {
                    self.state.credit(source, amount).await;
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

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl WrappedFungibleTokenContract {
    /// Enforces the authorization shared by `MintAndTransfer` and `Burn`: the
    /// cross-application caller must be the registered authorized caller (set via
    /// `RegisterAuthorizedCaller`), and the operation must run on the designated
    /// `mint_chain_id`. Both are **mandatory**: `MintAndTransfer` and `Burn` may be
    /// driven only by the authorized caller, so a wrapped token with no caller
    /// registered — or no mint chain configured — must never change supply.
    /// Requiring registration also removes any setup window in which an impostor
    /// application could mint or burn.
    ///
    /// There is intentionally no signer check: the authorized caller is the sole
    /// caller and is trusted to act only on its own validated input, so the
    /// authenticated signer — whoever relayed the request — is irrelevant to
    /// safety. Omitting it makes relaying permissionless.
    fn require_authorized_chain(&mut self) {
        let authorized_caller_id = (*self.state.authorized_caller_id.get())
            .expect("authorized caller not registered — call RegisterAuthorizedCaller first");
        let caller = self.runtime.authenticated_caller_id();
        assert!(
            caller == Some(authorized_caller_id),
            "only the authorized caller may mint or burn"
        );
        let mint_chain_id = self.runtime.application_parameters().mint_chain_id;
        assert!(
            self.runtime.chain_id() == mint_chain_id,
            "minting and burning are only allowed on the designated mint chain"
        );
    }

    async fn execute_mint_and_transfer(&mut self, target_account: Account, amount: U128) {
        if target_account.chain_id == self.runtime.chain_id() {
            self.state.credit(target_account.owner, amount).await;
        } else {
            let source = self
                .runtime
                .authenticated_owner()
                .unwrap_or(AccountOwner::CHAIN);
            self.runtime
                .prepare_message(Message::Credit {
                    target: target_account.owner,
                    amount,
                    source,
                })
                .with_authentication()
                .with_tracking()
                .send_to(target_account.chain_id);
        }
    }

    async fn claim(&mut self, source_account: Account, amount: U128, target_account: Account) {
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
        amount: U128,
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
