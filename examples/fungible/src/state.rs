// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fungible::InitialState;
use linera_sdk::{
    base::{AccountOwner, Amount},
    views::{linera_views, MapView, RootView, ViewStorageContext},
};

/// The application state.
#[derive(RootView)]
#[view(context = "ViewStorageContext")]
pub struct FungibleTokenState {
    pub accounts: MapView<AccountOwner, Amount>,
}

#[allow(dead_code)]
impl FungibleTokenState {
    /// Initializes the application state with some accounts with initial balances.
    pub(crate) async fn initialize_accounts(&mut self, state: InitialState) {
        for (k, v) in state.accounts {
            if v != Amount::ZERO {
                self.accounts
                    .insert(&k, v)
                    .expect("Error in insert statement");
            }
        }
    }

    /// Obtains the balance for an `account`, returning None if there's no entry for the account.
    pub(crate) async fn balance(&self, account: &AccountOwner) -> Option<Amount> {
        self.accounts
            .get(account)
            .await
            .expect("Failure in the retrieval")
    }

    /// Obtains the balance for an `account`.
    pub(crate) async fn balance_or_default(&self, account: &AccountOwner) -> Amount {
        self.balance(account).await.unwrap_or_default()
    }

    /// Credits an `account` with the provided `amount`.
    pub(crate) async fn credit(&mut self, account: AccountOwner, amount: Amount) {
        if amount == Amount::ZERO {
            return;
        }
        let mut balance = self.balance_or_default(&account).await;
        balance.saturating_add_assign(amount);
        self.accounts
            .insert(&account, balance)
            .expect("Failed insert statement");
    }

    /// Tries to debit the requested `amount` from an `account`.
    pub(crate) async fn debit(&mut self, account: AccountOwner, amount: Amount) {
        if amount == Amount::ZERO {
            return;
        }
        let mut balance = self.balance_or_default(&account).await;
        balance
            .try_sub_assign(amount)
            .expect("Source account does not have sufficient balance for transfer");
        if balance == Amount::ZERO {
            self.accounts
                .remove(&account)
                .expect("Failed to remove an empty account");
        } else {
            self.accounts
                .insert(&account, balance)
                .expect("Failed insertion operation");
        }
    }
}
