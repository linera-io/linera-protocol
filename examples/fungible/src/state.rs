// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fungible::{InitialState, OwnerSpender};
use linera_sdk::{
    linera_base_types::{AccountOwner, Amount},
    views::{linera_views, MapView, RootView, ViewStorageContext},
};

/// The application state.
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct FungibleTokenState {
    pub accounts: MapView<AccountOwner, Amount>,
    pub allowances: MapView<OwnerSpender, Amount>,
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
    pub(crate) async fn approve(
        &mut self,
        owner: AccountOwner,
        spender: AccountOwner,
        allowance: Amount,
    ) {
        if allowance == Amount::ZERO {
            return;
        }
        let owner_spender = OwnerSpender::new(owner, spender);
        let total_allowance = self
            .allowances
            .get_mut_or_default(&owner_spender)
            .await
            .expect("Failed allowance access");
        total_allowance.saturating_add_assign(allowance);
    }

    pub(crate) async fn debit_for_transfer_from(
        &mut self,
        owner: AccountOwner,
        spender: AccountOwner,
        amount: Amount,
    ) {
        if amount == Amount::ZERO {
            return;
        }
        let mut balance = self
            .accounts
            .get(&owner)
            .await
            .expect("Failed balance access")
            .unwrap_or_default();
        balance.try_sub_assign(amount).unwrap_or_else(|_| {
            panic!("Source owner {owner} does not have sufficient balance for transfer_from")
        });
        if balance == Amount::ZERO {
            self.accounts
                .remove(&owner)
                .expect("Failed to remove an empty account");
        } else {
            self.accounts
                .insert(&owner, balance)
                .expect("Failed insertion operation");
        }
        let owner_spender = OwnerSpender::new(owner, spender);
        let mut allowance = self
            .allowances
            .get(&owner_spender)
            .await
            .expect("Failed allowance access")
            .unwrap_or_default();
        allowance.try_sub_assign(amount).unwrap_or_else(|_| {
            panic!("Spender {spender} does not have a sufficient from owner {owner} for transfer_from; allowance={allowance} amount={amount}")
        });
        if allowance == Amount::ZERO {
            self.allowances
                .remove(&owner_spender)
                .expect("Failed to remove an empty account");
        } else {
            self.allowances
                .insert(&owner_spender, allowance)
                .expect("Failed insertion operation");
        }
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
        balance.try_sub_assign(amount).unwrap_or_else(|_| {
            panic!("Source account {account} does not have sufficient balance for transfer")
        });
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
