// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fungible::AccountOwner;
use linera_sdk::base::Amount;
use linera_views::{
    common::Context,
    map_view::MapView,
    views::{GraphQLView, RootView, View},
};
use std::collections::BTreeMap;
use thiserror::Error;

/// The application state.
#[derive(RootView, GraphQLView)]
pub struct FungibleToken<C> {
    accounts: MapView<C, AccountOwner, Amount>,
}

#[allow(dead_code)]
impl<C> FungibleToken<C>
where
    C: Context + Send + Sync + Clone + 'static,
    linera_views::views::ViewError: From<C::Error>,
{
    /// Initializes the application state with some accounts with initial balances.
    pub(crate) async fn initialize_accounts(&mut self, accounts: BTreeMap<AccountOwner, Amount>) {
        for (k, v) in accounts {
            self.accounts
                .insert(&k, v)
                .expect("Error in insert statement");
        }
    }

    /// Obtains the balance for an `account`.
    pub(crate) async fn balance(&self, account: &AccountOwner) -> Amount {
        self.accounts
            .get(account)
            .await
            .expect("Failure in the retrieval")
            .unwrap_or_default()
    }

    /// Credits an `account` with the provided `amount`.
    pub(crate) async fn credit(&mut self, account: AccountOwner, amount: Amount) {
        let mut value = self.balance(&account).await;
        value.saturating_add_assign(amount);
        self.accounts
            .insert(&account, value)
            .expect("Failed insert statement");
    }

    /// Tries to debit the requested `amount` from an `account`.
    pub(crate) async fn debit(
        &mut self,
        account: AccountOwner,
        amount: Amount,
    ) -> Result<(), InsufficientBalanceError> {
        let mut balance = self.balance(&account).await;
        balance
            .try_sub_assign(amount)
            .map_err(|_| InsufficientBalanceError)?;
        self.accounts
            .insert(&account, balance)
            .expect("Failed insertion operation");
        Ok(())
    }
}

/// Attempts to debit from an account with insufficient funds.
#[derive(Clone, Copy, Debug, Error)]
#[error("Insufficient balance for transfer")]
pub struct InsufficientBalanceError;
