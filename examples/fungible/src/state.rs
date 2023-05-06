// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fungible::{AccountOwner, InitialState};
use linera_sdk::base::Amount;
use linera_views::{
    common::Context,
    map_view::MapView,
    views::{GraphQLView, RootView, View},
};
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

    pub(crate) async fn initialize_accounts(&mut self, state: InitialState) {
        for (k, v) in state.accounts {
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

    pub(crate) async fn total_balance(&self) -> Amount {
        let mut total_balance = Amount::default();
        self.accounts.for_each_index_value(|_key, value| {
            total_balance.try_add_assign(value).expect("Arithmetic error");
            Ok(())
	}).await.expect("Failure to retrieve the total_balance");
        total_balance
    }

    /// Credits an `account` with the provided `amount`.
    pub(crate) async fn credit(&mut self, account: AccountOwner, amount: Amount) {
        let mut balance = self.balance(&account).await;
        balance.saturating_add_assign(amount);
        self.accounts
            .insert(&account, balance)
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
