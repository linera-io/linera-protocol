// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fungible::InitialState;
use linera_sdk::{
    base::{AccountOwner, Amount},
    views::{linera_views, MapView, RootView, ViewError, ViewStorageContext},
};
use thiserror::Error;

/// The application state.
#[derive(RootView, async_graphql::SimpleObject)]
// In the service we also want a tickerSymbol query, which is not derived from a struct member.
// This attribute requires having a ComplexObject implementation that adds such fields.
// The implementation with tickerSymbol is in service.rs. Since a ComplexObject impl is required,
// there is also an empty one in contract.rs.
#[graphql(complex)]
#[view(context = "ViewStorageContext")]
pub struct FungibleToken {
    accounts: MapView<AccountOwner, Amount>,
}

#[allow(dead_code)]
impl FungibleToken {
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
    pub(crate) async fn debit(
        &mut self,
        account: AccountOwner,
        amount: Amount,
    ) -> Result<(), InsufficientBalanceError> {
        let mut balance = self.balance_or_default(&account).await;
        balance
            .try_sub_assign(amount)
            .map_err(|_| InsufficientBalanceError)?;
        if balance == Amount::ZERO {
            match self.accounts.remove(&account) {
                Ok(()) | Err(ViewError::NotFound(_)) => {}
                Err(error) => panic!("Failed to remove empty account: {}", error),
            }
        } else {
            self.accounts
                .insert(&account, balance)
                .expect("Failed insertion operation");
        }
        Ok(())
    }
}

/// Attempts to debit from an account with insufficient funds.
#[derive(Clone, Copy, Debug, Error)]
#[error("Insufficient balance for transfer")]
pub struct InsufficientBalanceError;
