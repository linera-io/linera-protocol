// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fungible::AccountOwner;
use linera_sdk::base::Amount;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

/// The application state.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct FungibleToken {
    accounts: BTreeMap<AccountOwner, Amount>,
}

#[allow(dead_code)]
impl FungibleToken {
    /// Initialize the application state with some accounts with initial balances.
    pub(crate) fn initialize_accounts(&mut self, accounts: BTreeMap<AccountOwner, Amount>) {
        self.accounts = accounts;
    }

    /// Obtain the balance for an `account`.
    pub(crate) fn balance(&self, account: &AccountOwner) -> Amount {
        self.accounts.get(account).copied().unwrap_or_default()
    }

    /// Credit an `account` with the provided `amount`.
    pub(crate) fn credit(&mut self, account: AccountOwner, amount: Amount) {
        self.accounts
            .entry(account)
            .or_default()
            .saturating_add_assign(amount)
    }

    /// Try to debit the requested `amount` from an `account`.
    pub(crate) fn debit(
        &mut self,
        account: AccountOwner,
        amount: Amount,
    ) -> Result<(), InsufficientBalanceError> {
        let balance = self
            .accounts
            .get_mut(&account)
            .ok_or(InsufficientBalanceError)?;

        balance
            .try_sub_assign(amount)
            .map_err(|_| InsufficientBalanceError)
    }
}

/// Attempt to debit from an account with insufficient funds.
#[derive(Clone, Copy, Debug, Error)]
#[error("Insufficient balance for transfer")]
pub struct InsufficientBalanceError;
