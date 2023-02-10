// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use simple_fungible::types::{AccountOwner, Nonce};
use linera_sdk::ensure;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

/// The application state.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SimpleFungibleToken {
    accounts: BTreeMap<AccountOwner, u128>,
    nonces: BTreeMap<AccountOwner, Nonce>,
}

#[allow(dead_code)]
impl SimpleFungibleToken {
    /// Initialize the application state with some accounts with initial balances.
    pub(crate) fn initialize_accounts(&mut self, accounts: BTreeMap<AccountOwner, u128>) {
        self.accounts = accounts;
    }

    /// Obtain the balance for an `account`.
    pub(crate) fn balance(&self, account: &AccountOwner) -> u128 {
        self.accounts.get(account).copied().unwrap_or(0)
    }

    /// Credit an `account` with the provided `amount`.
    pub(crate) fn credit(&mut self, account: AccountOwner, amount: u128) {
        *self.accounts.entry(account).or_default() += amount;
    }

    /// Try to debit the requested `amount` from an `account`.
    pub(crate) fn debit(
        &mut self,
        account: AccountOwner,
        amount: u128,
    ) -> Result<(), InsufficientBalanceError> {
        let balance = self
            .accounts
            .get_mut(&account)
            .ok_or(InsufficientBalanceError)?;

        ensure!(*balance >= amount, InsufficientBalanceError);

        *balance -= amount;

        Ok(())
    }

    /// Obtain the minimum allowed [`Nonce`] for an `account`.
    ///
    /// The minimum allowed nonce is the value of the previously used nonce plus one, or zero if
    /// this is the first transaction for the `account` on the current chain.
    ///
    /// If the increment to obtain the next nonce overflows, `None` is returned.
    pub(crate) fn minimum_nonce(&self, account: &AccountOwner) -> Option<Nonce> {
        self.nonces
            .get(account)
            .map(Nonce::next)
            .unwrap_or_else(|| Some(Nonce::default()))
    }

    /// Mark the provided [`Nonce`] as used for the `account`.
    pub(crate) fn mark_nonce_as_used(&mut self, account: AccountOwner, nonce: Nonce) {
        let minimum_nonce = self.minimum_nonce(&account);

        assert!(minimum_nonce.is_some());
        assert!(nonce >= minimum_nonce.unwrap());

        self.nonces.insert(account, nonce);
    }
}

/// Attempt to debit from an account with insufficient funds.
#[derive(Clone, Copy, Debug, Error)]
#[error("Insufficient balance for transfer")]
pub struct InsufficientBalanceError;
