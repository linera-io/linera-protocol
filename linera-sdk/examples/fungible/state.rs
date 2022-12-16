// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::{crypto::PublicKey, ensure, ApplicationId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

/// The application state.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct FungibleToken {
    accounts: BTreeMap<AccountOwner, u128>,
    nonces: BTreeMap<AccountOwner, Nonce>,
}

/// An account owner.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum AccountOwner {
    /// An account protected by a private key.
    Key(PublicKey),
    /// An account for an application.
    Application(ApplicationId),
}

/// A single-use number to prevent replay attacks.
#[derive(Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Nonce(u64);

impl Nonce {
    /// Obtain the next number to use, if there is one.
    fn next(&self) -> Option<Self> {
        let next_number = self.0.checked_add(1)?;

        Some(Nonce(next_number))
    }
}

#[allow(dead_code)]
impl FungibleToken {
    /// Initialize the application state with some accounts with initial balances.
    pub(crate) fn initialize_accounts(&mut self, accounts: BTreeMap<AccountOwner, u128>) {
        self.accounts = accounts;
    }

    /// Obtain the balance for an `account`.
    pub(crate) fn balance(&self, account: &AccountOwner) -> u128 {
        self.accounts.get(&account).copied().unwrap_or(0)
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
            .unwrap_or(Some(Nonce::default()))
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

/// Alias to the application type, so that the boilerplate module can reference it.
pub type ApplicationState = FungibleToken;
