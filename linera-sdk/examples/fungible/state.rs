// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::{crypto::PublicKey, ApplicationId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

#[allow(dead_code)]
impl FungibleToken {
    /// Obtain the balance for an `account`.
    pub(crate) fn balance(&self, account: &AccountOwner) -> u128 {
        self.accounts.get(&account).copied().unwrap_or(0)
    }
}

/// Alias to the application type, so that the boilerplate module can reference it.
pub type ApplicationState = FungibleToken;
