// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::{crypto::PublicKey, ApplicationId};
use serde::{Deserialize, Serialize};

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
    pub fn next(&self) -> Option<Self> {
        let next_number = self.0.checked_add(1)?;

        Some(Nonce(next_number))
    }
}
