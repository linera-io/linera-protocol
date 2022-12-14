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
