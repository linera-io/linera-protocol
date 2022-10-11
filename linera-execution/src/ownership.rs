// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::messages::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents the owner(s) of a chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub enum ChainOwnership {
    /// The chain is not active. (No blocks can be created)
    None,
    /// The chain is managed by a single owner.
    Single { owner: Owner },
    /// The chain is managed by multiple owners. Using a map instead a hashset because
    /// Serde/BCS treats HashSet's as vectors.
    Multi { owners: HashMap<Owner, ()> },
}

impl Default for ChainOwnership {
    fn default() -> Self {
        ChainOwnership::None
    }
}

impl ChainOwnership {
    pub fn single(owner: Owner) -> Self {
        ChainOwnership::Single { owner }
    }

    pub fn multiple(owners: Vec<Owner>) -> Self {
        ChainOwnership::Multi {
            owners: owners.into_iter().map(|o| (o, ())).collect(),
        }
    }

    pub fn is_active(&self) -> bool {
        !matches!(self, ChainOwnership::None)
    }

    pub fn has_owner(&self, owner: &Owner) -> bool {
        match self {
            ChainOwnership::Single { owner: owner1 } => owner1 == owner,
            ChainOwnership::Multi { owners } => owners.contains_key(owner),
            ChainOwnership::None => false,
        }
    }
}
