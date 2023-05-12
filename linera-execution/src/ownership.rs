// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{crypto::PublicKey, identifiers::Owner};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents the owner(s) of a chain.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub enum ChainOwnership {
    /// The chain is not active. (No blocks can be created)
    #[default]
    None,
    /// The chain is managed by a single owner.
    Single { owner: Owner, public_key: PublicKey },
    /// The chain is managed by multiple owners.
    Multi { owners: HashMap<Owner, PublicKey> },
}

impl ChainOwnership {
    pub fn single(public_key: PublicKey) -> Self {
        ChainOwnership::Single {
            owner: public_key.into(),
            public_key,
        }
    }

    pub fn multiple(public_keys: impl IntoIterator<Item = PublicKey>) -> Self {
        ChainOwnership::Multi {
            owners: public_keys
                .into_iter()
                .map(|key| (Owner::from(key), key))
                .collect(),
        }
    }

    pub fn is_active(&self) -> bool {
        !matches!(self, ChainOwnership::None)
    }

    pub fn verify_owner(&self, owner: &Owner) -> Option<PublicKey> {
        match self {
            ChainOwnership::Single {
                owner: owner1,
                public_key,
            } => {
                if owner1 == owner {
                    Some(*public_key)
                } else {
                    None
                }
            }
            ChainOwnership::Multi { owners } => owners.get(owner).copied(),
            ChainOwnership::None => None,
        }
    }
}
