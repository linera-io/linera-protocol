// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{crypto::PublicKey, data_types::RoundNumber, identifiers::Owner};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Represents the owner(s) of a chain.
#[derive(PartialEq, Eq, Clone, Hash, Debug, Default, Serialize, Deserialize)]
pub enum ChainOwnership {
    /// The chain is not active. (No blocks can be created)
    #[default]
    None,
    /// The chain is managed by a single owner.
    Single { owner: Owner, public_key: PublicKey },
    /// The chain is managed by multiple owners.
    Multi {
        /// The owners, with their weights that determine how often they are round leader.
        public_keys: BTreeMap<Owner, (PublicKey, u64)>,
        /// The number of initial rounds in which all owners are allowed to propose blocks,
        /// i.e. the first round with only a single leader.
        multi_leader_rounds: RoundNumber,
    },
}

impl ChainOwnership {
    pub fn single(public_key: PublicKey) -> Self {
        ChainOwnership::Single {
            owner: public_key.into(),
            public_key,
        }
    }

    pub fn multiple(
        keys_and_weights: impl IntoIterator<Item = (PublicKey, u64)>,
        multi_leader_rounds: RoundNumber,
    ) -> Self {
        ChainOwnership::Multi {
            public_keys: keys_and_weights
                .into_iter()
                .map(|(key, weight)| (Owner::from(key), (key, weight)))
                .collect(),
            multi_leader_rounds,
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
            ChainOwnership::Multi { public_keys, .. } => {
                public_keys.get(owner).map(|(key, _)| *key)
            }
            ChainOwnership::None => None,
        }
    }
}
