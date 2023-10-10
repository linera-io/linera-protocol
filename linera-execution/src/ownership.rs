// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{crypto::PublicKey, data_types::RoundNumber, identifiers::Owner};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, iter};

/// Represents the owner(s) of a chain.
#[derive(PartialEq, Eq, Clone, Hash, Debug, Default, Serialize, Deserialize)]
pub struct ChainOwnership {
    /// Super owners can propose fast blocks in round 0, and regular blocks in any round.
    pub super_owners: BTreeMap<Owner, PublicKey>,
    /// The regular owners, with their weights that determine how often they are round leader.
    pub owners: BTreeMap<Owner, (PublicKey, u64)>,
    /// The number of initial rounds after 0 in which all owners are allowed to propose blocks.
    pub multi_leader_rounds: RoundNumber,
}

impl ChainOwnership {
    pub fn single(public_key: PublicKey) -> Self {
        ChainOwnership {
            super_owners: iter::once((Owner::from(public_key), public_key)).collect(),
            owners: BTreeMap::new(),
            multi_leader_rounds: RoundNumber(2),
        }
    }

    pub fn multiple(
        keys_and_weights: impl IntoIterator<Item = (PublicKey, u64)>,
        multi_leader_rounds: RoundNumber,
    ) -> Self {
        ChainOwnership {
            super_owners: BTreeMap::new(),
            owners: keys_and_weights
                .into_iter()
                .map(|(public_key, weight)| (Owner::from(public_key), (public_key, weight)))
                .collect(),
            multi_leader_rounds,
        }
    }

    pub fn is_active(&self) -> bool {
        !self.super_owners.is_empty() || !self.owners.is_empty()
    }

    pub fn verify_owner(&self, owner: &Owner) -> Option<PublicKey> {
        if let Some(public_key) = self.super_owners.get(owner) {
            Some(*public_key)
        } else {
            self.owners.get(owner).map(|(public_key, _)| *public_key)
        }
    }

    pub fn first_round(&self) -> RoundNumber {
        if self.super_owners.is_empty() {
            RoundNumber(1)
        } else {
            RoundNumber::ZERO
        }
    }

    pub fn first_single_leader_round(&self) -> RoundNumber {
        self.multi_leader_rounds.saturating_add(RoundNumber(1))
    }

    pub fn all_owners(&self) -> impl Iterator<Item = &Owner> {
        self.super_owners.keys().chain(self.owners.keys())
    }
}
