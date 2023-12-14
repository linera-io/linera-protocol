// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{crypto::PublicKey, data_types::Round, identifiers::Owner};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, iter};

/// Represents the owner(s) of a chain.
#[derive(PartialEq, Eq, Clone, Hash, Debug, Default, Serialize, Deserialize)]
pub struct ChainOwnership {
    /// Super owners can propose fast blocks in the first round, and regular blocks in any round.
    pub super_owners: BTreeMap<Owner, PublicKey>,
    /// The regular owners, with their weights that determine how often they are round leader.
    pub owners: BTreeMap<Owner, (PublicKey, u64)>,
    /// The number of initial rounds after 0 in which all owners are allowed to propose blocks.
    pub multi_leader_rounds: u32,
}

impl ChainOwnership {
    pub fn single(public_key: PublicKey) -> Self {
        ChainOwnership {
            super_owners: iter::once((Owner::from(public_key), public_key)).collect(),
            owners: BTreeMap::new(),
            multi_leader_rounds: 2,
        }
    }

    pub fn multiple(
        keys_and_weights: impl IntoIterator<Item = (PublicKey, u64)>,
        multi_leader_rounds: u32,
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

    pub fn with_regular_owner(mut self, public_key: PublicKey, weight: u64) -> Self {
        self.owners
            .insert(Owner::from(public_key), (public_key, weight));
        self
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

    pub fn first_round(&self) -> Round {
        if !self.super_owners.is_empty() {
            Round::Fast
        } else if self.multi_leader_rounds > 0 {
            Round::MultiLeader(0)
        } else {
            Round::SingleLeader(0)
        }
    }

    pub fn all_owners(&self) -> impl Iterator<Item = &Owner> {
        self.super_owners.keys().chain(self.owners.keys())
    }

    pub fn next_round(&self, round: Round) -> Option<Round> {
        let next_round = match round {
            Round::Fast if self.multi_leader_rounds == 0 => Round::SingleLeader(0),
            Round::Fast => Round::MultiLeader(0),
            Round::MultiLeader(r) if r >= self.multi_leader_rounds.saturating_sub(1) => {
                Round::SingleLeader(0)
            }
            Round::MultiLeader(r) => Round::MultiLeader(r.checked_add(1)?),
            Round::SingleLeader(r) => Round::SingleLeader(r.checked_add(1)?),
        };
        Some(next_round)
    }

    pub fn previous_round(&self, round: Round) -> Option<Round> {
        let previous_round = match round {
            Round::Fast => return None,
            Round::MultiLeader(r) => {
                if let Some(prev_r) = r.checked_sub(1) {
                    Round::MultiLeader(prev_r)
                } else if self.super_owners.is_empty() {
                    return None;
                } else {
                    Round::Fast
                }
            }
            Round::SingleLeader(r) => {
                if let Some(prev_r) = r.checked_sub(1) {
                    Round::SingleLeader(prev_r)
                } else if let Some(last_multi_r) = self.multi_leader_rounds.checked_sub(1) {
                    Round::MultiLeader(last_multi_r)
                } else if self.super_owners.is_empty() {
                    return None;
                } else {
                    Round::Fast
                }
            }
        };
        Some(previous_round)
    }
}
