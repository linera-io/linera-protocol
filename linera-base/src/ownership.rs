// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Structures defining the set of owners and super owners, as well as the consensus
//! round types and timeouts for chains.

use std::{collections::BTreeMap, iter, time::Duration};

use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{crypto::PublicKey, data_types::Round, doc_scalar, identifiers::Owner};

/// The timeout configuration: how long fast, multi-leader and single-leader rounds last.
#[derive(PartialEq, Eq, Clone, Hash, Debug, Serialize, Deserialize, WitLoad, WitStore, WitType)]
pub struct TimeoutConfig {
    /// The duration of the fast round.
    pub fast_round_duration: Option<Duration>,
    /// The duration of the first single-leader and all multi-leader rounds.
    pub base_timeout: Duration,
    /// The duration by which the timeout increases after each single-leader round.
    pub timeout_increment: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            fast_round_duration: None,
            base_timeout: Duration::from_secs(10),
            timeout_increment: Duration::from_secs(1),
        }
    }
}

/// Represents the owner(s) of a chain.
#[derive(
    PartialEq, Eq, Clone, Hash, Debug, Default, Serialize, Deserialize, WitLoad, WitStore, WitType,
)]
pub struct ChainOwnership {
    /// Super owners can propose fast blocks in the first round, and regular blocks in any round.
    pub super_owners: BTreeMap<Owner, PublicKey>,
    /// The regular owners, with their weights that determine how often they are round leader.
    pub owners: BTreeMap<Owner, (PublicKey, u64)>,
    /// The number of initial rounds after 0 in which all owners are allowed to propose blocks.
    pub multi_leader_rounds: u32,
    /// The timeout configuration: how long fast, multi-leader and single-leader rounds last.
    pub timeout_config: TimeoutConfig,
}

impl ChainOwnership {
    /// Creates a `ChainOwnership` with a single super owner.
    pub fn single(public_key: PublicKey) -> Self {
        ChainOwnership {
            super_owners: iter::once((Owner::from(public_key), public_key)).collect(),
            owners: BTreeMap::new(),
            multi_leader_rounds: 2,
            timeout_config: TimeoutConfig::default(),
        }
    }

    /// Creates a `ChainOwnership` with the specified regular owners.
    pub fn multiple(
        keys_and_weights: impl IntoIterator<Item = (PublicKey, u64)>,
        multi_leader_rounds: u32,
        timeout_config: TimeoutConfig,
    ) -> Self {
        ChainOwnership {
            super_owners: BTreeMap::new(),
            owners: keys_and_weights
                .into_iter()
                .map(|(public_key, weight)| (Owner::from(public_key), (public_key, weight)))
                .collect(),
            multi_leader_rounds,
            timeout_config,
        }
    }

    /// Adds a regular owner.
    pub fn with_regular_owner(mut self, public_key: PublicKey, weight: u64) -> Self {
        self.owners
            .insert(Owner::from(public_key), (public_key, weight));
        self
    }

    /// Returns whether there are any owners or super owners.
    pub fn is_active(&self) -> bool {
        !self.super_owners.is_empty() || !self.owners.is_empty()
    }

    /// Returns the given owner's public key, if they are an owner or super owner.
    pub fn verify_owner(&self, owner: &Owner) -> Option<PublicKey> {
        if let Some(public_key) = self.super_owners.get(owner) {
            Some(*public_key)
        } else {
            self.owners.get(owner).map(|(public_key, _)| *public_key)
        }
    }

    /// Returns the duration of the given round.
    pub fn round_timeout(&self, round: Round) -> Option<Duration> {
        let tc = &self.timeout_config;
        match round {
            Round::Fast => tc.fast_round_duration,
            Round::MultiLeader(r) if r.saturating_add(1) == self.multi_leader_rounds => {
                Some(tc.base_timeout)
            }
            Round::MultiLeader(_) => None,
            Round::SingleLeader(r) => {
                let increment = tc.timeout_increment.saturating_mul(r);
                Some(tc.base_timeout.saturating_add(increment))
            }
        }
    }

    /// Returns the first consensus round for this configuration.
    pub fn first_round(&self) -> Round {
        if !self.super_owners.is_empty() {
            Round::Fast
        } else if self.multi_leader_rounds > 0 {
            Round::MultiLeader(0)
        } else {
            Round::SingleLeader(0)
        }
    }

    /// Returns an iterator over all super owners' keys, followed by all owners'.
    pub fn all_owners(&self) -> impl Iterator<Item = &Owner> {
        self.super_owners.keys().chain(self.owners.keys())
    }

    /// Returns the round following the specified one, if any.
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

    /// Returns the round preceding the specified one, if any.
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

/// Errors that can happen when attempting to close a chain.
#[derive(Clone, Copy, Debug, Error, WitStore, WitType)]
pub enum CloseChainError {
    /// Authenticated signer wasn't allowed to close the chain.
    #[error("Unauthorized attempt to close the chain")]
    NotPermitted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ownership_round_timeouts() {
        use crate::crypto::KeyPair;

        let super_pub_key = KeyPair::generate().public();
        let super_owner = Owner::from(super_pub_key);
        let pub_key = KeyPair::generate().public();
        let owner = Owner::from(pub_key);

        let ownership = ChainOwnership {
            super_owners: BTreeMap::from_iter([(super_owner, super_pub_key)]),
            owners: BTreeMap::from_iter([(owner, (pub_key, 100))]),
            multi_leader_rounds: 10,
            timeout_config: TimeoutConfig {
                fast_round_duration: Some(Duration::from_secs(5)),
                base_timeout: Duration::from_secs(10),
                timeout_increment: Duration::from_secs(1),
            },
        };

        assert_eq!(
            ownership.round_timeout(Round::Fast),
            Some(Duration::from_secs(5))
        );
        assert_eq!(ownership.round_timeout(Round::MultiLeader(8)), None);
        assert_eq!(
            ownership.round_timeout(Round::MultiLeader(9)),
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            ownership.round_timeout(Round::SingleLeader(0)),
            Some(Duration::from_secs(10))
        );
        assert_eq!(
            ownership.round_timeout(Round::SingleLeader(1)),
            Some(Duration::from_secs(11))
        );
        assert_eq!(
            ownership.round_timeout(Round::SingleLeader(8)),
            Some(Duration::from_secs(18))
        );
    }
}

doc_scalar!(ChainOwnership, "Represents the owner(s) of a chain");
