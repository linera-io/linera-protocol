// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Structures defining the set of owners and super owners, as well as the consensus
//! round types and timeouts for chains.

use std::{collections::BTreeMap, iter};

use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    crypto::PublicKey,
    data_types::{Round, TimeDelta},
    doc_scalar,
    identifiers::Owner,
};

/// The timeout configuration: how long fast, multi-leader and single-leader rounds last.
#[derive(PartialEq, Eq, Clone, Hash, Debug, Serialize, Deserialize, WitLoad, WitStore, WitType)]
pub struct TimeoutConfig {
    /// The duration of the fast round.
    pub fast_round_duration: Option<TimeDelta>,
    /// The duration of the first single-leader and all multi-leader rounds.
    pub base_timeout: TimeDelta,
    /// The duration by which the timeout increases after each single-leader round.
    pub timeout_increment: TimeDelta,
    /// The age of an incoming tracked or protected message after which the validators start
    /// transitioning the chain to fallback mode.
    pub fallback_duration: TimeDelta,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            fast_round_duration: None,
            base_timeout: TimeDelta::from_secs(10),
            timeout_increment: TimeDelta::from_secs(1),
            fallback_duration: TimeDelta::from_secs(60 * 60 * 24),
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
    pub fn round_timeout(&self, round: Round) -> Option<TimeDelta> {
        let tc = &self.timeout_config;
        match round {
            Round::Fast => tc.fast_round_duration,
            Round::MultiLeader(r) if r.saturating_add(1) == self.multi_leader_rounds => {
                Some(tc.base_timeout)
            }
            Round::MultiLeader(_) => None,
            Round::SingleLeader(r) => {
                let increment = tc.timeout_increment.saturating_mul(u64::from(r));
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
                fast_round_duration: Some(TimeDelta::from_secs(5)),
                base_timeout: TimeDelta::from_secs(10),
                timeout_increment: TimeDelta::from_secs(1),
                fallback_duration: TimeDelta::from_secs(60 * 60),
            },
        };

        assert_eq!(
            ownership.round_timeout(Round::Fast),
            Some(TimeDelta::from_secs(5))
        );
        assert_eq!(ownership.round_timeout(Round::MultiLeader(8)), None);
        assert_eq!(
            ownership.round_timeout(Round::MultiLeader(9)),
            Some(TimeDelta::from_secs(10))
        );
        assert_eq!(
            ownership.round_timeout(Round::SingleLeader(0)),
            Some(TimeDelta::from_secs(10))
        );
        assert_eq!(
            ownership.round_timeout(Round::SingleLeader(1)),
            Some(TimeDelta::from_secs(11))
        );
        assert_eq!(
            ownership.round_timeout(Round::SingleLeader(8)),
            Some(TimeDelta::from_secs(18))
        );
    }
}

doc_scalar!(ChainOwnership, "Represents the owner(s) of a chain");
