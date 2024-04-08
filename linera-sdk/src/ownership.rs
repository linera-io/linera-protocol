// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Structures defining the set of owners and super owners, as well as the consensus
//! round types and timeouts for chains.

use std::{collections::BTreeMap, iter, time::Duration};

use linera_base::{crypto::PublicKey, doc_scalar, identifiers::Owner};
use serde::{Deserialize, Serialize};

/// The timeout configuration: how long fast, multi-leader and single-leader rounds last.
///
/// Due to how these are used in the Linera protocol, all durations will be rounded to
/// microseconds and truncated to at most 2^64 microseconds.
#[derive(PartialEq, Eq, Clone, Hash, Debug, Serialize, Deserialize)]
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
#[derive(PartialEq, Eq, Clone, Hash, Debug, Default, Serialize, Deserialize)]
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
}

doc_scalar!(ChainOwnership, "Represents the owner(s) of a chain");
