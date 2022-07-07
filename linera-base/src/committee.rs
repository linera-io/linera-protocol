// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::messages::ValidatorName;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Public state of validator.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Default, Serialize, Deserialize)]
pub struct ValidatorState {
    /// The network address (in a string format understood by the networking layer).
    pub network_address: String,
    /// The voting power.
    pub votes: u64,
}

/// A set of validators (identified by their public keys) and their voting rights.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Default, Serialize, Deserialize)]
pub struct Committee {
    /// The validators in the committee.
    pub validators: BTreeMap<ValidatorName, ValidatorState>,
    /// The sum of all voting rights.
    pub total_votes: u64,
}

impl Committee {
    pub fn new(validators: BTreeMap<ValidatorName, ValidatorState>) -> Self {
        let total_votes = validators
            .iter()
            .fold(0, |sum, (_, state)| sum + state.votes);
        Committee {
            validators,
            total_votes,
        }
    }

    /// For testing
    pub fn make_simple(keys: Vec<ValidatorName>) -> Self {
        let total_votes = keys.len() as u64;
        Committee {
            validators: keys
                .into_iter()
                .map(|k| {
                    (
                        k,
                        ValidatorState {
                            network_address: k.to_string(),
                            votes: 1,
                        },
                    )
                })
                .collect(),
            total_votes,
        }
    }

    pub fn weight(&self, author: &ValidatorName) -> u64 {
        match self.validators.get(author) {
            Some(state) => state.votes,
            None => 0,
        }
    }

    pub fn network_address(&self, author: &ValidatorName) -> Option<&str> {
        self.validators
            .get(author)
            .map(|state| state.network_address.as_ref())
    }

    pub fn quorum_threshold(&self) -> u64 {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        2 * self.total_votes / 3 + 1
    }

    pub fn validity_threshold(&self) -> u64 {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        (self.total_votes + 2) / 3
    }

    /// Find the highest value than is supported by a certain subset of validators.
    pub fn get_lower_bound<V>(&self, threshold: u64, mut values: Vec<(ValidatorName, V)>) -> V
    where
        V: Default + std::cmp::Ord,
    {
        values.sort_by(|(_, x), (_, y)| V::cmp(y, x));
        // Browse values by decreasing order while collecting votes.
        let mut score = 0;
        for (name, value) in values {
            score += self.weight(&name);
            if score >= threshold {
                return value;
            }
        }
        V::default()
    }

    /// Find the highest value than is supported by a quorum of validators.
    pub fn get_strong_majority_lower_bound<V>(&self, values: Vec<(ValidatorName, V)>) -> V
    where
        V: Default + std::cmp::Ord,
    {
        self.get_lower_bound(self.quorum_threshold(), values)
    }

    /// Find the highest value than is guaranteed to be supported by at least one honest validator.
    pub fn get_validity_lower_bound<V>(&self, values: Vec<(ValidatorName, V)>) -> V
    where
        V: Default + std::cmp::Ord,
    {
        self.get_lower_bound(self.validity_threshold(), values)
    }
}
