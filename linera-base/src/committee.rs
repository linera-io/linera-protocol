// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::messages::ValidatorName;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A set of validators (identified by their public keys) and their voting rights.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Default, Serialize, Deserialize)]
pub struct Committee {
    /// The voting rights.
    pub voting_rights: BTreeMap<ValidatorName, usize>,
    /// The sum of all voting rights.
    pub total_votes: usize,
}

impl Committee {
    pub fn new(voting_rights: BTreeMap<ValidatorName, usize>) -> Self {
        let total_votes = voting_rights.iter().fold(0, |sum, (_, votes)| sum + *votes);
        Committee {
            voting_rights,
            total_votes,
        }
    }

    pub fn make_simple(keys: Vec<ValidatorName>) -> Self {
        let total_votes = keys.len();
        Committee {
            voting_rights: keys.into_iter().map(|k| (k, 1)).collect(),
            total_votes,
        }
    }

    pub fn weight(&self, author: &ValidatorName) -> usize {
        *self.voting_rights.get(author).unwrap_or(&0)
    }

    pub fn quorum_threshold(&self) -> usize {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        2 * self.total_votes / 3 + 1
    }

    pub fn validity_threshold(&self) -> usize {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        (self.total_votes + 2) / 3
    }

    /// Find the highest value than is supported by a certain subset of validators.
    pub fn get_lower_bound<V>(&self, threshold: usize, mut values: Vec<(ValidatorName, V)>) -> V
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
