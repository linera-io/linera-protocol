// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::InputObject;
use linera_base::{
    crypto::{CryptoError, PublicKey},
    data_types::ArithmeticError,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A number identifying the configuration of the chain (aka the committee).
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct Epoch(pub u64);

/// The identity of a validator.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Serialize, Deserialize)]
pub struct ValidatorName(pub PublicKey);

/// Public state of validator.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Default, Serialize, Deserialize)]
pub struct ValidatorState {
    /// The network address (in a string format understood by the networking layer).
    pub network_address: String,
    /// The voting power.
    pub votes: u64,
}

/// A set of validators (identified by their public keys) and their voting rights.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Default, Serialize, Deserialize, InputObject)]
pub struct Committee {
    /// The validators in the committee.
    pub validators: BTreeMap<ValidatorName, ValidatorState>,
    /// The sum of all voting rights.
    pub total_votes: u64,
    /// The threshold to form a quorum.
    pub quorum_threshold: u64,
    /// The threshold to prove the validity of a statement.
    pub validity_threshold: u64,
}

impl std::fmt::Display for ValidatorName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for ValidatorName {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ValidatorName(PublicKey::from_str(s)?))
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Epoch {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Epoch(s.parse()?))
    }
}

impl From<u64> for Epoch {
    fn from(value: u64) -> Self {
        Epoch(value)
    }
}

impl From<PublicKey> for ValidatorName {
    fn from(value: PublicKey) -> Self {
        Self(value)
    }
}

impl Epoch {
    #[inline]
    pub fn try_add_one(self) -> Result<Self, ArithmeticError> {
        let val = self.0.checked_add(1).ok_or(ArithmeticError::Overflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign_one(&mut self) -> Result<(), ArithmeticError> {
        self.0 = self.0.checked_add(1).ok_or(ArithmeticError::Overflow)?;
        Ok(())
    }
}

impl Committee {
    pub fn new(validators: BTreeMap<ValidatorName, ValidatorState>) -> Self {
        let total_votes = validators.values().fold(0, |sum, state| sum + state.votes);
        // Let N = 3f + 1 + k (0 <= k < 3).
        // * (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        // * (N + 2) / 3 = f + 1 + k/3 = f + 1
        let quorum_threshold = 2 * total_votes / 3 + 1;
        let validity_threshold = (total_votes + 2) / 3;

        Committee {
            validators,
            total_votes,
            quorum_threshold,
            validity_threshold,
        }
    }

    #[cfg(any(test, feature = "test"))]
    pub fn make_simple(keys: Vec<ValidatorName>) -> Self {
        let map = keys
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
            .collect();
        Committee::new(map)
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
        self.quorum_threshold
    }

    pub fn validity_threshold(&self) -> u64 {
        self.validity_threshold
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
