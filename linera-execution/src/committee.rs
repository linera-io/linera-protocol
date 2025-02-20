// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, collections::BTreeMap, str::FromStr};

use async_graphql::InputObject;
use linera_base::{
    crypto::{CryptoError, ValidatorPublicKey},
    data_types::ArithmeticError,
};
use serde::{Deserialize, Serialize};

use crate::policy::ResourceControlPolicy;

/// A number identifying the configuration of the chain (aka the committee).
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug)]
pub struct Epoch(pub u32);

impl Epoch {
    pub const ZERO: Epoch = Epoch(0);
}

impl Serialize for Epoch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.0.to_string())
        } else {
            serializer.serialize_newtype_struct("Epoch", &self.0)
        }
    }
}

impl<'de> Deserialize<'de> for Epoch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            Ok(Epoch(u32::from_str(&s).map_err(serde::de::Error::custom)?))
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "Epoch")]
            struct EpochDerived(u32);

            let value = EpochDerived::deserialize(deserializer)?;
            Ok(Self(value.0))
        }
    }
}

/// The identity of a validator.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug)]
pub struct ValidatorName(pub ValidatorPublicKey);

impl Serialize for ValidatorName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            serializer.serialize_newtype_struct("ValidatorName", &self.0)
        }
    }
}

impl<'de> Deserialize<'de> for ValidatorName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let value = Self::from_str(&s).map_err(serde::de::Error::custom)?;
            Ok(value)
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "ValidatorName")]
            struct ValidatorNameDerived(ValidatorPublicKey);

            let value = ValidatorNameDerived::deserialize(deserializer)?;
            Ok(Self(value.0))
        }
    }
}

/// Public state of a validator.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Default, Serialize, Deserialize)]
pub struct ValidatorState {
    /// The network address (in a string format understood by the networking layer).
    pub network_address: String,
    /// The voting power.
    pub votes: u64,
}

/// A set of validators (identified by their public keys) and their voting rights.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Default, InputObject)]
pub struct Committee {
    /// The validators in the committee.
    validators: BTreeMap<ValidatorPublicKey, ValidatorState>,
    /// The sum of all voting rights.
    total_votes: u64,
    /// The threshold to form a quorum.
    quorum_threshold: u64,
    /// The threshold to prove the validity of a statement.
    validity_threshold: u64,
    /// The policy agreed on for this epoch.
    policy: ResourceControlPolicy,
}

impl Serialize for Committee {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            CommitteeFull::from(self).serialize(serializer)
        } else {
            CommitteeMinimal::from(self).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Committee {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let committee_full = CommitteeFull::deserialize(deserializer)?;
            Committee::try_from(committee_full).map_err(serde::de::Error::custom)
        } else {
            let committee_minimal = CommitteeMinimal::deserialize(deserializer)?;
            Ok(Committee::from(committee_minimal))
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "Committee")]
struct CommitteeFull<'a> {
    validators: Cow<'a, BTreeMap<ValidatorPublicKey, ValidatorState>>,
    total_votes: u64,
    quorum_threshold: u64,
    validity_threshold: u64,
    policy: Cow<'a, ResourceControlPolicy>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "Committee")]
struct CommitteeMinimal<'a> {
    validators: Cow<'a, BTreeMap<ValidatorPublicKey, ValidatorState>>,
    policy: Cow<'a, ResourceControlPolicy>,
}

impl TryFrom<CommitteeFull<'static>> for Committee {
    type Error = String;

    fn try_from(committee_full: CommitteeFull) -> Result<Committee, Self::Error> {
        let CommitteeFull {
            validators,
            total_votes,
            quorum_threshold,
            validity_threshold,
            policy,
        } = committee_full;
        let committee = Committee::new(validators.into_owned(), policy.into_owned());
        if total_votes != committee.total_votes {
            Err(format!(
                "invalid committee: total_votes is {}; should be {}",
                total_votes, committee.total_votes,
            ))
        } else if quorum_threshold != committee.quorum_threshold {
            Err(format!(
                "invalid committee: quorum_threshold is {}; should be {}",
                quorum_threshold, committee.quorum_threshold,
            ))
        } else if validity_threshold != committee.validity_threshold {
            Err(format!(
                "invalid committee: validity_threshold is {}; should be {}",
                validity_threshold, committee.validity_threshold,
            ))
        } else {
            Ok(committee)
        }
    }
}

impl<'a> From<&'a Committee> for CommitteeFull<'a> {
    fn from(committee: &'a Committee) -> CommitteeFull<'a> {
        let Committee {
            validators,
            total_votes,
            quorum_threshold,
            validity_threshold,
            policy,
        } = committee;
        CommitteeFull {
            validators: Cow::Borrowed(validators),
            total_votes: *total_votes,
            quorum_threshold: *quorum_threshold,
            validity_threshold: *validity_threshold,
            policy: Cow::Borrowed(policy),
        }
    }
}

impl From<CommitteeMinimal<'static>> for Committee {
    fn from(committee_min: CommitteeMinimal) -> Committee {
        let CommitteeMinimal { validators, policy } = committee_min;
        Committee::new(validators.into_owned(), policy.into_owned())
    }
}

impl<'a> From<&'a Committee> for CommitteeMinimal<'a> {
    fn from(committee: &'a Committee) -> CommitteeMinimal<'a> {
        let Committee {
            validators,
            total_votes: _,
            quorum_threshold: _,
            validity_threshold: _,
            policy,
        } = committee;
        CommitteeMinimal {
            validators: Cow::Borrowed(validators),
            policy: Cow::Borrowed(policy),
        }
    }
}

impl std::fmt::Display for ValidatorName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for ValidatorName {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ValidatorName(ValidatorPublicKey::from_str(s)?))
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

impl From<u32> for Epoch {
    fn from(value: u32) -> Self {
        Epoch(value)
    }
}

impl From<ValidatorPublicKey> for ValidatorName {
    fn from(value: ValidatorPublicKey) -> Self {
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
    pub fn new(
        validators: BTreeMap<ValidatorPublicKey, ValidatorState>,
        policy: ResourceControlPolicy,
    ) -> Self {
        let total_votes = validators.values().fold(0, |sum, state| sum + state.votes);
        // Let N = 3f + 1 + k such that 0 <= k <= 2. (Notably ⌊k / 3⌋ = 0 and ⌊(2 - k) / 3⌋ = 0.)
        // The following thresholds verify:
        // * ⌊2 N / 3⌋ + 1 = ⌊(6f + 2 + 2k) / 3⌋ + 1 = 2f + 1 + k + ⌊(2 - k) / 3⌋ = N - f
        // * ⌊(N + 2) / 3⌋= ⌊(3f + 3 + k) / 3⌋ = f + 1 + ⌊k / 3⌋ = f + 1
        let quorum_threshold = 2 * total_votes / 3 + 1;
        let validity_threshold = (total_votes + 2) / 3;

        Committee {
            validators,
            total_votes,
            quorum_threshold,
            validity_threshold,
            policy,
        }
    }

    #[cfg(with_testing)]
    pub fn make_simple(keys: Vec<ValidatorPublicKey>) -> Self {
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
        Committee::new(map, ResourceControlPolicy::default())
    }

    pub fn weight(&self, author: &ValidatorPublicKey) -> u64 {
        match self.validators.get(author) {
            Some(state) => state.votes,
            None => 0,
        }
    }

    pub fn keys_and_weights(&self) -> impl Iterator<Item = (ValidatorPublicKey, u64)> + '_ {
        self.validators
            .iter()
            .map(|(name, validator)| (*name, validator.votes))
    }

    pub fn network_address(&self, author: &ValidatorPublicKey) -> Option<&str> {
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

    pub fn validators(&self) -> &BTreeMap<ValidatorPublicKey, ValidatorState> {
        &self.validators
    }

    pub fn validator_addresses(&self) -> impl Iterator<Item = (ValidatorPublicKey, &str)> {
        self.validators
            .iter()
            .map(|(name, validator)| (*name, &*validator.network_address))
    }

    pub fn total_votes(&self) -> u64 {
        self.total_votes
    }

    pub fn policy(&self) -> &ResourceControlPolicy {
        &self.policy
    }
}
