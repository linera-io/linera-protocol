// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

use allocative::Allocative;
use linera_base::{
    crypto::{AccountPublicKey, CryptoHash, ValidatorPublicKey},
    data_types::ArithmeticError,
};
use serde::{Deserialize, Serialize};

use crate::policy::ResourceControlPolicy;

/// Public state of a validator.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Serialize, Deserialize, Allocative)]
pub struct ValidatorState {
    /// The network address (in a string format understood by the networking layer).
    pub network_address: String,
    /// The voting power.
    pub votes: u64,
    /// The public key of the account associated with the validator.
    pub account_public_key: AccountPublicKey,
}

/// A set of validators (identified by their public keys) and their voting rights.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Default, Allocative)]
#[cfg_attr(with_graphql, derive(async_graphql::InputObject))]
pub struct Committee {
    /// The validators in the committee.
    pub validators: BTreeMap<ValidatorPublicKey, ValidatorState>,
    /// The sum of all voting rights.
    total_votes: u64,
    /// The threshold to form a quorum.
    quorum_threshold: u64,
    /// The threshold to prove the validity of a statement. I.e. the assumption is that strictly
    /// less than `validity_threshold` are faulty.
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
            Committee::try_from(committee_minimal).map_err(serde::de::Error::custom)
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
        let committee = Committee::new(validators.into_owned(), policy.into_owned())
            .map_err(|e| e.to_string())?;
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

impl TryFrom<CommitteeMinimal<'static>> for Committee {
    type Error = ArithmeticError;

    fn try_from(committee_min: CommitteeMinimal) -> Result<Committee, ArithmeticError> {
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

impl Committee {
    pub fn new(
        validators: BTreeMap<ValidatorPublicKey, ValidatorState>,
        policy: ResourceControlPolicy,
    ) -> Result<Self, ArithmeticError> {
        let mut total_votes: u64 = 0;
        for state in validators.values() {
            total_votes = total_votes
                .checked_add(state.votes)
                .ok_or(ArithmeticError::Overflow)?;
        }
        // The validity threshold is f + 1, where f is maximal so that it is less than a third.
        // So the threshold is N / 3, rounded up.
        let validity_threshold = total_votes.div_ceil(3);
        // The quorum threshold is minimal such that any two quorums intersect in at least one
        // validity threshold.
        let quorum_threshold = total_votes
            .checked_add(validity_threshold)
            .ok_or(ArithmeticError::Overflow)?
            .div_ceil(2);

        Ok(Committee {
            validators,
            total_votes,
            quorum_threshold,
            validity_threshold,
            policy,
        })
    }

    #[cfg(with_testing)]
    pub fn make_simple(keys: Vec<(ValidatorPublicKey, AccountPublicKey)>) -> Self {
        let map = keys
            .into_iter()
            .map(|(validator_key, account_key)| {
                (
                    validator_key,
                    ValidatorState {
                        network_address: "Tcp:localhost:8080".to_string(),
                        votes: 100,
                        account_public_key: account_key,
                    },
                )
            })
            .collect();
        Committee::new(map, ResourceControlPolicy::default())
            .expect("test committee votes should not overflow")
    }

    pub fn weight(&self, author: &ValidatorPublicKey) -> u64 {
        match self.validators.get(author) {
            Some(state) => state.votes,
            None => 0,
        }
    }

    pub fn account_keys_and_weights(&self) -> impl Iterator<Item = (AccountPublicKey, u64)> + '_ {
        self.validators
            .values()
            .map(|validator| (validator.account_public_key, validator.votes))
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

    /// Returns a mutable reference to this committee's [`ResourceControlPolicy`].
    pub fn policy_mut(&mut self) -> &mut ResourceControlPolicy {
        &mut self.policy
    }
}

/// Process-global, append-only cache of committees keyed by their blob hash.
///
/// Committees are network-global state (created by the admin chain, agreed
/// on by every validator), so caching them once per process avoids holding a
/// separate copy in every chain's execution state. The map is populated
/// lazily by `get_or_load_committee_by_hash` in the storage layer.
#[derive(Clone, Debug, Default)]
pub struct SharedCommittees {
    map: Arc<papaya::HashMap<CryptoHash, Arc<Committee>>>,
}

impl SharedCommittees {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the cached committee for `hash`, if any.
    pub fn get(&self, hash: CryptoHash) -> Option<Arc<Committee>> {
        self.map.pin().get(&hash).cloned()
    }

    /// Inserts `committee` under `hash`. If an entry was already present, the
    /// existing value wins and is returned (avoiding spurious clones when two
    /// callers race to populate the same hash).
    pub fn insert(&self, hash: CryptoHash, committee: Arc<Committee>) -> Arc<Committee> {
        let pinned = self.map.pin();
        match pinned.try_insert(hash, committee) {
            Ok(inserted) => inserted.clone(),
            Err(e) => e.current.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_committees_insert_and_get() {
        let shared = SharedCommittees::new();
        let hash = CryptoHash::test_hash("c0");
        assert!(shared.get(hash).is_none());
        let committee = Arc::new(Committee::default());
        let inserted = shared.insert(hash, committee.clone());
        assert!(Arc::ptr_eq(&inserted, &committee));
        let fetched = shared.get(hash).unwrap();
        assert!(Arc::ptr_eq(&fetched, &committee));
    }

    #[test]
    fn shared_committees_insert_is_first_writer_wins() {
        let shared = SharedCommittees::new();
        let hash = CryptoHash::test_hash("c1");
        let first = Arc::new(Committee::default());
        let second = Arc::new(Committee::default());
        let winner = shared.insert(hash, first.clone());
        assert!(Arc::ptr_eq(&winner, &first));
        let loser = shared.insert(hash, second.clone());
        assert!(Arc::ptr_eq(&loser, &first));
        assert!(!Arc::ptr_eq(&loser, &second));
    }

    #[test]
    fn shared_committees_clones_share_storage() {
        let a = SharedCommittees::new();
        let b = a.clone();
        let hash = CryptoHash::test_hash("c2");
        let committee = Arc::new(Committee::default());
        a.insert(hash, committee.clone());
        let fetched = b.get(hash).unwrap();
        assert!(Arc::ptr_eq(&fetched, &committee));
    }
}
