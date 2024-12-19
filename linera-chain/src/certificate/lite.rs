// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

use linera_base::{crypto::Signature, data_types::Round, hashed::Hashed};
use linera_execution::committee::{Committee, ValidatorName};
use serde::{Deserialize, Serialize};

use super::{CertificateValue, GenericCertificate};
use crate::{
    data_types::{check_signatures, LiteValue, LiteVote},
    ChainError,
};

/// A certified statement from the committee, without the value.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct LiteCertificate<'a> {
    /// Hash and chain ID of the certified value (used as key for storage).
    pub value: LiteValue,
    /// The round in which the value was certified.
    pub round: Round,
    /// Signatures on the value.
    pub signatures: Cow<'a, [(ValidatorName, Signature)]>,
}

impl<'a> LiteCertificate<'a> {
    pub fn new(
        value: LiteValue,
        round: Round,
        mut signatures: Vec<(ValidatorName, Signature)>,
    ) -> Self {
        signatures.sort_by_key(|&(validator_name, _)| validator_name);

        let signatures = Cow::Owned(signatures);
        Self {
            value,
            round,
            signatures,
        }
    }

    /// Creates a [`LiteCertificate`] from a list of votes, without cryptographically checking the
    /// signatures. Returns `None` if the votes are empty or don't have matching values and rounds.
    pub fn try_from_votes(votes: impl IntoIterator<Item = LiteVote>) -> Option<Self> {
        let mut votes = votes.into_iter();
        let LiteVote {
            value,
            round,
            validator,
            signature,
        } = votes.next()?;
        let mut signatures = vec![(validator, signature)];
        for vote in votes {
            if vote.value.value_hash != value.value_hash || vote.round != round {
                return None;
            }
            signatures.push((vote.validator, vote.signature));
        }
        Some(LiteCertificate::new(value, round, signatures))
    }

    /// Verifies the certificate.
    pub fn check(&self, committee: &Committee) -> Result<&LiteValue, ChainError> {
        check_signatures(
            self.value.value_hash,
            self.value.kind,
            self.round,
            &self.signatures,
            committee,
        )?;
        Ok(&self.value)
    }

    /// Returns the [`GenericCertificate`] with the specified value, if it matches.
    pub fn with_value<T: CertificateValue>(
        self,
        value: Hashed<T>,
    ) -> Option<GenericCertificate<T>> {
        if self.value.chain_id != value.inner().chain_id()
            || T::KIND != self.value.kind
            || self.value.value_hash != value.hash()
        {
            return None;
        }
        Some(GenericCertificate::new(
            value,
            self.round,
            self.signatures.into_owned(),
        ))
    }

    /// Returns a [`LiteCertificate`] that owns the list of signatures.
    pub fn cloned(&self) -> LiteCertificate<'static> {
        LiteCertificate {
            value: self.value.clone(),
            round: self.round,
            signatures: Cow::Owned(self.signatures.clone().into_owned()),
        }
    }
}
