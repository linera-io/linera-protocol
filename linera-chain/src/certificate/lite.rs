// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, ops::Deref};

use allocative::{Allocative, Key, Visitor};
use linera_base::{
    crypto::{ValidatorPublicKey, ValidatorSignature},
    data_types::Round,
};
use linera_execution::committee::Committee;
use serde::{Deserialize, Serialize};

use super::{CertificateValue, GenericCertificate};
use crate::{
    data_types::{check_signatures, LiteValue, LiteVote},
    justification::JustificationChain,
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
    /// The unlocking round the `ValidatedBlock` voters signed (see [`VoteValue`]). Always `None`
    /// for `ConfirmedBlock`/`Timeout` certificates and for validated blocks with no justification.
    ///
    /// [`VoteValue`]: crate::data_types::VoteValue
    pub unlocking_round: Option<Round>,
    /// The first-round attestation the `ConfirmedBlock` voters signed (see [`VoteValue`]). Only
    /// `true` for a `ConfirmedBlock` certificate confirming a block in the chain's first round;
    /// always `false` for `ValidatedBlock`/`Timeout` certificates.
    ///
    /// [`VoteValue`]: crate::data_types::VoteValue
    pub first_round: bool,
    /// The justification chain attached to this certificate: for a `ValidatedBlock` certificate
    /// it is the chain of validated quorums in rounds below `round`; for a `ConfirmedBlock`
    /// certificate it is the full chain of validated quorums up to and including the confirm
    /// round. Empty for `Timeout` certificates and for blocks needing no justification.
    pub justification: JustificationChain,
    /// Signatures on the value.
    pub signatures: Cow<'a, [(ValidatorPublicKey, ValidatorSignature)]>,
}

impl Allocative for LiteCertificate<'_> {
    fn visit<'a, 'b: 'a>(&self, visitor: &'a mut Visitor<'b>) {
        visitor.visit_field(Key::new("LiteCertificate_value"), &self.value);
        visitor.visit_field(Key::new("LiteCertificate_round"), &self.round);
        visitor.visit_field(
            Key::new("LiteCertificate_justification"),
            &self.justification,
        );
        if matches!(self.signatures, Cow::Owned(_)) {
            for (public_key, signature) in self.signatures.deref() {
                visitor.visit_field(Key::new("ValidatorPublicKey"), public_key);
                visitor.visit_field(Key::new("ValidatorSignature"), signature);
            }
        }
    }
}

impl LiteCertificate<'_> {
    /// Creates a new lite certificate from a value, round and list of signatures.
    pub fn new(
        value: LiteValue,
        round: Round,
        signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    ) -> Self {
        Self::new_with_unlocking_round(value, round, None, signatures)
    }

    /// Creates a new lite certificate that also records the unlocking round its `ValidatedBlock`
    /// voters signed (see [`VoteValue`]).
    ///
    /// [`VoteValue`]: crate::data_types::VoteValue
    pub fn new_with_unlocking_round(
        value: LiteValue,
        round: Round,
        unlocking_round: Option<Round>,
        signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    ) -> Self {
        Self::new_with_unlocking_round_and_first_round(
            value,
            round,
            unlocking_round,
            false,
            signatures,
        )
    }

    /// Creates a new lite certificate that also records the unlocking round its `ValidatedBlock`
    /// voters signed and the first-round attestation its `ConfirmedBlock` voters signed (see
    /// [`VoteValue`]).
    ///
    /// [`VoteValue`]: crate::data_types::VoteValue
    pub fn new_with_unlocking_round_and_first_round(
        value: LiteValue,
        round: Round,
        unlocking_round: Option<Round>,
        first_round: bool,
        mut signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    ) -> Self {
        signatures.sort_by_key(|&(validator_name, _)| validator_name);

        let signatures = Cow::Owned(signatures);
        Self {
            value,
            round,
            unlocking_round,
            first_round,
            justification: JustificationChain::default(),
            signatures,
        }
    }

    /// Creates a [`LiteCertificate`] from a list of votes with their validator public keys, without cryptographically checking the
    /// signatures. Returns `None` if the votes are empty or don't have matching values and rounds.
    pub fn try_from_votes(
        votes: impl IntoIterator<Item = (ValidatorPublicKey, LiteVote)>,
    ) -> Option<Self> {
        let mut votes = votes.into_iter();
        let (
            public_key,
            LiteVote {
                value,
                round,
                unlocking_round,
                first_round,
                signature,
            },
        ) = votes.next()?;
        let mut signatures = vec![(public_key, signature)];
        for (validator_key, vote) in votes {
            if vote.value.value_hash != value.value_hash
                || vote.round != round
                || vote.unlocking_round != unlocking_round
                || vote.first_round != first_round
            {
                return None;
            }
            signatures.push((validator_key, vote.signature));
        }
        Some(LiteCertificate::new_with_unlocking_round_and_first_round(
            value,
            round,
            unlocking_round,
            first_round,
            signatures,
        ))
    }

    /// Verifies the certificate, including its justification chain.
    pub fn check(&self, committee: &Committee) -> Result<&LiteValue, ChainError> {
        check_signatures(
            self.value.value_hash,
            self.value.kind,
            self.round,
            self.unlocking_round,
            self.first_round,
            &self.signatures,
            committee,
        )?;
        self.justification
            .verify(self.value.value_hash, committee)?;
        Ok(&self.value)
    }

    /// Returns the full justification chain that a certificate validating in a higher round
    /// would carry below itself: the chain it already carries, with this certificate's own
    /// quorum appended as the new top link.
    pub fn full_justification(&self) -> JustificationChain {
        self.justification
            .append(self.round, self.signatures.to_vec())
    }

    /// Checks whether the value matches this certificate.
    pub fn check_value<T: CertificateValue>(&self, value: &T) -> bool {
        self.value.chain_id == value.chain_id()
            && T::KIND == self.value.kind
            && self.value.value_hash == value.hash()
    }

    /// Returns the [`GenericCertificate`] with the specified value, if it matches.
    pub fn with_value<T: CertificateValue>(self, value: T) -> Option<GenericCertificate<T>> {
        if self.value.chain_id != value.chain_id()
            || T::KIND != self.value.kind
            || self.value.value_hash != value.hash()
        {
            return None;
        }
        Some(
            GenericCertificate::new_with_unlocking_round_and_first_round(
                value,
                self.round,
                self.unlocking_round,
                self.first_round,
                self.signatures.into_owned(),
            ),
        )
    }

    /// Returns a [`LiteCertificate`] that owns the list of signatures.
    pub fn cloned(&self) -> LiteCertificate<'static> {
        LiteCertificate {
            value: self.value.clone(),
            round: self.round,
            unlocking_round: self.unlocking_round,
            first_round: self.first_round,
            justification: self.justification.clone(),
            signatures: Cow::Owned(self.signatures.clone().into_owned()),
        }
    }
}
