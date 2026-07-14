// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, ops::Deref};

use allocative::{Allocative, Key, Visitor};
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey, ValidatorSignature},
    data_types::Round,
    ensure,
};
use linera_execution::committee::Committee;
use serde::{Deserialize, Serialize};

use super::{
    CertificateValue, ConfirmedBlockCertificate, GenericCertificate, ValidatedBlockCertificate,
};
use crate::{
    block::{ConfirmedBlock, ValidatedBlock},
    data_types::{check_signatures, LiteValue, LiteVote, VoteValue},
    justification::{CommittedQuorum, JustificationChain},
    types::CertificateKind,
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
    /// The justification commitment the voters signed (see [`VoteValue`]): the hash of the
    /// carried chain's top link, or `None` if the chain is empty.
    ///
    /// [`VoteValue`]: crate::data_types::VoteValue
    pub justification_commitment: Option<CryptoHash>,
    /// The justification chain attached to this certificate: for a `ValidatedBlock` certificate
    /// it is the chain of validated quorums in rounds below `round`; for a `ConfirmedBlock`
    /// certificate it is the full chain of validated quorums up to and including the confirm
    /// round. Empty for `Timeout` certificates and for blocks needing no justification. Borrowed,
    /// like `signatures`, so building a lite certificate from a full one never clones the chain.
    pub justification: Cow<'a, JustificationChain>,
    /// Signatures on the value.
    pub signatures: Cow<'a, [(ValidatorPublicKey, ValidatorSignature)]>,
}

impl Allocative for LiteCertificate<'_> {
    fn visit<'a, 'b: 'a>(&self, visitor: &'a mut Visitor<'b>) {
        visitor.visit_field(Key::new("LiteCertificate_value"), &self.value);
        visitor.visit_field(Key::new("LiteCertificate_round"), &self.round);
        visitor.visit_field(
            Key::new("LiteCertificate_justification"),
            self.justification.as_ref(),
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
    /// Creates a new lite certificate that records the signed payload fields beyond the value
    /// and round: the unlocking round its `ValidatedBlock` voters signed, the first-round
    /// attestation its `ConfirmedBlock` voters signed, and the justification commitment (see
    /// [`VoteValue`]) — with an empty justification chain.
    ///
    /// [`VoteValue`]: crate::data_types::VoteValue
    pub fn new_with_payload(
        value: LiteValue,
        round: Round,
        unlocking_round: Option<Round>,
        first_round: bool,
        justification_commitment: Option<CryptoHash>,
        mut signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    ) -> Self {
        signatures.sort_by_key(|&(validator_name, _)| validator_name);

        Self {
            value,
            round,
            unlocking_round,
            first_round,
            justification_commitment,
            justification: Cow::Owned(JustificationChain::default()),
            signatures: Cow::Owned(signatures),
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
                justification_commitment,
                signature,
            },
        ) = votes.next()?;
        let mut signatures = vec![(public_key, signature)];
        for (validator_key, vote) in votes {
            if vote.value.value_hash != value.value_hash
                || vote.round != round
                || vote.unlocking_round != unlocking_round
                || vote.first_round != first_round
                || vote.justification_commitment != justification_commitment
            {
                return None;
            }
            signatures.push((validator_key, vote.signature));
        }
        Some(LiteCertificate::new_with_payload(
            value,
            round,
            unlocking_round,
            first_round,
            justification_commitment,
            signatures,
        ))
    }

    /// Verifies the certificate: its signatures, its justification chain, and that the signed
    /// unlocking round and first-round attestation are bound to that chain exactly as
    /// [`ValidatedBlockCertificate::check`] and [`ConfirmedBlockCertificate::check`] require. This
    /// is the single verification the worker applies to the certificate a retry proposal carries,
    /// so it must reject a stripped or mismatched chain, not just check the pieces in isolation.
    ///
    /// [`ValidatedBlockCertificate::check`]: super::ValidatedBlockCertificate::check
    /// [`ConfirmedBlockCertificate::check`]: super::ConfirmedBlockCertificate::check
    pub fn check(&self, committee: &Committee) -> Result<&LiteValue, ChainError> {
        // The carried chain's links are not signature-checked: the signed justification
        // commitment is the hash-linked head of the chain, so the single signature check over
        // this certificate's own quorum (below) attests every link — each link's voters verified
        // the quorum beneath them before signing over its hash.
        let derived_commitment = self.justification.verify(self.value.value_hash)?;
        ensure!(
            self.justification_commitment == derived_commitment,
            ChainError::JustificationCommitmentMismatch
        );
        let value = VoteValue(
            self.value.value_hash,
            self.round,
            self.value.kind,
            self.unlocking_round,
            self.first_round,
            self.justification_commitment,
        );
        check_signatures(&value, &self.signatures, committee)?;
        let top = self.justification.top_unlocking_round();
        match self.value.kind {
            CertificateKind::Validated => {
                // The signed unlocking round must be the top of the chain, which must lie strictly
                // below the certified round.
                ensure!(
                    self.unlocking_round == top,
                    ChainError::JustificationUnlockingRoundMismatch
                );
                ensure!(
                    top.is_none_or(|top| top < self.round),
                    ChainError::JustificationChainNotBelowCertificate
                );
            }
            CertificateKind::Confirmed => {
                // The first-round attestation can only be set in a round that could be a chain's
                // first one.
                if self.first_round {
                    ensure!(
                        matches!(
                            self.round,
                            Round::Fast
                                | Round::MultiLeader(0)
                                | Round::SingleLeader(0)
                                | Round::Validator(0)
                        ),
                        ChainError::FalseFirstRoundAttestation
                    );
                }
                match top {
                    // An absent chain is allowed only for a first-round confirmation.
                    None => ensure!(
                        self.first_round,
                        ChainError::JustificationUnlockingRoundMismatch
                    ),
                    // Otherwise the chain's top link is the validation in the confirmation round.
                    Some(top) => ensure!(
                        top == self.round,
                        ChainError::JustificationUnlockingRoundMismatch
                    ),
                }
            }
            // Timeout certificates carry no justification.
            CertificateKind::Timeout => ensure!(
                top.is_none(),
                ChainError::JustificationUnlockingRoundMismatch
            ),
        }
        Ok(&self.value)
    }

    /// Returns the full justification chain that a certificate validating in a higher round
    /// would carry below itself: the chain it already carries, with this certificate's own
    /// quorum appended as the new top link.
    pub fn full_justification(&self) -> JustificationChain {
        self.justification
            .append(self.round, self.signatures.to_vec())
    }

    /// Returns the justification commitment that a vote citing this certificate signs: the hash
    /// of this certificate's own quorum as a [`CommittedQuorum`], which transitively commits to
    /// the chain below it. Equals [`full_justification`](Self::full_justification)'s commitment.
    pub fn full_justification_commitment(&self) -> CryptoHash {
        CommittedQuorum {
            value_hash: self.value.value_hash,
            round: self.round,
            unlocking_round: self.unlocking_round,
            previous: self.justification_commitment,
            signatures: self.signatures.to_vec(),
        }
        .commitment()
    }

    /// Checks whether the value matches this certificate.
    pub fn check_value<T: CertificateValue>(&self, value: &T) -> bool {
        self.value.chain_id == value.chain_id()
            && T::KIND == self.value.kind
            && self.value.value_hash == value.hash()
    }

    /// Returns the [`GenericCertificate`] with the specified value, if it matches. The
    /// justification chain is dropped; use [`into_confirmed_certificate`](Self::into_confirmed_certificate)
    /// or [`into_validated_certificate`](Self::into_validated_certificate) to keep it.
    pub fn with_value<T: CertificateValue>(self, value: T) -> Option<GenericCertificate<T>> {
        Some(self.into_quorum_and_chain(value)?.0)
    }

    /// Consumes this lite certificate into the full [`ConfirmedBlockCertificate`] for `value`,
    /// carrying the justification chain across (never cloning it). Returns `None` if the value
    /// does not match.
    pub fn into_confirmed_certificate(
        self,
        value: ConfirmedBlock,
    ) -> Option<ConfirmedBlockCertificate> {
        let (quorum, justification) = self.into_quorum_and_chain(value)?;
        Some(ConfirmedBlockCertificate::from_parts(quorum, justification))
    }

    /// Consumes this lite certificate into the full [`ValidatedBlockCertificate`] for `value`,
    /// carrying the justification chain across (never cloning it). Returns `None` if the value
    /// does not match.
    pub fn into_validated_certificate(
        self,
        value: ValidatedBlock,
    ) -> Option<ValidatedBlockCertificate> {
        let (quorum, justification) = self.into_quorum_and_chain(value)?;
        Some(ValidatedBlockCertificate::from_parts(quorum, justification))
    }

    /// Splits this lite certificate into the signed quorum for `value` and its justification
    /// chain, moving both out. Returns `None` if the value does not match.
    fn into_quorum_and_chain<T: CertificateValue>(
        self,
        value: T,
    ) -> Option<(GenericCertificate<T>, JustificationChain)> {
        if !self.check_value(&value) {
            return None;
        }
        let quorum = GenericCertificate::new_with_payload(
            value,
            self.round,
            self.unlocking_round,
            self.first_round,
            self.justification_commitment,
            self.signatures.into_owned(),
        );
        Some((quorum, self.justification.into_owned()))
    }

    /// Returns a [`LiteCertificate`] that owns its signatures and justification chain.
    pub fn cloned(&self) -> LiteCertificate<'static> {
        LiteCertificate {
            value: self.value.clone(),
            round: self.round,
            unlocking_round: self.unlocking_round,
            first_round: self.first_round,
            justification_commitment: self.justification_commitment,
            justification: Cow::Owned(self.justification.as_ref().clone()),
            signatures: Cow::Owned(self.signatures.clone().into_owned()),
        }
    }
}
