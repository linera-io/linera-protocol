// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, ops::Deref};

use allocative::Allocative;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey, ValidatorSignature},
    data_types::Round,
};
use linera_execution::committee::Committee;
use serde::{
    ser::{Serialize, Serializer},
    Deserialize, Deserializer,
};

use super::{generic::GenericCertificate, Certificate, Certified, LiteCertificate};
use crate::{
    block::{Block, ConversionError, ValidatedBlock},
    justification::JustificationChain,
    ChainError,
};

/// The serialized representation of a [`ValidatedBlockCertificate`]. Deriving the
/// (de)serialization on this single type keeps both directions in sync and free of manual field
/// bookkeeping; the manual impls only add the strictly-ordered-signatures invariant.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename = "ValidatedBlockCertificate")]
struct Repr<'a> {
    value: Cow<'a, ValidatedBlock>,
    round: Round,
    unlocking_round: Option<Round>,
    justification_commitment: Option<CryptoHash>,
    signatures: Cow<'a, [(ValidatorPublicKey, ValidatorSignature)]>,
    justification: Cow<'a, JustificationChain>,
}

/// Certificate for a [`ValidatedBlock`] instance, certified in some round whose `ValidatedBlock`
/// voters signed an unlocking round.
///
/// A validated block certificate means the block is valid (but not necessarily finalized yet).
/// Since only one block per round is validated, there can be at most one such certificate in
/// every round. It wraps the signed quorum and carries the justification chain that grounds the
/// unlocking round the voters signed.
#[derive(Clone, Debug, Allocative)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct ValidatedBlockCertificate {
    /// The signed quorum of `ValidatedBlock` votes. Its unlocking round equals
    /// `justification.top_unlocking_round()`.
    quorum: GenericCertificate<ValidatedBlock>,
    /// The chain of validated quorums for the same block in rounds below this certificate's,
    /// rising from the grounding round to its top link in the unlocking round. Empty iff
    /// the unlocking round is `None`.
    justification: JustificationChain,
}

impl ValidatedBlockCertificate {
    /// Creates a validated block certificate with an empty justification chain (unlocking round `None`).
    pub fn new(
        value: ValidatedBlock,
        round: Round,
        signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    ) -> Self {
        Self {
            quorum: GenericCertificate::new(value, round, signatures),
            justification: JustificationChain::default(),
        }
    }

    /// Creates a validated block certificate from a signed quorum and its justification chain.
    pub fn from_parts(
        quorum: GenericCertificate<ValidatedBlock>,
        justification: JustificationChain,
    ) -> Self {
        Self {
            quorum,
            justification,
        }
    }

    /// Returns the signed quorum of `ValidatedBlock` votes.
    pub fn quorum(&self) -> &GenericCertificate<ValidatedBlock> {
        &self.quorum
    }

    /// Returns the chain of validated quorums in rounds below this certificate's.
    pub fn justification(&self) -> &JustificationChain {
        &self.justification
    }

    /// Consumes this certificate, returning the signed quorum and the justification chain.
    pub fn into_parts(self) -> (GenericCertificate<ValidatedBlock>, JustificationChain) {
        (self.quorum, self.justification)
    }

    /// Returns the round in which the value was certified.
    pub fn round(&self) -> Round {
        self.quorum.round()
    }

    /// Consumes this certificate, returning the validated block it contains.
    pub fn into_value(self) -> ValidatedBlock {
        self.quorum.into_value()
    }

    /// Consumes this certificate, returning the validated block it contains.
    pub fn into_inner(self) -> ValidatedBlock {
        self.quorum.into_inner()
    }

    /// Returns the full justification chain that a certificate certified in a higher round on
    /// top of this one would carry: the chain below it, with this certificate's own quorum
    /// appended as the new top link.
    pub fn full_justification(&self) -> JustificationChain {
        self.justification
            .append(self.quorum.round(), self.quorum.signatures().clone())
    }

    /// Returns the justification commitment that a vote citing this certificate signs: the hash
    /// of this certificate's own quorum as a [`CommittedQuorum`], which transitively commits to
    /// the chain below it. Equals [`full_justification`](Self::full_justification)'s commitment.
    ///
    /// [`CommittedQuorum`]: crate::justification::CommittedQuorum
    pub fn full_justification_commitment(&self) -> CryptoHash {
        crate::justification::CommittedQuorum {
            value_hash: self.hash(),
            round: self.quorum.round(),
            unlocking_round: self.quorum.unlocking_round(),
            previous: self.quorum.justification_commitment(),
            signatures: self.quorum.signatures().clone(),
        }
        .commitment()
    }

    /// Verifies the certificate: its signatures, its justification chain, that the unlocking round
    /// matches the top of the chain, and that the chain lies in rounds strictly below this
    /// certificate's. Delegates to [`LiteCertificate::check`], the single source of truth for the
    /// quorum-to-chain binding.
    pub fn check(&self, committee: &Committee) -> Result<(), ChainError> {
        self.lite_certificate().check(committee)?;
        Ok(())
    }

    /// Returns the [`LiteCertificate`] corresponding to this certificate, borrowing the chain.
    pub fn lite_certificate(&self) -> LiteCertificate<'_> {
        let mut lite = self.quorum.lite_certificate_without_justification();
        lite.justification = Cow::Borrowed(&self.justification);
        lite
    }
}

impl Deref for ValidatedBlockCertificate {
    type Target = GenericCertificate<ValidatedBlock>;

    fn deref(&self) -> &Self::Target {
        &self.quorum
    }
}

impl Certified for ValidatedBlockCertificate {
    type Value = ValidatedBlock;

    fn value(&self) -> &ValidatedBlock {
        self.quorum.value()
    }

    fn round(&self) -> Round {
        ValidatedBlockCertificate::round(self)
    }

    fn unlocking_round(&self) -> Option<Round> {
        self.quorum.unlocking_round()
    }

    fn signatures(&self) -> &Vec<(ValidatorPublicKey, ValidatorSignature)> {
        self.quorum.signatures()
    }

    fn lite_certificate(&self) -> LiteCertificate<'_> {
        ValidatedBlockCertificate::lite_certificate(self)
    }

    fn check(&self, committee: &Committee) -> Result<(), ChainError> {
        ValidatedBlockCertificate::check(self, committee)
    }
}

impl GenericCertificate<ValidatedBlock> {
    /// Returns the total number of outgoing messages in the certified block.
    #[cfg(with_testing)]
    pub fn outgoing_message_count(&self) -> usize {
        self.block().messages().iter().map(Vec::len).sum()
    }

    /// Returns reference to the [`Block`] contained in this certificate.
    pub fn block(&self) -> &Block {
        self.inner().block()
    }
}

impl TryFrom<Certificate> for ValidatedBlockCertificate {
    type Error = ConversionError;

    fn try_from(cert: Certificate) -> Result<Self, Self::Error> {
        match cert {
            Certificate::Validated(validated) => Ok(validated),
            _ => Err(ConversionError::ValidatedBlock),
        }
    }
}

impl From<ValidatedBlockCertificate> for Certificate {
    fn from(cert: ValidatedBlockCertificate) -> Certificate {
        Certificate::Validated(cert)
    }
}

impl Serialize for ValidatedBlockCertificate {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Repr {
            value: Cow::Borrowed(self.quorum.inner()),
            round: self.quorum.round(),
            unlocking_round: self.quorum.unlocking_round(),
            justification_commitment: self.quorum.justification_commitment(),
            signatures: Cow::Borrowed(self.quorum.signatures().as_slice()),
            justification: Cow::Borrowed(&self.justification),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ValidatedBlockCertificate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inner = Repr::deserialize(deserializer)?;
        let signatures = inner.signatures.into_owned();
        if !crate::data_types::is_strictly_ordered(&signatures) {
            Err(serde::de::Error::custom(
                "Signatures are not strictly ordered",
            ))
        } else {
            Ok(Self::from_parts(
                GenericCertificate::new_with_payload(
                    inner.value.into_owned(),
                    inner.round,
                    inner.unlocking_round,
                    false,
                    inner.justification_commitment,
                    signatures,
                ),
                inner.justification.into_owned(),
            ))
        }
    }
}
