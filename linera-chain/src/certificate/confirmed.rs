// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, ops::Deref};

use allocative::Allocative;
use linera_base::{
    crypto::{ValidatorPublicKey, ValidatorSignature},
    data_types::{Epoch, Round},
    ensure,
    identifiers::ChainId,
};
use linera_execution::committee::Committee;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{generic::GenericCertificate, Certificate, Certified, LiteCertificate};
use crate::{
    block::{Block, ConfirmedBlock, ConversionError},
    data_types::MessageBundle,
    justification::JustificationChain,
    ChainError,
};

/// The serialized representation of a [`ConfirmedBlockCertificate`]. Deriving the
/// (de)serialization on this single type keeps both directions in sync and free of manual field
/// bookkeeping; the manual impls only add the strictly-ordered-signatures invariant.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename = "ConfirmedBlockCertificate")]
struct Repr<'a> {
    value: Cow<'a, ConfirmedBlock>,
    round: Round,
    first_round: bool,
    signatures: Cow<'a, [(ValidatorPublicKey, ValidatorSignature)]>,
    validated: Cow<'a, JustificationChain>,
}

/// Certificate for a [`ConfirmedBlock`] instance, certified in some round by a quorum of
/// `ConfirmedBlock` votes (which carry no lock).
///
/// A confirmed block certificate means that the block is finalized: it is the agreed block at
/// that height on that chain. It wraps the signed quorum and carries the full chain of validated
/// quorums for the block, making it self-contained evidence for fault attribution.
#[derive(Clone, Debug, Allocative)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct ConfirmedBlockCertificate {
    /// The signed quorum of `ConfirmedBlock` votes. Its lock is always `None`.
    quorum: GenericCertificate<ConfirmedBlock>,
    /// The full chain of validated quorums for the block, with its top link in the round the
    /// block was confirmed, descending to the grounding round. Empty iff the block was confirmed
    /// in the fast round.
    validated: JustificationChain,
}

impl ConfirmedBlockCertificate {
    /// Creates a confirmed block certificate with an empty justification chain (fast round).
    pub fn new(
        value: ConfirmedBlock,
        round: Round,
        signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    ) -> Self {
        Self {
            quorum: GenericCertificate::new(value, round, signatures),
            validated: JustificationChain::default(),
        }
    }

    /// Creates a confirmed block certificate from a signed quorum and its justification chain.
    pub fn from_parts(
        quorum: GenericCertificate<ConfirmedBlock>,
        validated: JustificationChain,
    ) -> Self {
        Self { quorum, validated }
    }

    /// Returns the signed quorum of `ConfirmedBlock` votes.
    pub fn quorum(&self) -> &GenericCertificate<ConfirmedBlock> {
        &self.quorum
    }

    /// Returns the full chain of validated quorums for the block.
    pub fn validated(&self) -> &JustificationChain {
        &self.validated
    }

    /// Consumes this certificate, returning the signed quorum and the justification chain.
    pub fn into_parts(self) -> (GenericCertificate<ConfirmedBlock>, JustificationChain) {
        (self.quorum, self.validated)
    }

    /// Returns the round in which the value was certified.
    pub fn round(&self) -> Round {
        self.quorum.round()
    }

    /// Consumes this certificate, returning the confirmed block it contains.
    pub fn into_value(self) -> ConfirmedBlock {
        self.quorum.into_value()
    }

    /// Consumes this certificate, returning the confirmed block it contains.
    pub fn into_inner(self) -> ConfirmedBlock {
        self.quorum.into_inner()
    }

    /// Verifies the certificate's signatures and justification chain: the quorum of
    /// `ConfirmedBlock` votes, that the validated chain is itself a valid chain of quorums, and
    /// that — if present — it heads at the confirmation round.
    ///
    /// An *absent* chain is accepted only when the quorum carries the first-round attestation
    /// (a fast-round confirmation always does, since the fast round is a chain's first round):
    /// such a block is always the lower block in any fork, so its own chain is never needed to
    /// attribute one. The attestation is also sanity-checked here against the round — it can only
    /// be set in a round that could be a chain's first one. Whether it is the *actual* first
    /// round depends on the chain's ownership at that height, which a committee-only check cannot
    /// know; that, and the obligation of a later-round block to carry its chain, rest on honest
    /// block construction (see `finalize_block`), full-execution verification, and the
    /// per-signature justifications retained by the commitment scheme.
    pub fn check(&self, committee: &Committee) -> Result<(), ChainError> {
        self.quorum.check(committee)?;
        self.validated.verify(self.hash(), committee)?;
        // The first-round attestation can only be set in a round that could be a chain's first:
        // the fast round, or the index-0 round of whichever round type the chain starts with —
        // a single- or multi-leader chain begins at `SingleLeader(0)`/`MultiLeader(0)`, and a
        // validators-only chain at `Validator(0)`.
        if self.quorum().first_round() {
            ensure!(
                matches!(
                    self.round(),
                    Round::Fast
                        | Round::MultiLeader(0)
                        | Round::SingleLeader(0)
                        | Round::Validator(0)
                ),
                ChainError::JustificationLockMismatch
            );
        }
        if self.validated().links().is_empty() {
            ensure!(
                self.quorum().first_round(),
                ChainError::JustificationLockMismatch
            );
        } else {
            ensure!(
                self.validated().links()[0].round == self.round(),
                ChainError::JustificationLockMismatch
            );
        }
        Ok(())
    }

    /// Returns the [`LiteCertificate`] corresponding to this certificate, carrying the chain.
    pub fn lite_certificate(&self) -> LiteCertificate<'_> {
        let mut lite = self.quorum.lite_certificate();
        lite.justification = self.validated.clone();
        lite
    }
}

impl Deref for ConfirmedBlockCertificate {
    type Target = GenericCertificate<ConfirmedBlock>;

    fn deref(&self) -> &Self::Target {
        &self.quorum
    }
}

impl Certified for ConfirmedBlockCertificate {
    type Value = ConfirmedBlock;

    fn value(&self) -> &ConfirmedBlock {
        self.quorum.value()
    }

    fn round(&self) -> Round {
        ConfirmedBlockCertificate::round(self)
    }

    fn lock(&self) -> Option<Round> {
        self.quorum.lock()
    }

    fn signatures(&self) -> &Vec<(ValidatorPublicKey, ValidatorSignature)> {
        self.quorum.signatures()
    }

    fn lite_certificate(&self) -> LiteCertificate<'_> {
        ConfirmedBlockCertificate::lite_certificate(self)
    }

    fn check(&self, committee: &Committee) -> Result<(), ChainError> {
        ConfirmedBlockCertificate::check(self, committee)
    }
}

impl GenericCertificate<ConfirmedBlock> {
    /// Returns reference to the `Block` contained in this certificate.
    pub fn block(&self) -> &Block {
        self.inner().block()
    }

    /// Returns the bundles of messages sent to the specified recipient.
    /// Messages originating from different transactions of the original block
    /// are kept in separate bundles.
    pub fn message_bundles_for(
        &self,
        recipient: ChainId,
    ) -> impl Iterator<Item = (Epoch, MessageBundle)> + '_ {
        let certificate_hash = self.hash();
        self.block()
            .message_bundles_for(recipient, certificate_hash)
    }

    /// Returns the total number of outgoing messages in the certified block.
    #[cfg(with_testing)]
    pub fn outgoing_message_count(&self) -> usize {
        self.block().messages().iter().map(Vec::len).sum()
    }
}

impl TryFrom<Certificate> for ConfirmedBlockCertificate {
    type Error = ConversionError;

    fn try_from(cert: Certificate) -> Result<Self, Self::Error> {
        match cert {
            Certificate::Confirmed(confirmed) => Ok(confirmed),
            _ => Err(ConversionError::ConfirmedBlock),
        }
    }
}

impl From<ConfirmedBlockCertificate> for Certificate {
    fn from(cert: ConfirmedBlockCertificate) -> Certificate {
        Certificate::Confirmed(cert)
    }
}

impl From<&ConfirmedBlockCertificate> for Certificate {
    fn from(cert: &ConfirmedBlockCertificate) -> Certificate {
        Certificate::Confirmed(cert.clone())
    }
}

impl Serialize for ConfirmedBlockCertificate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Repr {
            value: Cow::Borrowed(self.quorum.inner()),
            round: self.quorum.round(),
            first_round: self.quorum.first_round(),
            signatures: Cow::Borrowed(self.quorum.signatures().as_slice()),
            validated: Cow::Borrowed(&self.validated),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ConfirmedBlockCertificate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let helper = Repr::deserialize(deserializer)?;
        let signatures = helper.signatures.into_owned();
        if !crate::data_types::is_strictly_ordered(&signatures) {
            Err(serde::de::Error::custom("Vector is not strictly sorted"))
        } else {
            Ok(Self::from_parts(
                GenericCertificate::new_with_lock_and_first_round(
                    helper.value.into_owned(),
                    helper.round,
                    None,
                    helper.first_round,
                    signatures,
                ),
                helper.validated.into_owned(),
            ))
        }
    }
}
