// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::Deref;

use allocative::Allocative;
use linera_base::{
    crypto::{ValidatorPublicKey, ValidatorSignature},
    data_types::{Epoch, Round},
    ensure,
    identifiers::ChainId,
};
use linera_execution::committee::Committee;
use serde::{ser::SerializeStruct, Deserialize, Deserializer, Serialize};

use super::{generic::GenericCertificate, Certificate, Certified, LiteCertificate};
use crate::{
    block::{Block, ConfirmedBlock, ConversionError},
    data_types::MessageBundle,
    justification::JustificationChain,
    ChainError,
};

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

    /// Verifies the certificate: the quorum's `ConfirmedBlock` signatures, the validated chain,
    /// and that the chain heads at the confirm round (or is empty in the fast round).
    pub fn check(&self, committee: &Committee) -> Result<(), ChainError> {
        self.quorum.check(committee)?;
        self.validated.verify(self.hash(), committee)?;
        match self.validated.links().first() {
            None => ensure!(
                self.round().is_fast(),
                ChainError::JustificationLockMismatch
            ),
            Some(link) => ensure!(
                link.round == self.round(),
                ChainError::JustificationLockMismatch
            ),
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
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ConfirmedBlockCertificate", 4)?;
        state.serialize_field("value", self.quorum.inner())?;
        state.serialize_field("round", &self.quorum.round())?;
        state.serialize_field("signatures", self.quorum.signatures())?;
        state.serialize_field("validated", &self.validated)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for ConfirmedBlockCertificate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(rename = "ConfirmedBlockCertificate")]
        struct Helper {
            value: ConfirmedBlock,
            round: Round,
            signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
            validated: JustificationChain,
        }

        let helper = Helper::deserialize(deserializer)?;
        if !crate::data_types::is_strictly_ordered(&helper.signatures) {
            Err(serde::de::Error::custom("Vector is not strictly sorted"))
        } else {
            Ok(Self::from_parts(
                GenericCertificate::new(helper.value, helper.round, helper.signatures),
                helper.validated,
            ))
        }
    }
}
