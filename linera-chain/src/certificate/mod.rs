// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod confirmed;
mod generic;
mod lite;
mod timeout;
mod validated;

use std::collections::BTreeSet;

use allocative::Allocative;
pub use confirmed::ConfirmedBlockCertificate;
pub use generic::GenericCertificate;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey, ValidatorSignature},
    data_types::{BlockHeight, Epoch, Round},
    identifiers::{BlobId, ChainId},
};
use linera_execution::committee::Committee;
pub use lite::LiteCertificate;
use serde::{Deserialize, Serialize};
pub use validated::ValidatedBlockCertificate;

use crate::{
    types::{ConfirmedBlock, Timeout, ValidatedBlock},
    ChainError,
};

/// Certificate for a [`Timeout`] instance.
/// A timeout certificate means that the next consensus round has begun.
pub type TimeoutCertificate = GenericCertificate<Timeout>;

/// The common read interface shared by all certificate types: the signed value, the round and
/// lock it was certified under, its signatures, and verification.
pub trait Certified {
    /// The kind of value this certificate certifies.
    type Value: CertificateValue;

    /// Returns a reference to the certified value.
    fn value(&self) -> &Self::Value;

    /// Returns the round in which the value was certified.
    fn round(&self) -> Round;

    /// Returns the lock round `ℓ` the `ValidatedBlock` voters signed, if any.
    fn lock(&self) -> Option<Round>;

    /// Returns the validator signatures certifying this value.
    fn signatures(&self) -> &Vec<(ValidatorPublicKey, ValidatorSignature)>;

    /// Returns the certified value's hash.
    fn hash(&self) -> CryptoHash {
        self.value().hash()
    }

    /// Returns the [`LiteCertificate`] corresponding to this certificate, without the value but
    /// with the justification chain.
    fn lite_certificate(&self) -> LiteCertificate<'_>;

    /// Verifies the certificate, including its justification chain.
    fn check(&self, committee: &Committee) -> Result<(), ChainError>;

    /// Returns whether the validator is among the signatories of this certificate.
    fn is_signed_by(&self, validator_name: &ValidatorPublicKey) -> bool {
        self.signatures()
            .binary_search_by(|(name, _)| name.cmp(validator_name))
            .is_ok()
    }
}

impl<T: CertificateValue> Certified for GenericCertificate<T> {
    type Value = T;

    fn value(&self) -> &T {
        GenericCertificate::value(self)
    }

    fn round(&self) -> Round {
        GenericCertificate::round(self)
    }

    fn lock(&self) -> Option<Round> {
        GenericCertificate::lock(self)
    }

    fn signatures(&self) -> &Vec<(ValidatorPublicKey, ValidatorSignature)> {
        GenericCertificate::signatures(self)
    }

    fn lite_certificate(&self) -> LiteCertificate<'_> {
        GenericCertificate::lite_certificate(self)
    }

    fn check(&self, committee: &Committee) -> Result<(), ChainError> {
        GenericCertificate::check(self, committee)
    }
}

/// Enum wrapping all types of certificates that can be created.
/// A certified statement from the committee.
/// Every certificate is a statement signed by the quorum of the committee.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub enum Certificate {
    /// Certificate for [`ValidatedBlock`].
    Validated(ValidatedBlockCertificate),
    /// Certificate for [`ConfirmedBlock`].
    Confirmed(ConfirmedBlockCertificate),
    /// Certificate for [`Timeout`].
    Timeout(TimeoutCertificate),
}

impl Certificate {
    /// Returns the consensus round in which this certificate was created.
    pub fn round(&self) -> Round {
        match self {
            Certificate::Validated(cert) => cert.round(),
            Certificate::Confirmed(cert) => cert.round(),
            Certificate::Timeout(cert) => cert.round(),
        }
    }

    /// Returns the block height this certificate applies to.
    pub fn height(&self) -> BlockHeight {
        match self {
            Certificate::Validated(cert) => cert.value().block().header.height,
            Certificate::Confirmed(cert) => cert.value().block().header.height,
            Certificate::Timeout(cert) => cert.value().height(),
        }
    }

    /// Returns the ID of the chain this certificate applies to.
    pub fn chain_id(&self) -> ChainId {
        match self {
            Certificate::Validated(cert) => cert.value().block().header.chain_id,
            Certificate::Confirmed(cert) => cert.value().block().header.chain_id,
            Certificate::Timeout(cert) => cert.value().chain_id(),
        }
    }

    /// Returns the validator signatures that certify this value.
    pub fn signatures(&self) -> &Vec<(ValidatorPublicKey, ValidatorSignature)> {
        match self {
            Certificate::Validated(cert) => cert.signatures(),
            Certificate::Confirmed(cert) => cert.signatures(),
            Certificate::Timeout(cert) => cert.signatures(),
        }
    }
}

/// The kind of value a certificate certifies.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Hash, Eq, PartialEq, Allocative)]
#[repr(u8)]
#[allow(missing_docs)]
pub enum CertificateKind {
    Timeout = 0,
    Validated = 1,
    Confirmed = 2,
}

/// A value that can be certified by a quorum of validators.
pub trait CertificateValue: Clone {
    /// The kind of certificate this value produces.
    const KIND: CertificateKind;

    /// Returns the ID of the chain this value applies to.
    fn chain_id(&self) -> ChainId;

    /// Returns the epoch this value belongs to.
    fn epoch(&self) -> Epoch;

    /// Returns the block height this value applies to.
    fn height(&self) -> BlockHeight;

    /// Returns the IDs of all blobs required to validate this value.
    fn required_blob_ids(&self) -> BTreeSet<BlobId>;

    /// Returns the hash that uniquely identifies this value.
    fn hash(&self) -> CryptoHash;
}

impl CertificateValue for Timeout {
    const KIND: CertificateKind = CertificateKind::Timeout;

    fn chain_id(&self) -> ChainId {
        self.chain_id()
    }

    fn epoch(&self) -> Epoch {
        self.epoch()
    }

    fn height(&self) -> BlockHeight {
        self.height()
    }

    fn required_blob_ids(&self) -> BTreeSet<BlobId> {
        BTreeSet::new()
    }

    fn hash(&self) -> CryptoHash {
        self.inner().hash()
    }
}

impl CertificateValue for ValidatedBlock {
    const KIND: CertificateKind = CertificateKind::Validated;

    fn chain_id(&self) -> ChainId {
        self.block().header.chain_id
    }

    fn epoch(&self) -> Epoch {
        self.block().header.epoch
    }

    fn height(&self) -> BlockHeight {
        self.block().header.height
    }

    fn required_blob_ids(&self) -> BTreeSet<BlobId> {
        self.block().required_blob_ids()
    }

    fn hash(&self) -> CryptoHash {
        self.inner().hash()
    }
}

impl CertificateValue for ConfirmedBlock {
    const KIND: CertificateKind = CertificateKind::Confirmed;

    fn chain_id(&self) -> ChainId {
        self.block().header.chain_id
    }

    fn epoch(&self) -> Epoch {
        self.block().header.epoch
    }

    fn height(&self) -> BlockHeight {
        self.block().header.height
    }

    fn required_blob_ids(&self) -> BTreeSet<BlobId> {
        self.block().required_blob_ids()
    }

    fn hash(&self) -> CryptoHash {
        self.inner().hash()
    }
}
