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
pub use generic::GenericCertificate;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey, ValidatorSignature},
    data_types::{BlockHeight, Epoch, Round},
    identifiers::{BlobId, ChainId},
};
pub use lite::LiteCertificate;
use serde::{Deserialize, Serialize};

use crate::types::{ConfirmedBlock, Timeout, ValidatedBlock};

/// Certificate for a [`ValidatedBlock`] instance.
/// A validated block certificate means the block is valid (but not necessarily finalized yet).
/// Since only one block per round is validated,
/// there can be at most one such certificate in every round.
pub type ValidatedBlockCertificate = GenericCertificate<ValidatedBlock>;

/// Certificate for a [`ConfirmedBlock`] instance.
/// A confirmed block certificate means that the block is finalized:
/// It is the agreed block at that height on that chain.
pub type ConfirmedBlockCertificate = GenericCertificate<ConfirmedBlock>;

/// Certificate for a [`Timeout`] instance.
/// A timeout certificate means that the next consensus round has begun.
pub type TimeoutCertificate = GenericCertificate<Timeout>;

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
            Certificate::Validated(cert) => cert.round,
            Certificate::Confirmed(cert) => cert.round,
            Certificate::Timeout(cert) => cert.round,
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

    /// Returns the retained chain owner's signature over the certified block's
    /// proposal content, if available. Always `None` for timeouts.
    pub fn owner_authorization(&self) -> Option<&crate::data_types::OwnerAuthorization> {
        match self {
            Certificate::Validated(cert) => cert.owner_authorization(),
            Certificate::Confirmed(cert) => cert.owner_authorization(),
            Certificate::Timeout(_) => None,
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
