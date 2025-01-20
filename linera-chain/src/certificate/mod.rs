// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod confirmed;
mod generic;
mod lite;
mod timeout;
mod validated;

use std::collections::HashSet;

pub use generic::GenericCertificate;
use linera_base::{
    crypto::Signature,
    data_types::{BlockHeight, Round},
    identifiers::{BlobId, ChainId},
};
use linera_execution::committee::{Epoch, ValidatorName};
pub use lite::LiteCertificate;
use serde::{Deserialize, Serialize};

use crate::types::{ConfirmedBlock, Timeout, ValidatedBlock};

/// Certificate for a [`ValidatedBlock`]` instance.
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
    pub fn round(&self) -> Round {
        match self {
            Certificate::Validated(cert) => cert.round,
            Certificate::Confirmed(cert) => cert.round,
            Certificate::Timeout(cert) => cert.round,
        }
    }

    pub fn height(&self) -> BlockHeight {
        match self {
            Certificate::Validated(cert) => cert.value().inner().block().header.height,
            Certificate::Confirmed(cert) => cert.value().inner().block().header.height,
            Certificate::Timeout(cert) => cert.inner().height,
        }
    }

    pub fn chain_id(&self) -> ChainId {
        match self {
            Certificate::Validated(cert) => cert.value().inner().block().header.chain_id,
            Certificate::Confirmed(cert) => cert.value().inner().block().header.chain_id,
            Certificate::Timeout(cert) => cert.inner().chain_id,
        }
    }

    pub fn signatures(&self) -> &Vec<(ValidatorName, Signature)> {
        match self {
            Certificate::Validated(cert) => cert.signatures(),
            Certificate::Confirmed(cert) => cert.signatures(),
            Certificate::Timeout(cert) => cert.signatures(),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[repr(u8)]
pub enum CertificateKind {
    Timeout = 0,
    Validated = 1,
    Confirmed = 2,
}

pub trait CertificateValue: Clone {
    const KIND: CertificateKind;

    fn chain_id(&self) -> ChainId;

    fn epoch(&self) -> Epoch;

    fn height(&self) -> BlockHeight;

    fn required_blob_ids(&self) -> HashSet<BlobId>;
}

impl CertificateValue for Timeout {
    const KIND: CertificateKind = CertificateKind::Timeout;

    fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    fn epoch(&self) -> Epoch {
        self.epoch
    }

    fn height(&self) -> BlockHeight {
        self.height
    }

    fn required_blob_ids(&self) -> HashSet<BlobId> {
        HashSet::new()
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

    fn required_blob_ids(&self) -> HashSet<BlobId> {
        self.block().required_blob_ids()
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

    fn required_blob_ids(&self) -> HashSet<BlobId> {
        self.block().required_blob_ids()
    }
}
