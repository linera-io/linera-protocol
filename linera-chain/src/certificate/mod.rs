// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod confirmed;
mod generic;
mod hashed;
mod lite;
mod timeout;
mod validated;

use std::collections::HashSet;

pub use generic::GenericCertificate;
pub use hashed::Hashed;
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
pub type ValidatedBlockCertificate = GenericCertificate<ValidatedBlock>;

/// Certificate for a [`ConfirmedBlock`] instance.
pub type ConfirmedBlockCertificate = GenericCertificate<ConfirmedBlock>;

/// Certificate for a [`Timeout`] instance.
pub type TimeoutCertificate = GenericCertificate<Timeout>;

/// Enum wrapping all types of certificates that can be created.
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
            Certificate::Validated(cert) => cert.value().inner().executed_block().block.height,
            Certificate::Confirmed(cert) => cert.value().inner().executed_block().block.height,
            Certificate::Timeout(cert) => cert.inner().height,
        }
    }

    pub fn chain_id(&self) -> ChainId {
        match self {
            Certificate::Validated(cert) => cert.value().inner().executed_block().block.chain_id,
            Certificate::Confirmed(cert) => cert.value().inner().executed_block().block.chain_id,
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

pub trait CertificateValueT: Clone {
    fn chain_id(&self) -> ChainId;

    fn epoch(&self) -> Epoch;

    fn height(&self) -> BlockHeight;

    fn required_blob_ids(&self) -> HashSet<BlobId>;

    #[cfg(with_testing)]
    fn is_validated(&self) -> bool {
        false
    }
}

impl CertificateValueT for Timeout {
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

impl CertificateValueT for ValidatedBlock {
    fn chain_id(&self) -> ChainId {
        self.executed_block().block.chain_id
    }

    fn epoch(&self) -> Epoch {
        self.executed_block().block.epoch
    }

    fn height(&self) -> BlockHeight {
        self.executed_block().block.height
    }

    fn required_blob_ids(&self) -> HashSet<BlobId> {
        self.executed_block().outcome.required_blob_ids().clone()
    }

    #[cfg(with_testing)]
    fn is_validated(&self) -> bool {
        true
    }
}

impl CertificateValueT for ConfirmedBlock {
    fn chain_id(&self) -> ChainId {
        self.executed_block().block.chain_id
    }

    fn epoch(&self) -> Epoch {
        self.executed_block().block.epoch
    }

    fn height(&self) -> BlockHeight {
        self.executed_block().block.height
    }

    fn required_blob_ids(&self) -> HashSet<BlobId> {
        self.executed_block().outcome.required_blob_ids().clone()
    }
}
