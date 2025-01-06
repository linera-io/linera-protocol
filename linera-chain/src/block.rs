// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;

use linera_base::{
    crypto::{BcsHashable, CryptoHash},
    data_types::BlockHeight,
    hashed::Hashed,
    identifiers::ChainId,
};
use linera_execution::committee::Epoch;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{data_types::ExecutedBlock, ChainError};

/// Wrapper around an `ExecutedBlock` that has been validated.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ValidatedBlock(Hashed<ExecutedBlock>);

impl ValidatedBlock {
    /// Creates a new `ValidatedBlock` from an `ExecutedBlock`.
    pub fn new(executed_block: ExecutedBlock) -> Self {
        Self(Hashed::new(executed_block))
    }

    pub fn from_hashed(executed_block: Hashed<ExecutedBlock>) -> Self {
        Self(executed_block)
    }

    pub fn inner(&self) -> &Hashed<ExecutedBlock> {
        &self.0
    }

    /// Returns a reference to the `ExecutedBlock` contained in this `ValidatedBlock`.
    pub fn executed_block(&self) -> &ExecutedBlock {
        self.0.inner()
    }

    /// Consumes this `ValidatedBlock`, returning the `ExecutedBlock` it contains.
    pub fn into_inner(self) -> ExecutedBlock {
        self.0.into_inner()
    }

    pub fn to_log_str(&self) -> &'static str {
        "validated_block"
    }

    pub fn chain_id(&self) -> ChainId {
        self.0.inner().block.chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.0.inner().block.height
    }

    pub fn epoch(&self) -> Epoch {
        self.0.inner().block.epoch
    }
}

impl<'de> BcsHashable<'de> for ValidatedBlock {}

/// Wrapper around an `ExecutedBlock` that has been confirmed.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConfirmedBlock(Hashed<ExecutedBlock>);

#[async_graphql::Object(cache_control(no_cache))]
impl ConfirmedBlock {
    #[graphql(derived(name = "executed_block"))]
    async fn _executed_block(&self) -> ExecutedBlock {
        self.0.inner().clone()
    }

    async fn status(&self) -> String {
        "confirmed".to_string()
    }
}

impl<'de> BcsHashable<'de> for ConfirmedBlock {}

impl ConfirmedBlock {
    pub fn new(executed_block: ExecutedBlock) -> Self {
        Self(Hashed::new(executed_block))
    }

    pub fn from_hashed(executed_block: Hashed<ExecutedBlock>) -> Self {
        Self(executed_block)
    }

    pub fn inner(&self) -> &Hashed<ExecutedBlock> {
        &self.0
    }

    /// Returns a reference to the `ExecutedBlock` contained in this `ConfirmedBlock`.
    pub fn executed_block(&self) -> &ExecutedBlock {
        self.0.inner()
    }

    /// Consumes this `ConfirmedBlock`, returning the `ExecutedBlock` it contains.
    pub fn into_inner(self) -> ExecutedBlock {
        self.0.into_inner()
    }

    pub fn chain_id(&self) -> ChainId {
        self.0.inner().block.chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.0.inner().block.height
    }

    pub fn to_log_str(&self) -> &'static str {
        "confirmed_block"
    }

    /// Creates a `HashedCertificateValue` without checking that this is the correct hash!
    pub fn with_hash_unchecked(self, hash: CryptoHash) -> Hashed<ConfirmedBlock> {
        Hashed::unchecked_new(self, hash)
    }

    fn with_hash(self) -> Hashed<Self> {
        let hash = CryptoHash::new(&self);
        Hashed::unchecked_new(self, hash)
    }

    /// Creates a `HashedCertificateValue` checking that this is the correct hash.
    pub fn with_hash_checked(self, hash: CryptoHash) -> Result<Hashed<ConfirmedBlock>, ChainError> {
        let hashed_certificate_value = self.with_hash();
        if hashed_certificate_value.hash() == hash {
            Ok(hashed_certificate_value)
        } else {
            Err(ChainError::CertificateValueHashMismatch {
                expected: hash,
                actual: hashed_certificate_value.hash(),
            })
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub struct Timeout {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub epoch: Epoch,
}

impl Timeout {
    pub fn new(chain_id: ChainId, height: BlockHeight, epoch: Epoch) -> Self {
        Self {
            chain_id,
            height,
            epoch,
        }
    }

    pub fn to_log_str(&self) -> &'static str {
        "timeout"
    }

    pub fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.height
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
}

impl<'de> BcsHashable<'de> for Timeout {}

/// Failure to convert a `Certificate` into one of the expected certificate types.
#[derive(Clone, Copy, Debug, Error)]
pub enum ConversionError {
    /// Failure to convert to [`ConfirmedBlock`] certificate.
    #[error("Expected a `ConfirmedBlockCertificate` value")]
    ConfirmedBlock,

    /// Failure to convert to [`ValidatedBlock`] certificate.
    #[error("Expected a `ValidatedBlockCertificate` value")]
    ValidatedBlock,

    /// Failure to convert to [`Timeout`] certificate.
    #[error("Expected a `TimeoutCertificate` value")]
    Timeout,
}
