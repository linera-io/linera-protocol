// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;

use linera_base::{
    crypto::{BcsHashable, CryptoHash},
    data_types::BlockHeight,
    identifiers::ChainId,
};
use linera_execution::committee::Epoch;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{data_types::ExecutedBlock, types::Hashed, ChainError};

/// Wrapper around an `ExecutedBlock` that has been validated.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub struct ValidatedBlock {
    executed_block: ExecutedBlock,
}

impl ValidatedBlock {
    /// Creates a new `ValidatedBlock` from an `ExecutedBlock`.
    pub fn new(executed_block: ExecutedBlock) -> Self {
        Self { executed_block }
    }

    /// Returns a reference to the `ExecutedBlock` contained in this `ValidatedBlock`.
    pub fn executed_block(&self) -> &ExecutedBlock {
        &self.executed_block
    }

    /// Consumes this `ValidatedBlock`, returning the `ExecutedBlock` it contains.
    pub fn into_inner(self) -> ExecutedBlock {
        self.executed_block
    }

    pub fn to_log_str(&self) -> &'static str {
        "validated_block"
    }

    pub fn chain_id(&self) -> ChainId {
        self.executed_block.block.chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.executed_block.block.height
    }

    pub fn epoch(&self) -> Epoch {
        self.executed_block.block.epoch
    }
}

impl BcsHashable for ValidatedBlock {}

/// Wrapper around an `ExecutedBlock` that has been confirmed.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub struct ConfirmedBlock {
    // The executed block contained in this `ConfirmedBlock`.
    executed_block: ExecutedBlock,
}

#[async_graphql::Object(cache_control(no_cache))]
impl ConfirmedBlock {
    #[graphql(derived(name = "executed_block"))]
    async fn _executed_block(&self) -> ExecutedBlock {
        self.executed_block.clone()
    }

    async fn status(&self) -> String {
        "confirmed".to_string()
    }
}

#[async_graphql::Object(cache_control(no_cache), name_type)]
impl Hashed<ConfirmedBlock> {
    #[graphql(derived(name = "hash"))]
    async fn _hash(&self) -> CryptoHash {
        self.hash()
    }

    #[graphql(derived(name = "value"))]
    async fn _value(&self) -> ConfirmedBlock {
        self.inner().clone()
    }
}

impl BcsHashable for ConfirmedBlock {}

impl ConfirmedBlock {
    pub fn new(executed_block: ExecutedBlock) -> Self {
        Self { executed_block }
    }

    /// Creates a new `ConfirmedBlock` from a `ValidatedBlock`.
    /// Note: There's no `new` method for `ConfirmedBlock` because it's
    /// only created from a `ValidatedBlock`.
    pub fn from_validated(validated: ValidatedBlock) -> Self {
        Self {
            executed_block: validated.executed_block,
        }
    }

    /// Returns a reference to the `ExecutedBlock` contained in this `ConfirmedBlock`.
    pub fn executed_block(&self) -> &ExecutedBlock {
        &self.executed_block
    }

    /// Consumes this `ConfirmedBlock`, returning the `ExecutedBlock` it contains.
    pub fn into_inner(self) -> ExecutedBlock {
        self.executed_block
    }

    pub fn chain_id(&self) -> ChainId {
        self.executed_block.block.chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.executed_block.block.height
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

impl BcsHashable for Timeout {}

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
