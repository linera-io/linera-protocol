// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;

use linera_base::{crypto::BcsHashable, data_types::BlockHeight, identifiers::ChainId};
use linera_execution::committee::Epoch;
use serde::{Deserialize, Serialize};

use crate::{
    data_types::ExecutedBlock,
    types::{CertificateValue, Has, Hashed, HashedCertificateValue},
};

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
    pub fn inner(&self) -> &ExecutedBlock {
        &self.executed_block
    }

    /// Consumes this `ValidatedBlock`, returning the `ExecutedBlock` it contains.
    pub fn into_inner(self) -> ExecutedBlock {
        self.executed_block
    }

    pub fn to_log_str(&self) -> &'static str {
        "validated_block"
    }
}

impl BcsHashable for ValidatedBlock {}

impl Has<ChainId> for ValidatedBlock {
    fn get(&self) -> ChainId {
        self.executed_block.block.chain_id
    }
}

impl TryFrom<HashedCertificateValue> for Hashed<ValidatedBlock> {
    type Error = &'static str;

    fn try_from(value: HashedCertificateValue) -> Result<Self, Self::Error> {
        let hash = value.hash();
        match value.into_inner() {
            CertificateValue::ValidatedBlock(validated) => {
                Ok(Hashed::unchecked_new(validated, hash))
            }
            _ => Err("Expected a ValidatedBlock value"),
        }
    }
}

/// Wrapper around an `ExecutedBlock` that has been confirmed.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub struct ConfirmedBlock {
    // The executed block contained in this `ConfirmedBlock`.
    executed_block: ExecutedBlock,
}

impl TryFrom<HashedCertificateValue> for Hashed<ConfirmedBlock> {
    type Error = &'static str;

    fn try_from(value: HashedCertificateValue) -> Result<Self, Self::Error> {
        let hash = value.hash();
        match value.into_inner() {
            CertificateValue::ConfirmedBlock(confirmed) => {
                Ok(Hashed::unchecked_new(confirmed, hash))
            }
            _ => Err("Expected a ConfirmedBlock value"),
        }
    }
}

impl BcsHashable for ConfirmedBlock {}

impl ConfirmedBlock {
    #[cfg(not(feature = "benchmark"))]
    pub(super) fn new(executed_block: ExecutedBlock) -> Self {
        Self { executed_block }
    }

    #[cfg(feature = "benchmark")]
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
    pub fn inner(&self) -> &ExecutedBlock {
        &self.executed_block
    }

    /// Consumes this `ConfirmedBlock`, returning the `ExecutedBlock` it contains.
    pub fn into_inner(self) -> ExecutedBlock {
        self.executed_block
    }

    pub fn to_log_str(&self) -> &'static str {
        "confirmed_block"
    }
}

impl Has<ChainId> for ConfirmedBlock {
    fn get(&self) -> ChainId {
        self.executed_block.block.chain_id
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
}

impl TryFrom<HashedCertificateValue> for Hashed<Timeout> {
    type Error = &'static str;
    fn try_from(value: HashedCertificateValue) -> Result<Hashed<Timeout>, Self::Error> {
        let hash = value.hash();
        match value.into_inner() {
            CertificateValue::Timeout(timeout) => Ok(Hashed::unchecked_new(timeout, hash)),
            _ => Err("Expected a Timeout value"),
        }
    }
}

impl BcsHashable for Timeout {}

impl Has<ChainId> for Timeout {
    fn get(&self) -> ChainId {
        self.chain_id
    }
}
