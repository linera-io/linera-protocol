// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{crypto::CryptoHash, data_types::BlockHeight, ensure, identifiers::ChainId};
use linera_execution::committee::Epoch;
use serde::{Deserialize, Deserializer, Serialize};

use super::Hashed;
#[cfg(with_testing)]
use crate::data_types::OutgoingMessage;
use crate::{
    block::{ConfirmedBlock, Timeout, ValidatedBlock},
    data_types::{Block, ExecutedBlock, LiteValue},
    ChainError,
};

/// A statement to be certified by the validators.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub enum CertificateValue {
    ValidatedBlock(ValidatedBlock),
    ConfirmedBlock(ConfirmedBlock),
    Timeout(Timeout),
}

impl CertificateValue {
    pub fn chain_id(&self) -> ChainId {
        match self {
            CertificateValue::ConfirmedBlock(confirmed) => confirmed.inner().block.chain_id,
            CertificateValue::ValidatedBlock(validated) => validated.inner().block.chain_id,
            CertificateValue::Timeout(Timeout { chain_id, .. }) => *chain_id,
        }
    }

    pub fn height(&self) -> BlockHeight {
        match self {
            CertificateValue::ConfirmedBlock(confirmed) => confirmed.inner().block.height,
            CertificateValue::ValidatedBlock(validated) => validated.inner().block.height,
            CertificateValue::Timeout(Timeout { height, .. }) => *height,
        }
    }

    pub fn epoch(&self) -> Epoch {
        match self {
            CertificateValue::ConfirmedBlock(confirmed) => confirmed.inner().block.epoch,
            CertificateValue::ValidatedBlock(validated) => validated.inner().block.epoch,
            CertificateValue::Timeout(Timeout { epoch, .. }) => *epoch,
        }
    }

    /// Creates a `HashedCertificateValue` checking that this is the correct hash.
    pub fn with_hash_checked(self, hash: CryptoHash) -> Result<HashedCertificateValue, ChainError> {
        let hashed_certificate_value = self.with_hash();
        ensure!(
            hashed_certificate_value.hash() == hash,
            ChainError::CertificateValueHashMismatch {
                expected: hash,
                actual: hashed_certificate_value.hash()
            }
        );
        Ok(hashed_certificate_value)
    }

    /// Creates a `HashedCertificateValue` by hashing `self`. No hash checks are made!
    pub fn with_hash(self) -> HashedCertificateValue {
        let hash = CryptoHash::new(&self);
        HashedCertificateValue::unchecked_new(self, hash)
    }

    /// Creates a `HashedCertificateValue` without checking that this is the correct hash!
    pub fn with_hash_unchecked(self, hash: CryptoHash) -> HashedCertificateValue {
        HashedCertificateValue::unchecked_new(self, hash)
    }

    pub fn is_confirmed(&self) -> bool {
        matches!(self, CertificateValue::ConfirmedBlock { .. })
    }

    pub fn is_validated(&self) -> bool {
        matches!(self, CertificateValue::ValidatedBlock { .. })
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, CertificateValue::Timeout { .. })
    }

    #[cfg(with_testing)]
    pub fn messages(&self) -> Option<&Vec<Vec<OutgoingMessage>>> {
        Some(self.executed_block()?.messages())
    }

    pub fn executed_block(&self) -> Option<&ExecutedBlock> {
        match self {
            CertificateValue::ConfirmedBlock(confirmed) => Some(confirmed.inner()),
            CertificateValue::ValidatedBlock(validated) => Some(validated.inner()),
            CertificateValue::Timeout(_) => None,
        }
    }

    pub fn block(&self) -> Option<&Block> {
        self.executed_block()
            .map(|executed_block| &executed_block.block)
    }

    pub fn to_log_str(&self) -> &'static str {
        match self {
            CertificateValue::ConfirmedBlock { .. } => "confirmed_block",
            CertificateValue::ValidatedBlock { .. } => "validated_block",
            CertificateValue::Timeout { .. } => "timeout",
        }
    }
}

#[async_graphql::Object(cache_control(no_cache))]
impl CertificateValue {
    #[graphql(derived(name = "executed_block"))]
    async fn _executed_block(&self) -> Option<ExecutedBlock> {
        self.executed_block().cloned()
    }

    async fn status(&self) -> String {
        match self {
            CertificateValue::ValidatedBlock { .. } => "validated".to_string(),
            CertificateValue::ConfirmedBlock { .. } => "confirmed".to_string(),
            CertificateValue::Timeout { .. } => "timeout".to_string(),
        }
    }
}

/// A statement to be certified by the validators, with its hash.
pub type HashedCertificateValue = Hashed<CertificateValue>;

impl HashedCertificateValue {
    /// Creates a [`ConfirmedBlock`](CertificateValue::ConfirmedBlock) value.
    pub fn new_confirmed(executed_block: ExecutedBlock) -> HashedCertificateValue {
        CertificateValue::ConfirmedBlock(ConfirmedBlock::new(executed_block)).into()
    }

    /// Creates a [`ValidatedBlock`](CertificateValue::ValidatedBlock) value.
    pub fn new_validated(executed_block: ExecutedBlock) -> HashedCertificateValue {
        CertificateValue::ValidatedBlock(ValidatedBlock::new(executed_block)).into()
    }

    /// Creates a [`Timeout`](CertificateValue::Timeout) value.
    pub fn new_timeout(
        chain_id: ChainId,
        height: BlockHeight,
        epoch: Epoch,
    ) -> HashedCertificateValue {
        CertificateValue::Timeout(Timeout::new(chain_id, height, epoch)).into()
    }

    pub fn lite(&self) -> LiteValue {
        LiteValue {
            value_hash: self.hash(),
            chain_id: self.inner().chain_id(),
        }
    }

    /// Returns the corresponding `ConfirmedBlock`, if this is a `ValidatedBlock`.
    pub fn validated_to_confirmed(&self) -> Option<HashedCertificateValue> {
        match self.inner() {
            CertificateValue::ValidatedBlock(validated) => {
                Some(ConfirmedBlock::from_validated(validated.clone()).into())
            }
            CertificateValue::ConfirmedBlock(_) | CertificateValue::Timeout(_) => None,
        }
    }
}

impl Serialize for HashedCertificateValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner().serialize(serializer)
    }
}

impl<'a> Deserialize<'a> for HashedCertificateValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        Ok(CertificateValue::deserialize(deserializer)?.into())
    }
}

impl From<CertificateValue> for HashedCertificateValue {
    fn from(value: CertificateValue) -> HashedCertificateValue {
        value.with_hash()
    }
}

impl From<HashedCertificateValue> for CertificateValue {
    fn from(hv: HashedCertificateValue) -> CertificateValue {
        hv.into_inner()
    }
}

#[async_graphql::Object(cache_control(no_cache))]
impl HashedCertificateValue {
    #[graphql(derived(name = "hash"))]
    async fn _hash(&self) -> CryptoHash {
        self.hash()
    }

    #[graphql(derived(name = "value"))]
    async fn _value(&self) -> CertificateValue {
        self.inner().clone()
    }
}
