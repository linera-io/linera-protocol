// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::{BcsHashable, CryptoHash},
    data_types::BlockHeight,
    identifiers::ChainId,
};
use linera_execution::committee::Epoch;
use serde::{Deserialize, Serialize};

use super::Hashed;
#[cfg(with_testing)]
use crate::data_types::OutgoingMessage;
use crate::{
    block::{ConfirmedBlock, Timeout, ValidatedBlock},
    data_types::{Block, ExecutedBlock},
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

impl BcsHashable for CertificateValue {}

/// A statement to be certified by the validators, with its hash.
pub type HashedCertificateValue = Hashed<CertificateValue>;

impl HashedCertificateValue {
    /// Creates a [`ConfirmedBlock`](CertificateValue::ConfirmedBlock) value.
    pub fn new_confirmed(executed_block: ExecutedBlock) -> Hashed<ConfirmedBlock> {
        HashedCertificateValue::from(CertificateValue::ConfirmedBlock(ConfirmedBlock::new(
            executed_block,
        )))
        .try_into()
        .unwrap()
    }

    /// Creates a [`ValidatedBlock`](CertificateValue::ValidatedBlock) value.
    pub fn new_validated(executed_block: ExecutedBlock) -> Hashed<ValidatedBlock> {
        HashedCertificateValue::from(CertificateValue::ValidatedBlock(ValidatedBlock::new(
            executed_block,
        )))
        .try_into()
        .unwrap()
    }

    /// Creates a [`Timeout`](CertificateValue::Timeout) value.
    pub fn new_timeout(chain_id: ChainId, height: BlockHeight, epoch: Epoch) -> Hashed<Timeout> {
        HashedCertificateValue::from(CertificateValue::Timeout(Timeout::new(
            chain_id, height, epoch,
        )))
        .try_into()
        .unwrap()
    }

    /// Returns the corresponding `ConfirmedBlock`, if this is a `ValidatedBlock`.
    pub fn validated_to_confirmed(self) -> Option<HashedCertificateValue> {
        match self.into_inner() {
            CertificateValue::ValidatedBlock(validated) => {
                Some(HashedCertificateValue::new_confirmed(validated.into_inner()).into())
            }
            CertificateValue::ConfirmedBlock(_) | CertificateValue::Timeout(_) => None,
        }
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

impl From<Hashed<ConfirmedBlock>> for HashedCertificateValue {
    fn from(confirmed: Hashed<ConfirmedBlock>) -> HashedCertificateValue {
        let hash = confirmed.hash();
        let value = confirmed.into_inner();
        Hashed::unchecked_new(CertificateValue::ConfirmedBlock(value), hash)
    }
}

impl From<Hashed<ValidatedBlock>> for HashedCertificateValue {
    fn from(validated: Hashed<ValidatedBlock>) -> HashedCertificateValue {
        let hash = validated.hash();
        let value = validated.into_inner();
        Hashed::unchecked_new(CertificateValue::ValidatedBlock(value), hash)
    }
}

impl From<Hashed<Timeout>> for HashedCertificateValue {
    fn from(timeout: Hashed<Timeout>) -> HashedCertificateValue {
        let hash = timeout.hash();
        let value = timeout.into_inner();
        Hashed::unchecked_new(CertificateValue::Timeout(value), hash)
    }
}
