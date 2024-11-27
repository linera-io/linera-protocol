// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod confirmed;
mod generic;
mod hashed;
mod lite;
mod timeout;
mod validated;
mod value;

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
use serde::{Deserialize, Deserializer, Serialize};
pub use value::{CertificateValue, HashedCertificateValue};

use crate::{
    data_types::{is_strictly_ordered, LiteValue, Medium, MessageBundle},
    types::{ConfirmedBlock, Timeout, ValidatedBlock},
};

/// Certificate for a [`ValidatedBlock`]` instance.
pub type ValidatedBlockCertificate = GenericCertificate<ValidatedBlock>;

/// Certificate for a [`ConfirmedBlock`] instance.
pub type ConfirmedBlockCertificate = GenericCertificate<ConfirmedBlock>;

/// Certificate for a [`Timeout`] instance.
pub type TimeoutCertificate = GenericCertificate<Timeout>;

/// A certified statement from the committee.
pub type Certificate = GenericCertificate<CertificateValue>;

impl Serialize for Certificate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Debug, Serialize)]
        #[serde(rename = "Certificate")]
        struct CertificateHelper<'a> {
            value: &'a CertificateValue,
            round: Round,
            signatures: &'a Vec<(ValidatorName, Signature)>,
        }

        let helper = CertificateHelper {
            value: self.inner(),
            round: self.round,
            signatures: self.signatures(),
        };

        helper.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Certificate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(rename = "Certificate")]
        struct CertificateHelper {
            value: HashedCertificateValue,
            round: Round,
            signatures: Vec<(ValidatorName, Signature)>,
        }

        let helper: CertificateHelper = Deserialize::deserialize(deserializer)?;
        if !is_strictly_ordered(&helper.signatures) {
            Err(serde::de::Error::custom("Vector is not strictly sorted"))
        } else {
            Ok(Self::new(helper.value, helper.round, helper.signatures))
        }
    }
}

impl Certificate {
    /// Returns the `LiteValue` corresponding to the certified value.
    pub fn lite_value(&self) -> LiteValue {
        LiteValue {
            value_hash: self.hash(),
            chain_id: self.inner().chain_id(),
        }
    }

    /// Returns the bundles of messages sent via the given medium to the specified
    /// recipient. Messages originating from different transactions of the original block
    /// are kept in separate bundles. If the medium is a channel, does not verify that the
    /// recipient is actually subscribed to that channel.
    pub fn message_bundles_for<'a>(
        &'a self,
        medium: &'a Medium,
        recipient: ChainId,
    ) -> impl Iterator<Item = (Epoch, MessageBundle)> + 'a {
        let certificate_hash = self.hash();
        self.inner()
            .executed_block()
            .into_iter()
            .flat_map(move |executed_block| {
                executed_block.message_bundles_for(medium, recipient, certificate_hash)
            })
    }

    pub fn requires_blob(&self, blob_id: &BlobId) -> bool {
        self.inner()
            .executed_block()
            .is_some_and(|executed_block| executed_block.requires_blob(blob_id))
    }

    #[cfg(with_testing)]
    pub fn outgoing_message_count(&self) -> usize {
        let Some(executed_block) = self.inner().executed_block() else {
            return 0;
        };
        executed_block.messages().iter().map(Vec::len).sum()
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
        self.inner().block.chain_id
    }

    fn epoch(&self) -> Epoch {
        self.inner().block.epoch
    }

    fn height(&self) -> BlockHeight {
        self.inner().block.height
    }

    fn required_blob_ids(&self) -> HashSet<BlobId> {
        self.inner().required_blob_ids()
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
        self.executed_block().required_blob_ids()
    }
}

impl CertificateValueT for CertificateValue {
    fn chain_id(&self) -> ChainId {
        match self {
            CertificateValue::ValidatedBlock(validated) => validated.chain_id(),
            CertificateValue::ConfirmedBlock(confirmed) => confirmed.chain_id(),
            CertificateValue::Timeout(timeout) => timeout.chain_id(),
        }
    }

    fn epoch(&self) -> Epoch {
        match self {
            CertificateValue::ValidatedBlock(validated) => validated.epoch(),
            CertificateValue::ConfirmedBlock(confirmed) => confirmed.epoch(),
            CertificateValue::Timeout(timeout) => timeout.epoch(),
        }
    }

    fn height(&self) -> BlockHeight {
        match self {
            CertificateValue::ValidatedBlock(validated) => validated.height(),
            CertificateValue::ConfirmedBlock(confirmed) => confirmed.height(),
            CertificateValue::Timeout(timeout) => timeout.height(),
        }
    }

    fn required_blob_ids(&self) -> HashSet<BlobId> {
        match self {
            CertificateValue::ValidatedBlock(validated) => validated.required_blob_ids(),
            CertificateValue::ConfirmedBlock(confirmed) => confirmed.required_blob_ids(),
            CertificateValue::Timeout(timeout) => timeout.required_blob_ids(),
        }
    }
}
