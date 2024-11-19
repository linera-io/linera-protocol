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

use std::borrow::Cow;

pub use generic::GenericCertificate;
pub use hashed::{Has, Hashed, IsValidated, RequiredBlobIds};
use linera_base::{
    crypto::Signature,
    data_types::Round,
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
    /// Returns the certificate without the full value.
    pub fn lite_certificate(&self) -> LiteCertificate<'_> {
        LiteCertificate {
            value: self.lite_value(),
            round: self.round,
            signatures: Cow::Borrowed(self.signatures()),
        }
    }

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
