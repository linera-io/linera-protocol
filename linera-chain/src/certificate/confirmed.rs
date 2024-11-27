// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::Signature,
    data_types::Round,
    identifiers::{BlobId, ChainId, MessageId},
};
use linera_execution::committee::{Epoch, ValidatorName};
use serde::{Deserialize, Deserializer, Serialize};

use super::{
    generic::GenericCertificate, hashed::Hashed, Certificate, CertificateValue,
    HashedCertificateValue,
};
use crate::{
    block::{ConfirmedBlock, ConversionError, ValidatedBlock},
    data_types::{is_strictly_ordered, ExecutedBlock, Medium, MessageBundle},
};

impl GenericCertificate<ConfirmedBlock> {
    /// Creates a new `ConfirmedBlockCertificate` from a `ValidatedBlockCertificate`.
    pub fn from_validated(validated: GenericCertificate<ValidatedBlock>) -> Self {
        let round = validated.round;
        let validated_block = validated.into_inner();
        // To keep the signature checks passing, we need to obtain a hash over the old type.
        let old_confirmed = HashedCertificateValue::new_confirmed(validated_block.inner().clone());
        let confirmed = ConfirmedBlock::from_validated(validated_block);
        let hashed = Hashed::unchecked_new(confirmed, old_confirmed.hash());

        Self::new(hashed, round, vec![])
    }

    /// Returns reference to the `ExecutedBlock` contained in this certificate.
    pub fn executed_block(&self) -> &ExecutedBlock {
        self.inner().executed_block()
    }

    /// Returns whether this value contains the message with the specified ID.
    pub fn has_message(&self, message_id: &MessageId) -> bool {
        self.executed_block().message_by_id(message_id).is_some()
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
        self.executed_block()
            .message_bundles_for(medium, recipient, certificate_hash)
    }

    pub fn requires_blob(&self, blob_id: &BlobId) -> bool {
        self.executed_block().requires_blob(blob_id)
    }
}

impl TryFrom<Certificate> for GenericCertificate<ConfirmedBlock> {
    type Error = ConversionError;

    fn try_from(cert: Certificate) -> Result<Self, Self::Error> {
        let hash = cert.hash();
        let (value, round, signatures) = cert.destructure();
        match value.into_inner() {
            CertificateValue::ConfirmedBlock(confirmed) => Ok(Self::new(
                Hashed::unchecked_new(confirmed, hash),
                round,
                signatures,
            )),
            _ => Err(ConversionError::ConfirmedBlock),
        }
    }
}

impl From<GenericCertificate<ConfirmedBlock>> for Certificate {
    fn from(cert: GenericCertificate<ConfirmedBlock>) -> Certificate {
        let (value, round, signatures) = cert.destructure();
        let hash = value.hash();
        let value =
            Hashed::unchecked_new(CertificateValue::ConfirmedBlock(value.into_inner()), hash);
        Certificate::new(value, round, signatures)
    }
}

impl Serialize for GenericCertificate<ConfirmedBlock> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Debug, Serialize)]
        #[serde(rename = "ConfirmedBlockCertificate")]
        struct CertificateHelper<'a> {
            value: &'a ConfirmedBlock,
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

impl<'de> Deserialize<'de> for GenericCertificate<ConfirmedBlock> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(rename = "ConfirmedBlockCertificate")]
        struct CertificateHelper {
            value: Hashed<ConfirmedBlock>,
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
