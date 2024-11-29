// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{crypto::Signature, data_types::Round, identifiers::BlobId};
use linera_execution::committee::ValidatorName;
use serde::{
    ser::{Serialize, SerializeStruct, Serializer},
    Deserialize, Deserializer,
};

use super::{generic::GenericCertificate, Certificate};
use crate::{
    block::{ConversionError, ValidatedBlock},
    data_types::{ExecutedBlock, LiteValue},
    types::Hashed,
};

impl GenericCertificate<ValidatedBlock> {
    pub fn requires_blob(&self, blob_id: &BlobId) -> bool {
        self.executed_block().requires_blob(blob_id)
    }

    #[cfg(with_testing)]
    pub fn outgoing_message_count(&self) -> usize {
        self.executed_block().messages().iter().map(Vec::len).sum()
    }

    /// Returns the `LiteValue` corresponding to the certified value.
    pub fn lite_value(&self) -> LiteValue {
        LiteValue {
            value_hash: self.hash(),
            chain_id: self.executed_block().block.chain_id,
        }
    }

    /// Returns reference to the `ExecutedBlock` contained in this certificate.
    pub fn executed_block(&self) -> &ExecutedBlock {
        self.inner().executed_block()
    }
}

impl TryFrom<Certificate> for GenericCertificate<ValidatedBlock> {
    type Error = ConversionError;

    fn try_from(cert: Certificate) -> Result<Self, Self::Error> {
        match cert {
            Certificate::Validated(validated) => Ok(validated),
            _ => Err(ConversionError::ValidatedBlock),
        }
    }
}

impl From<GenericCertificate<ValidatedBlock>> for Certificate {
    fn from(cert: GenericCertificate<ValidatedBlock>) -> Certificate {
        Certificate::Validated(cert)
    }
}

impl Serialize for GenericCertificate<ValidatedBlock> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("ValidatedBlockCertificate", 3)?;
        state.serialize_field("value", self.inner())?;
        state.serialize_field("round", &self.round)?;
        state.serialize_field("signatures", self.signatures())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for GenericCertificate<ValidatedBlock> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "ValidatedBlockCertificate")]
        struct Inner {
            value: Hashed<ValidatedBlock>,
            round: Round,
            signatures: Vec<(ValidatorName, Signature)>,
        }
        let inner = Inner::deserialize(deserializer)?;
        if !crate::data_types::is_strictly_ordered(&inner.signatures) {
            Err(serde::de::Error::custom(
                "Signatures are not strictly ordered",
            ))
        } else {
            Ok(Self::new(inner.value, inner.round, inner.signatures))
        }
    }
}
