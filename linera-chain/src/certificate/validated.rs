// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{crypto::Signature, data_types::Round, hashed::Hashed, identifiers::BlobId};
use linera_execution::committee::ValidatorName;
use serde::{
    ser::{Serialize, SerializeStruct, Serializer},
    Deserialize, Deserializer,
};

use super::{generic::GenericCertificate, Certificate};
use crate::block::{Block, ConversionError, ValidatedBlock};

impl GenericCertificate<ValidatedBlock> {
    pub fn requires_blob(&self, blob_id: &BlobId) -> bool {
        self.block().requires_blob(blob_id)
    }

    #[cfg(with_testing)]
    pub fn outgoing_message_count(&self) -> usize {
        self.block().messages().iter().map(Vec::len).sum()
    }

    /// Returns reference to the [`Block`] contained in this certificate.
    pub fn block(&self) -> &Block {
        self.inner().block()
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
