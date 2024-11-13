// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

use linera_base::{crypto::Signature, data_types::Round, identifiers::BlobId};
use linera_execution::committee::ValidatorName;
use serde::{
    ser::{Serialize, SerializeStruct, Serializer},
    Deserialize, Deserializer,
};

use super::{
    generic::GenericCertificate, hashed::Hashed, Certificate, CertificateValue,
    HashedCertificateValue,
};
use crate::{
    block::ValidatedBlock,
    data_types::{ExecutedBlock, LiteCertificate, LiteValue},
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

    /// Returns the certificate without the full value.
    pub fn lite_certificate(&self) -> LiteCertificate<'_> {
        LiteCertificate {
            value: self.lite_value(),
            round: self.round,
            signatures: Cow::Borrowed(self.signatures()),
        }
    }

    /// Returns reference to the `ExecutedBlock` contained in this certificate.
    pub fn executed_block(&self) -> &ExecutedBlock {
        self.inner().inner()
    }
}

impl From<Certificate> for GenericCertificate<ValidatedBlock> {
    fn from(cert: Certificate) -> Self {
        let (value, round, signatures) = cert.destructure();
        let value_hash = value.hash();
        match value.into_inner() {
            CertificateValue::ValidatedBlock(validated) => Self::new(
                Hashed::unchecked_new(validated, value_hash),
                round,
                signatures,
            ),
            _ => panic!("Expected a validated block certificate"),
        }
    }
}

impl From<GenericCertificate<ValidatedBlock>> for Certificate {
    fn from(cert: GenericCertificate<ValidatedBlock>) -> Certificate {
        let (value, round, signatures) = cert.destructure();
        Certificate::new(
            HashedCertificateValue::new_validated(value.into_inner().into_inner()),
            round,
            signatures,
        )
    }
}

impl Serialize for GenericCertificate<ValidatedBlock> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("ValidatedBlockCertificate", 4)?;
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
            value: ValidatedBlock,
            round: Round,
            signatures: Vec<(ValidatorName, Signature)>,
        }
        let inner = Inner::deserialize(deserializer)?;
        let validated_hashed: HashedCertificateValue = inner.value.clone().into();
        Ok(Self::new(
            Hashed::unchecked_new(inner.value, validated_hashed.hash()),
            inner.round,
            inner.signatures,
        ))
    }
}
