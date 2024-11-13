// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{crypto::Signature, data_types::Round};
use linera_execution::committee::ValidatorName;
use serde::{
    ser::{Serialize, SerializeStruct, Serializer},
    Deserialize, Deserializer,
};

use super::{generic::GenericCertificate, hashed::Hashed, Certificate};
use crate::{
    block::Timeout,
    data_types::{CertificateValue, HashedCertificateValue},
};

impl From<Certificate> for GenericCertificate<Timeout> {
    fn from(cert: Certificate) -> Self {
        let hash = cert.hash();
        let (value, round, signatures) = cert.destructure();
        match value.into_inner() {
            CertificateValue::Timeout(timeout) => {
                Self::new(Hashed::unchecked_new(timeout, hash), round, signatures)
            }
            _ => panic!("Expected a timeout certificate"),
        }
    }
}

impl From<GenericCertificate<Timeout>> for Certificate {
    fn from(cert: GenericCertificate<Timeout>) -> Certificate {
        let (value, round, signatures) = cert.destructure();
        Certificate::new(
            HashedCertificateValue::new_timeout(
                value.inner().chain_id,
                value.inner().height,
                value.inner().epoch,
            ),
            round,
            signatures,
        )
    }
}

impl Serialize for GenericCertificate<Timeout> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("TimeoutCertificate", 4)?;
        state.serialize_field("value", self.inner())?;
        state.serialize_field("round", &self.round)?;
        state.serialize_field("signatures", self.signatures())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for GenericCertificate<Timeout> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "TimeoutCertificate")]
        struct Inner {
            value: Timeout,
            round: Round,
            signatures: Vec<(ValidatorName, Signature)>,
        }
        let inner = Inner::deserialize(deserializer)?;
        let timeout_hashed: HashedCertificateValue = inner.value.clone().into();
        Ok(Self::new(
            Hashed::unchecked_new(inner.value, timeout_hashed.hash()),
            inner.round,
            inner.signatures,
        ))
    }
}
