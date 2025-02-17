// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{crypto::Signature, data_types::Round, hashed::Hashed};
use linera_execution::committee::ValidatorName;
use serde::{
    ser::{Serialize, SerializeStruct, Serializer},
    Deserialize, Deserializer,
};

use super::{generic::GenericCertificate, Certificate};
use crate::block::{ConversionError, Timeout};

impl TryFrom<Certificate> for GenericCertificate<Timeout> {
    type Error = ConversionError;

    fn try_from(cert: Certificate) -> Result<Self, Self::Error> {
        match cert {
            Certificate::Timeout(timeout) => Ok(timeout),
            _ => Err(ConversionError::Timeout),
        }
    }
}

impl From<GenericCertificate<Timeout>> for Certificate {
    fn from(cert: GenericCertificate<Timeout>) -> Certificate {
        Certificate::Timeout(cert)
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
            value: Hashed<Timeout>,
            round: Round,
            signatures: Vec<(ValidatorName, Signature)>,
        }
        let inner = Inner::deserialize(deserializer)?;
        if !crate::data_types::is_strictly_ordered(&inner.signatures) {
            Err(serde::de::Error::custom("Vector is not strictly sorted"))
        } else {
            Ok(Self::new(inner.value, inner.round, inner.signatures))
        }
    }
}
