// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

use linera_base::{
    crypto::{ValidatorPublicKey, ValidatorSignature},
    data_types::Round,
};
use serde::{
    ser::{Serialize, Serializer},
    Deserialize, Deserializer,
};

use super::{generic::GenericCertificate, Certificate};
use crate::block::{ConversionError, Timeout};

/// The serialized representation of a [`TimeoutCertificate`](super::TimeoutCertificate). Deriving
/// the (de)serialization on this single type keeps both directions in sync and free of manual
/// field bookkeeping; the manual impls only add the strictly-ordered-signatures invariant.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename = "TimeoutCertificate")]
struct Repr<'a> {
    value: Cow<'a, Timeout>,
    round: Round,
    signatures: Cow<'a, [(ValidatorPublicKey, ValidatorSignature)]>,
}

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
        Repr {
            value: Cow::Borrowed(self.inner()),
            round: self.round(),
            signatures: Cow::Borrowed(self.signatures().as_slice()),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GenericCertificate<Timeout> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let repr = Repr::deserialize(deserializer)?;
        let signatures = repr.signatures.into_owned();
        if !crate::data_types::is_strictly_ordered(&signatures) {
            Err(serde::de::Error::custom("Vector is not strictly sorted"))
        } else {
            Ok(Self::new(repr.value.into_owned(), repr.round, signatures))
        }
    }
}
