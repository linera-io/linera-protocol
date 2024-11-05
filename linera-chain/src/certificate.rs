// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Cow,
    fmt::{Debug, Formatter},
};

use linera_base::{
    crypto::{BcsHashable, CryptoHash, Signature},
    data_types::Round,
    identifiers::BlobId,
};
use linera_execution::committee::{Committee, ValidatorName};
use serde::{
    ser::{Serialize, SerializeStruct, Serializer},
    Deserialize, Deserializer,
};

use crate::{
    block::{ConfirmedBlock, ValidatedBlock},
    data_types::{
        Certificate, CertificateValue, ExecutedBlock, HashedCertificateValue, LiteCertificate,
        LiteValue,
    },
    ChainError,
};

pub type ValidatedBlockCertificate = CertificateT<ValidatedBlock>;

pub type ConfirmedBlockCertificate = CertificateT<ConfirmedBlock>;

/// Generic type representing a certificate for `value` of type `T`.
pub struct CertificateT<T> {
    value: Hashed<T>,
    pub round: Round,
    signatures: Vec<(ValidatorName, Signature)>,
}

impl<T> CertificateT<T> {
    /// Returns reference to the value contained in this certificate.
    pub fn inner(&self) -> &T {
        self.value.inner()
    }

    /// Consumes this certificate, returning the value it contains.
    pub fn into_inner(self) -> T {
        self.value.value
    }
}

impl<T: Clone> Clone for CertificateT<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            round: self.round,
            signatures: self.signatures.clone(),
        }
    }
}

impl<T: Debug + BcsHashable> Debug for CertificateT<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertificateT")
            .field("value", &self.value)
            .field("round", &self.round)
            .field("signatures", &self.signatures)
            .finish()
    }
}

impl<T: Serialize> Serialize for CertificateT<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let value = self.value.inner();
        let hash = self.value.hash();
        let round = self.round;
        let signatures = &self.signatures;
        let mut state = serializer.serialize_struct("Certificate", 4)?;
        state.serialize_field("value", value)?;
        state.serialize_field("hash", &hash)?;
        state.serialize_field("round", &round)?;
        state.serialize_field("signatures", signatures)?;
        state.end()
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for CertificateT<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        {
            #[derive(Debug, Deserialize)]
            #[serde(rename = "Certificate")]
            struct CertificateHelper<T2> {
                value: Hashed<T2>,
                round: Round,
                signatures: Vec<(ValidatorName, Signature)>,
            }

            let helper: CertificateHelper<T> = Deserialize::deserialize(deserializer)?;
            if !crate::data_types::is_strictly_ordered(&helper.signatures) {
                Err(serde::de::Error::custom("Vector is not strictly sorted"))
            } else {
                Ok(Self {
                    value: helper.value,
                    round: helper.round,
                    signatures: helper.signatures,
                })
            }
        }
    }
}

#[cfg(with_testing)]
impl<T: Eq + PartialEq> Eq for CertificateT<T> {}
#[cfg(with_testing)]
impl<T: Eq + PartialEq> PartialEq for CertificateT<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value.hash == other.value.hash
            && self.round == other.round
            && self.signatures == other.signatures
    }
}

impl From<ConfirmedBlockCertificate> for Certificate {
    fn from(cert: ConfirmedBlockCertificate) -> Certificate {
        let ConfirmedBlockCertificate {
            value,
            round,
            signatures,
        } = cert;
        Certificate::new(
            HashedCertificateValue::new_confirmed(value.into_inner().into_inner()),
            round,
            signatures,
        )
    }
}

impl From<ValidatedBlockCertificate> for Certificate {
    fn from(cert: ValidatedBlockCertificate) -> Certificate {
        let ValidatedBlockCertificate {
            value,
            round,
            signatures,
        } = cert;
        Certificate::new(
            HashedCertificateValue::new_validated(value.into_inner().into_inner()),
            round,
            signatures,
        )
    }
}

// In practice, it should be HashedCertificateValue = Hashed<CertificateValue>
// but [`HashedCertificateValue`] is used in too many places to change it now.
pub struct Hashed<T> {
    value: T,
    hash: CryptoHash,
}

// NOTE: We shouldn't serialize the hash and expect the deserializer to compute it.
// We implement it this way for backwards-compatibility with the old `Certificate` type.
// If we were to compute `hash` in the deserializer, we would need to do map `T` to corresponding
// variants of `CertificateValue` and then compute the hash. This would require a lot of boilerplate
// and is not possible to do on generic `T` type.
impl<T: Serialize> Serialize for Hashed<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Hashed { value, hash } = self;
        let mut state = serializer.serialize_struct("Hashed", 2)?;
        state.serialize_field("value", &value)?;
        state.serialize_field("hash", &hash)?;
        state.end()
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Hashed<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        {
            #[derive(Deserialize)]
            #[serde(rename = "Hashed")]
            struct HashedHelper<T2> {
                value: T2,
                hash: CryptoHash,
            }

            let helper: HashedHelper<T> = Deserialize::deserialize(deserializer)?;
            Ok(Self {
                value: helper.value,
                hash: helper.hash,
            })
        }
    }
}

impl<T> Hashed<T> {
    // Note on usage: This method is unsafe because it allows the caller to create a Hashed
    // with a hash that doesn't match the value. This is necessary for the rewrite state when
    // signers sign over old `Certificate` type.
    pub fn unsafe_new(value: T, hash: CryptoHash) -> Self {
        Self { value, hash }
    }

    pub fn hash(&self) -> CryptoHash {
        self.hash
    }

    pub fn inner(&self) -> &T {
        &self.value
    }

    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T: Debug> Debug for Hashed<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashedT")
            .field("value", &self.value)
            .field("hash", &self.hash())
            .finish()
    }
}

impl<T: Clone> Clone for Hashed<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            hash: self.hash,
        }
    }
}

impl<T: BcsHashable> CertificateT<T> {
    pub fn new(
        value: T,
        old_hash: CryptoHash,
        round: Round,
        mut signatures: Vec<(ValidatorName, Signature)>,
    ) -> Self {
        signatures.sort_by_key(|&(validator_name, _)| validator_name);
        Self {
            value: Hashed::unsafe_new(value, old_hash),
            round,
            signatures,
        }
    }

    pub fn signatures(&self) -> &Vec<(ValidatorName, Signature)> {
        &self.signatures
    }

    // Adds a signature to the certificate's list of signatures
    // It's the responsibility of the caller to not insert duplicates
    pub fn add_signature(
        &mut self,
        signature: (ValidatorName, Signature),
    ) -> &Vec<(ValidatorName, Signature)> {
        let index = self
            .signatures
            .binary_search_by(|(name, _)| name.cmp(&signature.0))
            .unwrap_or_else(std::convert::identity);
        self.signatures.insert(index, signature);
        &self.signatures
    }

    /// Returns whether the validator is among the signatories of this certificate.
    pub fn is_signed_by(&self, validator_name: &ValidatorName) -> bool {
        self.signatures
            .binary_search_by(|(name, _)| name.cmp(validator_name))
            .is_ok()
    }
}

impl<T: BcsHashable> CertificateT<T> {
    /// Returns the certified value's hash.
    pub fn hash(&self) -> CryptoHash {
        self.value.hash()
    }

    /// Verifies the certificate.
    pub fn check(&self, committee: &Committee) -> Result<(), ChainError> {
        crate::data_types::check_signatures(self.hash(), self.round, &self.signatures, committee)?;
        Ok(())
    }
}

impl ValidatedBlockCertificate {
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
            value_hash: self.value.hash(),
            chain_id: self.executed_block().block.chain_id,
        }
    }

    /// Returns the certificate without the full value.
    pub fn lite_certificate(&self) -> LiteCertificate<'_> {
        LiteCertificate {
            value: self.lite_value(),
            round: self.round,
            signatures: Cow::Borrowed(&self.signatures),
        }
    }

    /// Returns reference to the `ExecutedBlock` contained in this certificate.
    pub fn executed_block(&self) -> &ExecutedBlock {
        self.inner().inner()
    }
}

impl From<Certificate> for ValidatedBlockCertificate {
    fn from(cert: Certificate) -> Self {
        let signatures = cert.signatures().clone();
        let hash = cert.value.hash();
        match cert.value.into_inner() {
            CertificateValue::ValidatedBlock { executed_block } => Self {
                value: Hashed::unsafe_new(ValidatedBlock::new(executed_block), hash),
                round: cert.round,
                signatures,
            },
            _ => panic!("Expected a validated block certificate"),
        }
    }
}

impl ConfirmedBlockCertificate {
    /// Creates a new `ConfirmedBlockCertificate` from a `ValidatedBlockCertificate`.
    pub fn from_validated(validated: ValidatedBlockCertificate) -> Self {
        let ValidatedBlockCertificate {
            round,
            value: Hashed {
                value: validated_block,
                ..
            },
            ..
        } = validated;
        // To keep the signature checks passing, we need to obtain a hash over the old type.
        let old_confirmed = HashedCertificateValue::new_confirmed(validated_block.inner().clone());
        let confirmed = ConfirmedBlock::from_validated(validated_block);
        let hashed = Hashed::unsafe_new(confirmed, old_confirmed.hash());

        Self {
            value: hashed,
            round,
            signatures: vec![], // Signatures were cast for validated block certificate.
        }
    }

    /// Returns reference to the `ExecutedBlock` contained in this certificate.
    pub fn executed_block(&self) -> &ExecutedBlock {
        self.inner().inner()
    }
}
