// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Cow,
    fmt::{Debug, Formatter},
};

use linera_base::{
    crypto::{CryptoHash, Signature},
    data_types::Round,
    identifiers::BlobId,
};
use linera_execution::committee::{Committee, ValidatorName};
use serde::{ser::Serializer, Deserialize, Deserializer, Serialize};

use crate::{
    block::{ConfirmedBlock, ValidatedBlock},
    data_types::{
        Certificate, CertificateValue, ExecutedBlock, HashedCertificateValue, LiteCertificate,
        LiteValue,
    },
    ChainError,
};

/// Certificate for a [`ValidatedBlock`]` instance.
pub type ValidatedBlockCertificate = CertificateT<ValidatedBlock>;

/// Certificate for a [`ConfirmedBlock`] instance.
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

impl<T: Debug> Debug for CertificateT<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertificateT")
            .field("value", &self.value)
            .field("round", &self.round)
            .field("signatures", &self.signatures)
            .finish()
    }
}

// NOTE: For backwards compatiblity reasons we serialize the new `Certificate` type as the old
// one. Otherwise we would be breaking the RPC API schemas. We can't implement generic serialization
// for `Certificate<T>` since only specific `T`s have corresponding `CertificateValue` variants.
impl Serialize for ValidatedBlockCertificate {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let cert = Certificate::from(self.clone());
        cert.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ValidatedBlockCertificate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Certificate::deserialize(deserializer).map(ValidatedBlockCertificate::from)
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

// TODO(#2842): In practice, it should be HashedCertificateValue = Hashed<CertificateValue>
// but [`HashedCertificateValue`] is used in too many places to change it now.
/// Wrapper type around hashed instance of `T` type.
pub struct Hashed<T> {
    value: T,
    hash: CryptoHash,
}

impl<T> Hashed<T> {
    /// Creates an instance of [`Hashed`] with the given `hash` value.
    ///
    /// Note on usage: This method is unsafe because it allows the caller to create a Hashed
    /// with a hash that doesn't match the value. This is necessary for the rewrite state when
    /// signers sign over old `Certificate` type.
    pub fn unchecked_new(value: T, hash: CryptoHash) -> Self {
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

impl<T> CertificateT<T> {
    pub fn new(
        value: T,
        old_hash: CryptoHash,
        round: Round,
        mut signatures: Vec<(ValidatorName, Signature)>,
    ) -> Self {
        signatures.sort_by_key(|&(validator_name, _)| validator_name);
        Self {
            value: Hashed::unchecked_new(value, old_hash),
            round,
            signatures,
        }
    }

    pub fn signatures(&self) -> &Vec<(ValidatorName, Signature)> {
        &self.signatures
    }

    /// Adds a signature to the certificate's list of signatures
    /// It's the responsibility of the caller to not insert duplicates
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

impl<T> CertificateT<T> {
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
                value: Hashed::unchecked_new(ValidatedBlock::new(executed_block), hash),
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
        let round = validated.round;
        let validated_block = validated.value.into_inner();
        // To keep the signature checks passing, we need to obtain a hash over the old type.
        let old_confirmed = HashedCertificateValue::new_confirmed(validated_block.inner().clone());
        let confirmed = ConfirmedBlock::from_validated(validated_block);
        let hashed = Hashed::unchecked_new(confirmed, old_confirmed.hash());

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
