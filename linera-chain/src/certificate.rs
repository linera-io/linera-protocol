// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;
use std::fmt::{Debug, Formatter};

use crate::data_types::{
    Certificate, CertificateValue, ExecutedBlock, HashedCertificateValue, LiteCertificate,
    LiteValue,
};
use linera_base::crypto::BcsHashable;
use linera_base::{
    crypto::{CryptoHash, Signature},
    data_types::Round,
    identifiers::BlobId,
};
use linera_execution::committee::{Committee, ValidatorName};

use crate::block::{ConfirmedBlock, ValidatedBlock};
use crate::ChainError;

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

impl<T: BcsHashable> Hashed<T> {
    // Note on usage: This method is unsafe because it allows the caller to create a Hashed
    // with a hash that doesn't match the value. This is necessary for the rewrite state when
    // signers sign over old `Certificate` type.
    pub fn unsafe_new(value: T, hash: CryptoHash) -> Self {
        Self { value, hash }
    }

    pub fn new(value: T) -> Self {
        let hash = CryptoHash::new(&value);
        Self { value, hash }
    }

    pub fn hash(&self) -> CryptoHash {
        self.hash
    }
}

impl<T> Hashed<T> {
    pub fn inner(&self) -> &T {
        &self.value
    }

    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T: Debug + BcsHashable> Debug for Hashed<T> {
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
        // Not enforcing no duplicates, check the documentation for is_strictly_ordered
        // It's the responsibility of the caller to make sure signatures has no duplicates
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
            signatures,
            value: Hashed {
                value: validated_block,
                ..
            },
        } = validated;
        let confirmed = ConfirmedBlock::from_validated(validated_block);
        let hashed = Hashed::new(confirmed);

        Self {
            value: hashed,
            round,
            signatures,
        }
    }

    /// Returns reference to the `ExecutedBlock` contained in this certificate.
    pub fn executed_block(&self) -> &ExecutedBlock {
        self.inner().inner()
    }
}
