// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use custom_debug_derive::Debug;
use linera_base::{
    crypto::{CryptoHash, Signature},
    data_types::Round,
    hashed::Hashed,
};
use linera_execution::committee::{Committee, ValidatorName};

use super::CertificateValue;
use crate::{data_types::LiteValue, ChainError};

/// Generic type representing a certificate for `value` of type `T`.
#[derive(Debug)]
pub struct GenericCertificate<T> {
    value: Hashed<T>,
    pub round: Round,
    signatures: Vec<(ValidatorName, Signature)>,
}

impl<T> GenericCertificate<T> {
    pub fn new(
        value: Hashed<T>,
        round: Round,
        mut signatures: Vec<(ValidatorName, Signature)>,
    ) -> Self {
        signatures.sort_by_key(|&(validator_name, _)| validator_name);

        Self {
            value,
            round,
            signatures,
        }
    }

    /// Returns a reference to the `Hashed` value contained in this certificate.
    pub fn value(&self) -> &Hashed<T> {
        &self.value
    }

    /// Consumes this certificate, returning the value it contains.
    pub fn into_value(self) -> Hashed<T> {
        self.value
    }

    /// Returns reference to the value contained in this certificate.
    pub fn inner(&self) -> &T {
        self.value.inner()
    }

    /// Consumes this certificate, returning the value it contains.
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }

    /// Returns the certified value's hash.
    pub fn hash(&self) -> CryptoHash {
        self.value.hash()
    }

    pub fn destructure(self) -> (Hashed<T>, Round, Vec<(ValidatorName, Signature)>) {
        (self.value, self.round, self.signatures)
    }

    pub fn signatures(&self) -> &Vec<(ValidatorName, Signature)> {
        &self.signatures
    }

    #[cfg(with_testing)]
    pub fn signatures_mut(&mut self) -> &mut Vec<(ValidatorName, Signature)> {
        &mut self.signatures
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

    /// Verifies the certificate.
    pub fn check(&self, committee: &Committee) -> Result<(), ChainError>
    where
        T: CertificateValue,
    {
        crate::data_types::check_signatures(
            self.hash(),
            T::KIND,
            self.round,
            &self.signatures,
            committee,
        )?;
        Ok(())
    }

    pub fn lite_certificate(&self) -> crate::certificate::LiteCertificate<'_>
    where
        T: CertificateValue,
    {
        crate::certificate::LiteCertificate {
            value: LiteValue::new(&self.value),
            round: self.round,
            signatures: std::borrow::Cow::Borrowed(&self.signatures),
        }
    }
}

impl<T: Clone> Clone for GenericCertificate<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            round: self.round,
            signatures: self.signatures.clone(),
        }
    }
}

#[cfg(with_testing)]
impl<T: Eq + PartialEq> Eq for GenericCertificate<T> {}
#[cfg(with_testing)]
impl<T: Eq + PartialEq> PartialEq for GenericCertificate<T> {
    fn eq(&self, other: &Self) -> bool {
        self.hash() == other.hash()
            && self.round == other.round
            && self.signatures == other.signatures
    }
}
