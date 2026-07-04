// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use allocative::{Allocative, Key, Visitor};
use custom_debug_derive::Debug;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey, ValidatorSignature},
    data_types::Round,
};
use linera_execution::committee::Committee;

use super::CertificateValue;
use crate::{
    data_types::{LiteValue, OwnerAuthorization},
    ChainError,
};

/// Generic type representing a certificate for `value` of type `T`.
#[derive(Debug)]
pub struct GenericCertificate<T: CertificateValue> {
    value: T,
    /// The round in which the value was certified.
    pub round: Round,
    signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    /// The chain owner's signature over the certified block's proposal content.
    /// Not covered by the certified value's hash or the validator signatures.
    /// Required for blocks with an `authenticated_owner`; optional otherwise.
    /// Always `None` for timeouts.
    owner_authorization: Option<OwnerAuthorization>,
}

impl<T: Allocative + CertificateValue> Allocative for GenericCertificate<T> {
    fn visit<'a, 'b: 'a>(&self, visitor: &'a mut Visitor<'b>) {
        visitor.visit_field(Key::new("GenericCertificate_value"), &self.value);
        visitor.visit_field(Key::new("GenericCertificate_round"), &self.round);
        for (public_key, signature) in &self.signatures {
            visitor.visit_field(Key::new("ValidatorPublicKey"), public_key);
            visitor.visit_field(Key::new("ValidatorSignature"), signature);
        }
        if let Some(authorization) = &self.owner_authorization {
            visitor.visit_field(Key::new("OwnerAuthorization"), authorization);
        }
    }
}

impl<T: CertificateValue> GenericCertificate<T> {
    /// Creates a new certificate from a value, round and list of signatures.
    pub fn new(
        value: T,
        round: Round,
        mut signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    ) -> Self {
        signatures.sort_by_key(|&(validator_name, _)| validator_name);

        Self {
            value,
            round,
            signatures,
            owner_authorization: None,
        }
    }

    /// Returns the retained chain owner's signature over the certified block's
    /// proposal content, if available.
    pub fn owner_authorization(&self) -> Option<&OwnerAuthorization> {
        self.owner_authorization.as_ref()
    }

    /// Sets the retained owner authorization.
    pub fn with_owner_authorization(mut self, authorization: Option<OwnerAuthorization>) -> Self {
        self.owner_authorization = authorization;
        self
    }

    /// Returns a reference to the `Hashed` value contained in this certificate.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Consumes this certificate, returning the value it contains.
    pub fn into_value(self) -> T {
        self.value
    }

    /// Returns reference to the value contained in this certificate.
    pub fn inner(&self) -> &T {
        &self.value
    }

    /// Consumes this certificate, returning the value it contains.
    pub fn into_inner(self) -> T {
        self.value
    }

    /// Returns the certified value's hash.
    pub fn hash(&self) -> CryptoHash {
        self.value.hash()
    }

    /// Returns the list of signatures on the certified value.
    pub fn signatures(&self) -> &Vec<(ValidatorPublicKey, ValidatorSignature)> {
        &self.signatures
    }

    /// Returns a mutable reference to the list of signatures on the certified value.
    #[cfg(with_testing)]
    pub fn signatures_mut(&mut self) -> &mut Vec<(ValidatorPublicKey, ValidatorSignature)> {
        &mut self.signatures
    }

    /// Adds a signature to the certificate's list of signatures
    /// It's the responsibility of the caller to not insert duplicates
    pub fn add_signature(
        &mut self,
        signature: (ValidatorPublicKey, ValidatorSignature),
    ) -> &Vec<(ValidatorPublicKey, ValidatorSignature)> {
        let index = self
            .signatures
            .binary_search_by(|(name, _)| name.cmp(&signature.0))
            .unwrap_or_else(std::convert::identity);
        self.signatures.insert(index, signature);
        &self.signatures
    }

    /// Returns whether the validator is among the signatories of this certificate.
    pub fn is_signed_by(&self, validator_name: &ValidatorPublicKey) -> bool {
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

    /// Returns the `LiteCertificate` corresponding to this certificate, without the value.
    pub fn lite_certificate(&self) -> crate::certificate::LiteCertificate<'_>
    where
        T: CertificateValue,
    {
        crate::certificate::LiteCertificate {
            value: LiteValue::new(&self.value),
            round: self.round,
            signatures: std::borrow::Cow::Borrowed(&self.signatures),
            owner_authorization: self.owner_authorization,
        }
    }
}

impl<T: CertificateValue> Clone for GenericCertificate<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            round: self.round,
            signatures: self.signatures.clone(),
            owner_authorization: self.owner_authorization,
        }
    }
}

#[cfg(with_testing)]
impl<T: CertificateValue + Eq + PartialEq> Eq for GenericCertificate<T> {}
#[cfg(with_testing)]
impl<T: CertificateValue + Eq + PartialEq> PartialEq for GenericCertificate<T> {
    fn eq(&self, other: &Self) -> bool {
        self.hash() == other.hash()
            && self.round == other.round
            && self.signatures == other.signatures
            && self.owner_authorization == other.owner_authorization
    }
}
