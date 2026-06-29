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
use crate::{data_types::LiteValue, ChainError};

/// Generic type representing a certificate for `value` of type `T`.
#[derive(Debug)]
pub struct GenericCertificate<T: CertificateValue> {
    value: T,
    /// The round in which the value was certified.
    pub round: Round,
    /// The lock round `ℓ` the `ValidatedBlock` voters signed (see [`VoteValue`]). Always `None`
    /// for `ConfirmedBlock`/`Timeout` certificates and for validated blocks with no justification.
    ///
    /// [`VoteValue`]: crate::data_types::VoteValue
    lock: Option<Round>,
    signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
}

impl<T: Allocative + CertificateValue> Allocative for GenericCertificate<T> {
    fn visit<'a, 'b: 'a>(&self, visitor: &'a mut Visitor<'b>) {
        visitor.visit_field(Key::new("GenericCertificate_value"), &self.value);
        visitor.visit_field(Key::new("GenericCertificate_round"), &self.round);
        for (public_key, signature) in &self.signatures {
            visitor.visit_field(Key::new("ValidatorPublicKey"), public_key);
            visitor.visit_field(Key::new("ValidatorSignature"), signature);
        }
    }
}

impl<T: CertificateValue> GenericCertificate<T> {
    /// Creates a new certificate from a value, round and list of signatures.
    pub fn new(
        value: T,
        round: Round,
        signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    ) -> Self {
        Self::new_with_lock(value, round, None, signatures)
    }

    /// Creates a new certificate that also records the lock round `ℓ` its `ValidatedBlock`
    /// voters signed (see [`VoteValue`]).
    ///
    /// [`VoteValue`]: crate::data_types::VoteValue
    pub fn new_with_lock(
        value: T,
        round: Round,
        lock: Option<Round>,
        mut signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
    ) -> Self {
        signatures.sort_by_key(|&(validator_name, _)| validator_name);

        Self {
            value,
            round,
            lock,
            signatures,
        }
    }

    /// Returns the round in which the value was certified.
    pub fn round(&self) -> Round {
        self.round
    }

    /// Returns the lock round `ℓ` the `ValidatedBlock` voters signed, if any.
    pub fn lock(&self) -> Option<Round> {
        self.lock
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
            self.lock,
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
            lock: self.lock,
            justification: crate::justification::JustificationChain::default(),
            signatures: std::borrow::Cow::Borrowed(&self.signatures),
        }
    }
}

impl<T: CertificateValue> Clone for GenericCertificate<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            round: self.round,
            lock: self.lock,
            signatures: self.signatures.clone(),
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
            && self.lock == other.lock
            && self.signatures == other.signatures
    }
}
