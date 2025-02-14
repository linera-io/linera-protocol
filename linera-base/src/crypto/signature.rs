// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use ed25519_dalek::{self as dalek, Signer, Verifier};
use secp256k1::{self, Message};
use serde::{Deserialize, Serialize};

use super::{BcsSignable, CryptoError, CryptoHash, HasTypeName, Hashable, KeyPair, PublicKey};
use crate::doc_scalar;

/// A Secp256k1 signature.
#[derive(Eq, PartialEq, Copy, Clone)]
pub struct Secp256k1Signature(pub secp256k1::ecdsa::Signature);

impl Secp256k1Signature {
    /// Computes a secp256k1 signature for [`value`] using the given [`secret`].
    /// It first serializes the `T` type and then creates the `CryptoHash` from the serialized bytes.
    pub fn new<'de, T>(value: &T, secret: &secp256k1::SecretKey) -> Self
    where
        T: BcsSignable<'de>,
    {
        let secp = secp256k1::Secp256k1::new();
        let message = Message::from_digest(CryptoHash::new(value).as_bytes().0);
        let signature = secp.sign_ecdsa(&message, &secret);
        Secp256k1Signature(signature)
    }

    /// Checks a signature.
    pub fn check<'de, T>(&self, value: &T, author: &secp256k1::PublicKey) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de> + fmt::Debug,
    {
        let secp = secp256k1::Secp256k1::new();
        let message = Message::from_digest(CryptoHash::new(value).as_bytes().0);
        secp.verify_ecdsa(&message, &self.0, author)
            .map_err(|error| CryptoError::InvalidSignature {
                error: error.to_string(),
                type_name: T::type_name().to_string(),
            })
    }
}

impl Serialize for Secp256k1Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&hex::encode(self.0.serialize_der()))
        } else {
            serializer.serialize_newtype_struct("Signature", &self.0)
        }
    }
}

impl<'de> Deserialize<'de> for Secp256k1Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let value = hex::decode(s).map_err(serde::de::Error::custom)?;
            let sig =
                secp256k1::ecdsa::Signature::from_der(&value).map_err(serde::de::Error::custom)?;
            Ok(Secp256k1Signature(sig))
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "Signature")]
            struct Foo(secp256k1::ecdsa::Signature);

            let value = Foo::deserialize(deserializer)?;
            Ok(Self(value.0))
        }
    }
}

impl fmt::Display for Secp256k1Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = hex::encode(self.0.serialize_der());
        write!(f, "{}", s)
    }
}

impl fmt::Debug for Secp256k1Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0.serialize_der()[0..8]))
    }
}

doc_scalar!(Secp256k1Signature, "A Secp256k1 signature value");

/// A Ed25519 signature.
#[derive(Eq, PartialEq, Copy, Clone)]
pub struct Ed25519Signature(pub dalek::Signature);

impl Ed25519Signature {
    /// Computes a signature.
    pub fn new<'de, T>(value: &T, secret: &KeyPair) -> Self
    where
        T: BcsSignable<'de>,
    {
        let mut message = Vec::new();
        value.write(&mut message);
        let signature = secret.0.sign(&message);
        Ed25519Signature(signature)
    }

    fn check_internal<'de, T>(
        &self,
        value: &T,
        author: PublicKey,
    ) -> Result<(), dalek::SignatureError>
    where
        T: BcsSignable<'de>,
    {
        let mut message = Vec::new();
        value.write(&mut message);
        let public_key = dalek::VerifyingKey::from_bytes(&author.0)?;
        public_key.verify(&message, &self.0)
    }

    /// Checks a signature.
    pub fn check<'de, T>(&self, value: &T, author: PublicKey) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de> + fmt::Debug,
    {
        self.check_internal(value, author)
            .map_err(|error| CryptoError::InvalidSignature {
                error: error.to_string(),
                type_name: T::type_name().to_string(),
            })
    }

    /// Checks an optional signature.
    pub fn check_optional_signature<'de, T>(
        signature: Option<&Self>,
        value: &T,
        author: &PublicKey,
    ) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de> + fmt::Debug,
    {
        match signature {
            Some(sig) => sig.check(value, *author),
            None => Err(CryptoError::MissingSignature {
                type_name: T::type_name().to_string(),
            }),
        }
    }

    fn verify_batch_internal<'a, 'de, T, I>(
        value: &'a T,
        votes: I,
    ) -> Result<(), dalek::SignatureError>
    where
        T: BcsSignable<'de>,
        I: IntoIterator<Item = (&'a PublicKey, &'a Ed25519Signature)>,
    {
        let mut msg = Vec::new();
        value.write(&mut msg);
        let mut messages = Vec::new();
        let mut signatures = Vec::new();
        let mut public_keys = Vec::new();
        for (addr, sig) in votes.into_iter() {
            messages.push(msg.as_slice());
            signatures.push(sig.0);
            public_keys.push(dalek::VerifyingKey::from_bytes(&addr.0)?);
        }
        dalek::verify_batch(&messages[..], &signatures[..], &public_keys[..])
    }

    /// Verifies a batch of signatures.
    pub fn verify_batch<'a, 'de, T, I>(value: &'a T, votes: I) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de>,
        I: IntoIterator<Item = (&'a PublicKey, &'a Ed25519Signature)>,
    {
        Ed25519Signature::verify_batch_internal(value, votes).map_err(|error| {
            CryptoError::InvalidSignature {
                error: format!("batched {}", error),
                type_name: T::type_name().to_string(),
            }
        })
    }
}

impl Serialize for Ed25519Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&hex::encode(self.0.to_bytes()))
        } else {
            serializer.serialize_newtype_struct("Signature", &self.0)
        }
    }
}

impl<'de> Deserialize<'de> for Ed25519Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let value = hex::decode(s).map_err(serde::de::Error::custom)?;
            let sig =
                dalek::Signature::try_from(value.as_slice()).map_err(serde::de::Error::custom)?;
            Ok(Ed25519Signature(sig))
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "Signature")]
            struct Foo(dalek::Signature);

            let value = Foo::deserialize(deserializer)?;
            Ok(Self(value.0))
        }
    }
}

impl fmt::Display for Ed25519Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = hex::encode(self.0.to_bytes());
        write!(f, "{}", s)
    }
}

impl fmt::Debug for Ed25519Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0.to_bytes()[0..8]))
    }
}

doc_scalar!(Ed25519Signature, "An Ed25519 signature value");

/// A BCS-signable struct for testing.
#[cfg(with_testing)]
#[derive(Debug, Serialize, Deserialize)]
pub struct TestString(pub String);

#[cfg(with_testing)]
impl TestString {
    /// Creates a new `TestString` with the given string.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

#[cfg(with_testing)]
impl<'de> BcsSignable<'de> for TestString {}

#[cfg(with_testing)]
mod ed25519_tests {
    #[test]
    fn test_signatures() {
        use serde::{Deserialize, Serialize};

        use crate::crypto::{BcsSignable, Ed25519Signature, KeyPair, TestString};

        #[derive(Debug, Serialize, Deserialize)]
        struct Foo(String);

        impl<'de> BcsSignable<'de> for Foo {}

        let key1 = KeyPair::generate();
        let addr1 = key1.public();
        let key2 = KeyPair::generate();
        let addr2 = key2.public();

        let ts = TestString("hello".into());
        let tsx = TestString("hellox".into());
        let foo = Foo("hello".into());

        let s = Ed25519Signature::new(&ts, &key1);
        assert!(s.check(&ts, addr1).is_ok());
        assert!(s.check(&ts, addr2).is_err());
        assert!(s.check(&tsx, addr1).is_err());
        assert!(s.check(&foo, addr1).is_err());
    }
}

#[cfg(with_testing)]
mod secp256k1_tests {
    #[test]
    fn test_signatures() {
        use serde::{Deserialize, Serialize};

        use crate::crypto::{BcsSignable, Secp256k1Signature, TestString};

        #[derive(Debug, Serialize, Deserialize)]
        struct Foo(String);

        impl<'de> BcsSignable<'de> for Foo {}

        let (sk1, pk1) = secp256k1::Secp256k1::new().generate_keypair(&mut rand::thread_rng());
        let (_sk2, pk2) = secp256k1::Secp256k1::new().generate_keypair(&mut rand::thread_rng());

        let ts = TestString("hello".into());
        let tsx = TestString("hellox".into());
        let foo = Foo("hello".into());

        let s = Secp256k1Signature::new(&ts, &sk1);
        assert!(s.check(&ts, &pk1).is_ok());
        assert!(s.check(&ts, &pk2).is_err());
        assert!(s.check(&tsx, &pk1).is_err());
        assert!(s.check(&foo, &pk1).is_err());
    }
}
