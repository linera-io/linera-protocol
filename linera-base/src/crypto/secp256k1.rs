// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Defines secp256k1 signature primitives used by the Linera protocol.

use std::{fmt, str::FromStr};

use secp256k1::{self, Message};
use serde::{Deserialize, Serialize};

use super::{BcsSignable, CryptoError, CryptoHash, HasTypeName};
use crate::doc_scalar;

/// A secp256k1 secret key.
pub struct Secp256k1SecretKey(pub secp256k1::SecretKey);

/// A secp256k1 public key.
#[derive(Eq, PartialEq, Copy, Clone)]
pub struct Secp256k1PublicKey(pub secp256k1::PublicKey);

/// A secp256k1 signature.
#[derive(Eq, PartialEq, Copy, Clone)]
pub struct Secp256k1Signature(pub secp256k1::ecdsa::Signature);

impl Serialize for Secp256k1PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&hex::encode(self.0.serialize()))
        } else {
            serializer.serialize_newtype_struct("Secp256k1PublicKey", &self.0)
        }
    }
}

impl<'de> Deserialize<'de> for Secp256k1PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let value = hex::decode(s).map_err(serde::de::Error::custom)?;
            let pk = secp256k1::PublicKey::from_slice(&value).map_err(serde::de::Error::custom)?;
            Ok(Secp256k1PublicKey(pk))
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "Secp256k1PublicKey")]
            struct Foo(secp256k1::PublicKey);

            let value = Foo::deserialize(deserializer)?;
            Ok(Self(value.0))
        }
    }
}

impl FromStr for Secp256k1PublicKey {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let pk = secp256k1::PublicKey::from_str(s)
            .map_err(|_| CryptoError::IncorrectPublicKeySize(0))?;
        Ok(Secp256k1PublicKey(pk))
    }
}

impl From<[u8; 33]> for Secp256k1PublicKey {
    fn from(value: [u8; 33]) -> Self {
        let pk = secp256k1::PublicKey::from_slice(&value).expect("Invalid public key");
        Secp256k1PublicKey(pk)
    }
}

impl TryFrom<&[u8]> for Secp256k1PublicKey {
    type Error = CryptoError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let pk = secp256k1::PublicKey::from_slice(value)
            .map_err(|_| CryptoError::IncorrectPublicKeySize(value.len()))?;
        Ok(Secp256k1PublicKey(pk))
    }
}

impl fmt::Display for Secp256k1PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = hex::encode(self.0.serialize());
        write!(f, "{}", s)
    }
}

impl fmt::Debug for Secp256k1PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0.serialize()[0..9]))
    }
}

impl Secp256k1Signature {
    /// Computes a secp256k1 signature for `value` using the given `secret`.
    /// It first serializes the `T` type and then creates the `CryptoHash` from the serialized bytes.
    pub fn new<'de, T>(value: &T, secret: &secp256k1::SecretKey) -> Self
    where
        T: BcsSignable<'de>,
    {
        let secp = secp256k1::Secp256k1::new();
        let message = Message::from_digest(CryptoHash::new(value).as_bytes().0);
        let signature = secp.sign_ecdsa(&message, secret);
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
            serializer.serialize_newtype_struct("Secp256k1Signature", &self.0)
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
            #[serde(rename = "Secp256k1Signature")]
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

doc_scalar!(Secp256k1Signature, "A secp256k1 signature value");

#[cfg(with_testing)]
mod secp256k1_tests {
    #[test]
    fn test_signatures() {
        use serde::{Deserialize, Serialize};

        use crate::crypto::{secp256k1::Secp256k1Signature, BcsSignable, TestString};

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
