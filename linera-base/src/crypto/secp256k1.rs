// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Defines secp256k1 signature primitives used by the Linera protocol.

use std::{fmt, str::FromStr, sync::LazyLock};

use secp256k1::{self, All, Message, Secp256k1};
use serde::{Deserialize, Serialize};

use super::{BcsSignable, CryptoError, CryptoHash, HasTypeName};
use crate::doc_scalar;

/// Static secp256k1 context for reuse.
pub static SECP256K1: LazyLock<Secp256k1<All>> = LazyLock::new(secp256k1::Secp256k1::new);

/// A secp256k1 secret key.
#[derive(Eq, PartialEq)]
pub struct Secp256k1SecretKey(pub secp256k1::SecretKey);

/// A secp256k1 public key.
#[derive(Eq, PartialEq, Copy, Clone)]
pub struct Secp256k1PublicKey(pub secp256k1::PublicKey);

/// Secp256k1 public/secret key pair.
#[derive(Debug, PartialEq, Eq)]
pub struct Secp256k1KeyPair {
    /// Secret key.
    pub secret_key: Secp256k1SecretKey,
    /// Public key.
    pub public_key: Secp256k1PublicKey,
}

/// A secp256k1 signature.
#[derive(Eq, PartialEq, Copy, Clone)]
pub struct Secp256k1Signature(pub secp256k1::ecdsa::Signature);

impl fmt::Debug for Secp256k1SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<redacted for Secp256k1 secret key>")
    }
}

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
        let pk = secp256k1::PublicKey::from_str(s).map_err(CryptoError::Secp256k1Error)?;
        Ok(Secp256k1PublicKey(pk))
    }
}

impl TryFrom<&[u8]> for Secp256k1PublicKey {
    type Error = CryptoError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let pk = secp256k1::PublicKey::from_slice(value).map_err(CryptoError::Secp256k1Error)?;
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

impl Secp256k1KeyPair {
    /// Generates a new key pair.
    #[cfg(all(with_getrandom, with_testing))]
    pub fn generate() -> Self {
        let mut rng = rand::rngs::OsRng;
        Self::generate_from(&mut rng)
    }

    /// Generates a new key pair from the given RNG. Use with care.
    #[cfg(with_getrandom)]
    pub fn generate_from<R: super::CryptoRng>(rng: &mut R) -> Self {
        let (sk, pk) = SECP256K1.generate_keypair(rng);
        Secp256k1KeyPair {
            secret_key: Secp256k1SecretKey(sk),
            public_key: Secp256k1PublicKey(pk),
        }
    }
}

impl Secp256k1SecretKey {
    /// Returns a public key for the given secret key.
    pub fn to_public(&self) -> Secp256k1PublicKey {
        Secp256k1PublicKey(self.0.public_key(&SECP256K1))
    }
}

impl Secp256k1Signature {
    /// Computes a secp256k1 signature for `value` using the given `secret`.
    /// It first serializes the `T` type and then creates the `CryptoHash` from the serialized bytes.
    pub fn new<'de, T>(value: &T, secret: &Secp256k1SecretKey) -> Self
    where
        T: BcsSignable<'de>,
    {
        let secp = secp256k1::Secp256k1::signing_only();
        let message = Message::from_digest(CryptoHash::new(value).as_bytes().0);
        let signature = secp.sign_ecdsa(&message, &secret.0);
        Secp256k1Signature(signature)
    }

    /// Checks a signature.
    pub fn check<'de, T>(&self, value: &T, author: &Secp256k1PublicKey) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de> + fmt::Debug,
    {
        let message = Message::from_digest(CryptoHash::new(value).as_bytes().0);
        SECP256K1
            .verify_ecdsa(&message, &self.0, &author.0)
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

        use crate::crypto::{
            secp256k1::{Secp256k1KeyPair, Secp256k1Signature},
            BcsSignable, TestString,
        };

        #[derive(Debug, Serialize, Deserialize)]
        struct Foo(String);

        impl<'de> BcsSignable<'de> for Foo {}

        let keypair1 = Secp256k1KeyPair::generate();
        let keypair2 = Secp256k1KeyPair::generate();

        let ts = TestString("hello".into());
        let tsx = TestString("hellox".into());
        let foo = Foo("hello".into());

        let s = Secp256k1Signature::new(&ts, &keypair1.secret_key);
        assert!(s.check(&ts, &keypair1.public_key).is_ok());
        assert!(s.check(&ts, &keypair2.public_key).is_err());
        assert!(s.check(&tsx, &keypair1.public_key).is_err());
        assert!(s.check(&foo, &keypair1.public_key).is_err());
    }
}
