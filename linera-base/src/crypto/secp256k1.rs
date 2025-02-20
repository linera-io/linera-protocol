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
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash)]
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

impl Secp256k1PublicKey {
    /// A fake public key used for testing.
    #[cfg(with_testing)]
    pub fn test_key(seed: u8) -> Self {
        use rand::SeedableRng;

        let mut rng = rand::rngs::StdRng::seed_from_u64(seed as u64);
        let secp = secp256k1::Secp256k1::signing_only();
        Self(secp.generate_keypair(&mut rng).1)
    }
}

impl fmt::Debug for Secp256k1SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<redacted for Secp256k1 secret key>")
    }
}

impl Serialize for Secp256k1SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        // This is only used for JSON configuration.
        assert!(serializer.is_human_readable());
        serializer.serialize_str(&hex::encode(self.0.secret_bytes()))
    }
}

impl<'de> Deserialize<'de> for Secp256k1SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        // This is only used for JSON configuration.
        assert!(deserializer.is_human_readable());
        let str = String::deserialize(deserializer)?;
        let bytes = hex::decode(&str).map_err(serde::de::Error::custom)?;
        let sk = secp256k1::SecretKey::from_slice(&bytes).map_err(serde::de::Error::custom)?;
        Ok(Secp256k1SecretKey(sk))
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
            #[derive(Serialize)]
            #[serde(rename = "Secp256k1PublicKey")]
            struct Foo<'a>(&'a secp256k1::PublicKey);
            Foo(&self.0).serialize(serializer)
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
    pub fn public(&self) -> Secp256k1PublicKey {
        Secp256k1PublicKey(self.0.public_key(&SECP256K1))
    }

    /// Copies the key pair, **including the secret key**.
    ///
    /// The `Clone` and `Copy` traits are deliberately not implemented for `Secp256k1SecretKey` to prevent
    /// accidental copies of secret keys.
    pub fn copy(&self) -> Self {
        Self(self.0)
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

    pub fn verify_batch<'a, 'de, T, I>(value: &'a T, votes: I) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de> + fmt::Debug,
        I: IntoIterator<Item = &'a (Secp256k1PublicKey, Secp256k1Signature)>,
    {
        let message = Message::from_digest(CryptoHash::new(value).as_bytes().0);
        for (author, signature) in votes {
            SECP256K1
                .verify_ecdsa(&message, &signature.0, &author.0)
                .expect("Invalid signature");
        }
        Ok(())
    }

    /// Converts the signature to a byte array.
    /// Expects the signature to be serialized in compact form.
    pub fn from_slice<A: AsRef<[u8]>>(bytes: A) -> Result<Self, CryptoError> {
        let sig = secp256k1::ecdsa::Signature::from_compact(bytes.as_ref())
            .map_err(CryptoError::Secp256k1Error)?;
        Ok(Secp256k1Signature(sig))
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
doc_scalar!(Secp256k1PublicKey, "A secp256k1 public key value");

#[cfg(with_testing)]
mod tests {
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

    #[test]
    fn test_publickey_serialization() {
        use crate::crypto::secp256k1::Secp256k1PublicKey;
        let key_in = Secp256k1PublicKey::test_key(0);
        let s = serde_json::to_string(&key_in).unwrap();
        let key_out: Secp256k1PublicKey = serde_json::from_str(&s).unwrap();
        assert_eq!(key_out, key_in);

        let s = bincode::serialize(&key_in).unwrap();
        let key_out: Secp256k1PublicKey = bincode::deserialize(&s).unwrap();
        assert_eq!(key_out, key_in);
    }

    #[test]
    fn test_secretkey_deserialization() {
        use crate::crypto::secp256k1::{Secp256k1KeyPair, Secp256k1SecretKey};
        let key_in = Secp256k1KeyPair::generate().secret_key;
        let s = serde_json::to_string(&key_in).unwrap();
        let key_out: Secp256k1SecretKey = serde_json::from_str(&s).unwrap();
        assert_eq!(key_out, key_in);
    }
}
