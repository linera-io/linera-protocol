// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Defines secp256k1 signature primitives used by the Linera protocol.

use std::{
    borrow::Cow,
    fmt,
    hash::{Hash, Hasher},
    str::FromStr,
};

use k256::{
    ecdsa::{Signature, SigningKey, VerifyingKey},
    elliptic_curve::sec1::FromEncodedPoint,
    EncodedPoint,
};
use linera_witty::{
    GuestPointer, HList, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};
use serde::{Deserialize, Serialize};

use super::{BcsHashable, BcsSignable, CryptoError, CryptoHash, HasTypeName};
use crate::{doc_scalar, identifiers::Owner};

/// Name of the secp256k1 scheme.
const SECP256K1_SCHEME_LABEL: &str = "secp256k1";

/// Length of secp256k1 compressed public key.
const SECP256K1_PUBLIC_KEY_SIZE: usize = 33;

/// Length of secp256k1 signature.
const SECP256K1_SIGNATURE_SIZE: usize = 64;

/// A secp256k1 secret key.
#[derive(Eq, PartialEq)]
pub struct Secp256k1SecretKey(pub SigningKey);

/// A secp256k1 public key.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub struct Secp256k1PublicKey(pub VerifyingKey);

impl Hash for Secp256k1PublicKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_encoded_point(true).as_bytes().hash(state);
    }
}

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
pub struct Secp256k1Signature(pub Signature);

impl Secp256k1PublicKey {
    /// A fake public key used for testing.
    #[cfg(with_testing)]
    pub fn test_key(seed: u8) -> Self {
        use rand::SeedableRng;
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed as u64);
        let sk = k256::SecretKey::random(&mut rng);
        Self(sk.public_key().into())
    }

    /// Returns the bytes of the public key in compressed representation.
    pub fn as_bytes(&self) -> [u8; SECP256K1_PUBLIC_KEY_SIZE] {
        // UNWRAP: We already have valid key so conversion should not fail.
        self.0.to_encoded_point(true).as_bytes().try_into().unwrap()
    }

    /// Decodes the bytes into the public key.
    /// Expects the bytes to be of compressed representation.
    ///
    /// Panics if the encoding can't be done in a constant time.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CryptoError> {
        let encoded_point =
            EncodedPoint::from_bytes(bytes).map_err(|_| CryptoError::IncorrectPublicKeySize {
                scheme: SECP256K1_SCHEME_LABEL,
                len: bytes.len(),
                expected: SECP256K1_PUBLIC_KEY_SIZE,
            })?;

        match k256::PublicKey::from_encoded_point(&encoded_point).into_option() {
            Some(public_key) => Ok(Self(public_key.into())),
            None => {
                let error = CryptoError::Secp256k1PointAtInfinity(hex::encode(bytes));
                Err(error)
            }
        }
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
        serializer.serialize_str(&hex::encode(self.0.to_bytes()))
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
        let sk = k256::ecdsa::SigningKey::from_slice(&bytes).map_err(serde::de::Error::custom)?;
        Ok(Secp256k1SecretKey(sk))
    }
}

impl Serialize for Secp256k1PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&hex::encode(self.as_bytes()))
        } else {
            let compact_pk = serde_utils::CompressedPublicKey(self.as_bytes());
            serializer.serialize_newtype_struct("Secp256k1PublicKey", &compact_pk)
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
            Ok(Secp256k1PublicKey::from_bytes(&value).map_err(serde::de::Error::custom)?)
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "Secp256k1PublicKey")]
            struct PublicKey(serde_utils::CompressedPublicKey);
            let compact = PublicKey::deserialize(deserializer)?;
            Ok(Secp256k1PublicKey::from_bytes(&compact.0 .0).map_err(serde::de::Error::custom)?)
        }
    }
}

impl FromStr for Secp256k1PublicKey {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        hex::decode(s)?.as_slice().try_into()
    }
}

impl TryFrom<&[u8]> for Secp256k1PublicKey {
    type Error = CryptoError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::from_bytes(value)
    }
}

impl fmt::Display for Secp256k1PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = hex::encode(self.as_bytes());
        write!(f, "{}", str)
    }
}

impl fmt::Debug for Secp256k1PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..", hex::encode(&self.as_bytes()[0..9]))
    }
}

impl From<Secp256k1PublicKey> for Owner {
    fn from(value: Secp256k1PublicKey) -> Self {
        Self(CryptoHash::new(&value))
    }
}

impl From<&Secp256k1PublicKey> for Owner {
    fn from(value: &Secp256k1PublicKey) -> Self {
        Self(CryptoHash::new(value))
    }
}

impl<'de> BcsHashable<'de> for Secp256k1PublicKey {}

impl WitType for Secp256k1PublicKey {
    const SIZE: u32 = <(u64, u64, u64, u64, u8) as WitType>::SIZE;
    type Layout = <(u64, u64, u64, u64, u8) as WitType>::Layout;
    type Dependencies = HList![];

    fn wit_type_name() -> Cow<'static, str> {
        "secp256k1-public-key".into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        concat!(
            "    record secp256k1-public-key {\n",
            "        part1: u64,\n",
            "        part2: u64,\n",
            "        part3: u64,\n",
            "        part4: u64,\n",
            "        part5: u8\n",
            "    }\n",
        )
        .into()
    }
}

impl WitLoad for Secp256k1PublicKey {
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (part1, part2, part3, part4, part5) = WitLoad::load(memory, location)?;
        Ok(Self::from((part1, part2, part3, part4, part5)))
    }

    fn lift_from<Instance>(
        flat_layout: <Self::Layout as Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (part1, part2, part3, part4, part5) = WitLoad::lift_from(flat_layout, memory)?;
        Ok(Self::from((part1, part2, part3, part4, part5)))
    }
}

impl WitStore for Secp256k1PublicKey {
    fn store<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (part1, part2, part3, part4, part5) = (*self).into();
        (part1, part2, part3, part4, part5).store(memory, location)
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (part1, part2, part3, part4, part5) = (*self).into();
        (part1, part2, part3, part4, part5).lower(memory)
    }
}

impl From<(u64, u64, u64, u64, u8)> for Secp256k1PublicKey {
    fn from((part1, part2, part3, part4, part5): (u64, u64, u64, u64, u8)) -> Self {
        let mut bytes = [0u8; SECP256K1_PUBLIC_KEY_SIZE];
        bytes[0..8].copy_from_slice(&part1.to_be_bytes());
        bytes[8..16].copy_from_slice(&part2.to_be_bytes());
        bytes[16..24].copy_from_slice(&part3.to_be_bytes());
        bytes[24..32].copy_from_slice(&part4.to_be_bytes());
        bytes[32] = part5;
        Self::from_bytes(&bytes).unwrap()
    }
}

impl From<Secp256k1PublicKey> for (u64, u64, u64, u64, u8) {
    fn from(key: Secp256k1PublicKey) -> Self {
        let bytes = key.as_bytes();
        let part1 = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let part2 = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let part3 = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
        let part4 = u64::from_be_bytes(bytes[24..32].try_into().unwrap());
        let part5 = bytes[32];
        (part1, part2, part3, part4, part5)
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
        let secret_key = Secp256k1SecretKey(SigningKey::random(rng));
        let public_key = secret_key.public();
        Secp256k1KeyPair {
            secret_key,
            public_key,
        }
    }
}

impl Secp256k1SecretKey {
    /// Returns a public key for the given secret key.
    pub fn public(&self) -> Secp256k1PublicKey {
        Secp256k1PublicKey(*self.0.verifying_key())
    }

    /// Copies the key pair, **including the secret key**.
    ///
    /// The `Clone` and `Copy` traits are deliberately not implemented for `Secp256k1SecretKey` to prevent
    /// accidental copies of secret keys.
    pub fn copy(&self) -> Self {
        Self(self.0.clone())
    }

    /// Generates a new key pair.
    #[cfg(all(with_getrandom, with_testing))]
    pub fn generate() -> Self {
        let mut rng = rand::rngs::OsRng;
        Self::generate_from(&mut rng)
    }

    /// Generates a new key pair from the given RNG. Use with care.
    #[cfg(with_getrandom)]
    pub fn generate_from<R: super::CryptoRng>(rng: &mut R) -> Self {
        Secp256k1SecretKey(SigningKey::random(rng))
    }
}

impl Secp256k1Signature {
    /// Computes a secp256k1 signature for `value` using the given `secret`.
    /// It first serializes the `T` type and then creates the `CryptoHash` from the serialized bytes.
    pub fn new<'de, T>(value: &T, secret: &Secp256k1SecretKey) -> Self
    where
        T: BcsSignable<'de>,
    {
        use k256::ecdsa::signature::hazmat::PrehashSigner;

        let prehash = CryptoHash::new(value).as_bytes().0;
        let (signature, _rid) = secret
            .0
            .sign_prehash(&prehash)
            .expect("Failed to sign prehashed data"); // NOTE: This is a critical error we don't control.
        Secp256k1Signature(signature)
    }

    /// Checks a signature.
    pub fn check<'de, T>(&self, value: &T, author: &Secp256k1PublicKey) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de> + fmt::Debug,
    {
        let prehash = CryptoHash::new(value).as_bytes().0;
        self.verify_inner::<T>(prehash, author)
    }

    /// Verifies a batch of signatures.
    ///
    /// Returns an error on first failed signature.
    pub fn verify_batch<'a, 'de, T, I>(value: &'a T, votes: I) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de> + fmt::Debug,
        I: IntoIterator<Item = &'a (Secp256k1PublicKey, Secp256k1Signature)>,
    {
        let prehash = CryptoHash::new(value).as_bytes().0;
        for (author, signature) in votes {
            signature.verify_inner::<T>(prehash, author)?;
        }
        Ok(())
    }

    /// Returns the byte representation of the signature.
    pub fn as_bytes(&self) -> [u8; SECP256K1_SIGNATURE_SIZE] {
        self.0.to_bytes().into()
    }

    fn verify_inner<'de, T>(
        &self,
        prehash: [u8; 32],
        author: &Secp256k1PublicKey,
    ) -> Result<(), CryptoError>
    where
        T: BcsSignable<'de> + fmt::Debug,
    {
        use k256::ecdsa::signature::hazmat::PrehashVerifier;

        author
            .0
            .verify_prehash(&prehash, &self.0)
            .map_err(|error| CryptoError::InvalidSignature {
                error: error.to_string(),
                type_name: T::type_name().to_string(),
            })
    }

    /// Creates a signature from the bytes.
    /// Expects the signature to be serialized in raw-bytes form.
    pub fn from_slice<A: AsRef<[u8]>>(bytes: A) -> Result<Self, CryptoError> {
        let sig = k256::ecdsa::Signature::from_slice(bytes.as_ref())
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
            serializer.serialize_str(&hex::encode(self.as_bytes()))
        } else {
            let compact = serde_utils::CompactSignature(self.as_bytes());
            serializer.serialize_newtype_struct("Secp256k1Signature", &compact)
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
            Self::from_slice(&value).map_err(serde::de::Error::custom)
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "Secp256k1Signature")]
            struct Signature(serde_utils::CompactSignature);

            let value = Signature::deserialize(deserializer)?;
            Self::from_slice(value.0 .0.as_ref()).map_err(serde::de::Error::custom)
        }
    }
}

impl fmt::Display for Secp256k1Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = hex::encode(self.as_bytes());
        write!(f, "{}", s)
    }
}

impl fmt::Debug for Secp256k1Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..", hex::encode(&self.as_bytes()[0..9]))
    }
}

doc_scalar!(Secp256k1Signature, "A secp256k1 signature value");
doc_scalar!(Secp256k1PublicKey, "A secp256k1 public key value");

mod serde_utils {
    use serde::{Deserialize, Serialize};
    use serde_with::serde_as;

    use super::{SECP256K1_PUBLIC_KEY_SIZE, SECP256K1_SIGNATURE_SIZE};

    /// Wrapper around compact signature serialization
    /// so that we can implement custom serializer for it that uses fixed length.
    // Serde treats arrays larger than 32 as variable length arrays, and adds the length as a prefix.
    // Since we want a fixed size representation, we wrap it in this helper struct and use serde_as.
    #[serde_as]
    #[derive(Serialize, Deserialize)]
    #[serde(transparent)]
    pub struct CompactSignature(#[serde_as(as = "[_; 64]")] pub [u8; SECP256K1_SIGNATURE_SIZE]);

    #[serde_as]
    #[derive(Serialize, Deserialize)]
    #[serde(transparent)]
    pub struct CompressedPublicKey(#[serde_as(as = "[_; 33]")] pub [u8; SECP256K1_PUBLIC_KEY_SIZE]);
}

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
    fn test_public_key_serialization() {
        use crate::crypto::secp256k1::Secp256k1PublicKey;
        let key_in = Secp256k1PublicKey::test_key(0);
        let s = serde_json::to_string(&key_in).unwrap();
        let key_out: Secp256k1PublicKey = serde_json::from_str(&s).unwrap();
        assert_eq!(key_out, key_in);

        let s = bcs::to_bytes(&key_in).unwrap();
        let key_out: Secp256k1PublicKey = bcs::from_bytes(&s).unwrap();
        assert_eq!(key_out, key_in);
    }

    #[test]
    fn test_secret_key_serialization() {
        use crate::crypto::secp256k1::{Secp256k1KeyPair, Secp256k1SecretKey};
        let key_in = Secp256k1KeyPair::generate().secret_key;
        let s = serde_json::to_string(&key_in).unwrap();
        let key_out: Secp256k1SecretKey = serde_json::from_str(&s).unwrap();
        assert_eq!(key_out, key_in);
    }

    #[test]
    fn test_signature_serialization() {
        use crate::crypto::{
            secp256k1::{Secp256k1KeyPair, Secp256k1Signature},
            TestString,
        };
        let keypair = Secp256k1KeyPair::generate();
        let sig = Secp256k1Signature::new(&TestString("hello".into()), &keypair.secret_key);
        let s = serde_json::to_string(&sig).unwrap();
        let sig2: Secp256k1Signature = serde_json::from_str(&s).unwrap();
        assert_eq!(sig, sig2);

        let s = bcs::to_bytes(&sig).unwrap();
        let sig2: Secp256k1Signature = bcs::from_bytes(&s).unwrap();
        assert_eq!(sig, sig2);
    }

    #[test]
    fn public_key_from_str() {
        use std::str::FromStr;

        use crate::crypto::secp256k1::Secp256k1PublicKey;
        let key = Secp256k1PublicKey::test_key(0);
        let s = key.to_string();
        let key2 = Secp256k1PublicKey::from_str(s.as_str()).unwrap();
        assert_eq!(key, key2);
    }

    #[test]
    fn bytes_repr_compact_public_key() {
        use crate::crypto::secp256k1::{Secp256k1PublicKey, SECP256K1_PUBLIC_KEY_SIZE};
        let key_in: Secp256k1PublicKey = Secp256k1PublicKey::test_key(0);
        let bytes = key_in.as_bytes();
        assert!(
            bytes.len() == SECP256K1_PUBLIC_KEY_SIZE,
            "::to_bytes() should return compressed representation"
        );
        let key_out = Secp256k1PublicKey::from_bytes(&bytes).unwrap();
        assert_eq!(key_in, key_out);
    }

    #[test]
    fn human_readable_ser() {
        use crate::crypto::{
            secp256k1::{Secp256k1KeyPair, Secp256k1Signature},
            TestString,
        };
        let key_pair = Secp256k1KeyPair::generate();
        let sig = Secp256k1Signature::new(&TestString("hello".into()), &key_pair.secret_key);
        let s = serde_json::to_string(&sig).unwrap();
        let sig2: Secp256k1Signature = serde_json::from_str(&s).unwrap();
        assert_eq!(sig, sig2);
    }
}
