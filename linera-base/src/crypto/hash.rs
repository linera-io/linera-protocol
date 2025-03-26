// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Defines hashing primitives used by the Linera protocol.

#[cfg(with_testing)]
use std::ops::RangeInclusive;
use std::{borrow::Cow, fmt, io, str::FromStr};

#[cfg(with_testing)]
use alloy_primitives::FixedBytes;
use alloy_primitives::{Keccak256, B256};
use linera_witty::{
    GuestPointer, HList, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};
#[cfg(with_testing)]
use proptest::{
    collection::{vec, VecStrategy},
    prelude::{Arbitrary, Strategy},
    strategy,
};
use serde::{Deserialize, Serialize};

use crate::{
    crypto::{BcsHashable, CryptoError, Hashable},
    doc_scalar,
};

/// A Keccak256 value.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy, Hash)]
#[cfg_attr(with_testing, derive(Default))]
pub struct CryptoHash(B256);

impl CryptoHash {
    /// Computes a hash.
    pub fn new<'de, T: BcsHashable<'de>>(value: &T) -> Self {
        let mut hasher = Keccak256Ext(Keccak256::new());
        value.write(&mut hasher);
        CryptoHash(hasher.0.finalize())
    }

    /// Reads the bytes of the hash value.
    pub fn as_bytes(&self) -> &B256 {
        &self.0
    }

    /// set the hash as the one from EVM by setting the last 12 bytes to zero.
    pub fn set_as_evm(&mut self) {
        for index in 20..32 {
            self.0 .0[index] = 0;
        }
    }

    /// Returns the hash of `TestString(s)`, for testing purposes.
    #[cfg(with_testing)]
    pub fn test_hash(s: impl Into<String>) -> Self {
        use crate::crypto::TestString;

        CryptoHash::new(&TestString::new(s))
    }
}

/// Temporary struct to extend `Keccak256` with `io::Write`.
struct Keccak256Ext(Keccak256);

impl io::Write for Keccak256Ext {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A vector of cryptographic hashes.
/// This is used to represent a hash of a list of hashes.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Hash, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Default))]
pub struct CryptoHashVec(pub Vec<CryptoHash>);

impl BcsHashable<'_> for CryptoHashVec {}

impl Serialize for CryptoHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            serializer.serialize_newtype_struct("CryptoHash", &self.as_bytes().0)
        }
    }
}

impl<'de> Deserialize<'de> for CryptoHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            let value = Self::from_str(&s).map_err(serde::de::Error::custom)?;
            Ok(value)
        } else {
            #[derive(Deserialize)]
            #[serde(rename = "CryptoHash")]
            struct Foo([u8; 32]);

            let value = Foo::deserialize(deserializer)?;
            Ok(Self(value.0.into()))
        }
    }
}

impl FromStr for CryptoHash {
    type Err = CryptoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = hex::decode(s)?;
        (value.as_slice()).try_into()
    }
}

impl TryFrom<&[u8]> for CryptoHash {
    type Error = CryptoError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != B256::len_bytes() {
            return Err(CryptoError::IncorrectHashSize(value.len()));
        }
        Ok(Self(B256::from_slice(value)))
    }
}

impl From<[u64; 4]> for CryptoHash {
    fn from(integers: [u64; 4]) -> Self {
        CryptoHash(crate::crypto::u64_array_to_be_bytes(integers).into())
    }
}

impl From<CryptoHash> for [u64; 4] {
    fn from(crypto_hash: CryptoHash) -> Self {
        crate::crypto::be_bytes_to_u64_array(crypto_hash.0.as_ref())
    }
}

impl fmt::Display for CryptoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prec = f.precision().unwrap_or(self.0.len() * 2);
        hex::encode(&self.0[..prec.div_ceil(2)]).fmt(f)
    }
}

impl fmt::Debug for CryptoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..8]))
    }
}

impl WitType for CryptoHash {
    const SIZE: u32 = <(u64, u64, u64, u64) as WitType>::SIZE;
    type Layout = <(u64, u64, u64, u64) as WitType>::Layout;
    type Dependencies = HList![];

    fn wit_type_name() -> Cow<'static, str> {
        "crypto-hash".into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        concat!(
            "    record crypto-hash {\n",
            "        part1: u64,\n",
            "        part2: u64,\n",
            "        part3: u64,\n",
            "        part4: u64,\n",
            "    }\n",
        )
        .into()
    }
}

impl WitLoad for CryptoHash {
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (part1, part2, part3, part4) = WitLoad::load(memory, location)?;
        Ok(CryptoHash::from([part1, part2, part3, part4]))
    }

    fn lift_from<Instance>(
        flat_layout: <Self::Layout as linera_witty::Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (part1, part2, part3, part4) = WitLoad::lift_from(flat_layout, memory)?;
        Ok(CryptoHash::from([part1, part2, part3, part4]))
    }
}

impl WitStore for CryptoHash {
    fn store<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let [part1, part2, part3, part4] = (*self).into();
        (part1, part2, part3, part4).store(memory, location)
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let [part1, part2, part3, part4] = (*self).into();
        (part1, part2, part3, part4).lower(memory)
    }
}

#[cfg(with_testing)]
impl Arbitrary for CryptoHash {
    type Parameters = ();
    type Strategy = strategy::Map<VecStrategy<RangeInclusive<u8>>, fn(Vec<u8>) -> CryptoHash>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        vec(u8::MIN..=u8::MAX, FixedBytes::<32>::len_bytes())
            .prop_map(|vector| CryptoHash(B256::from_slice(&vector[..])))
    }
}

doc_scalar!(CryptoHash, "A Keccak256 value");
