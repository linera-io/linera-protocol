// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

use custom_debug_derive::Debug;
use linera_base::crypto::{BcsHashable, CryptoHash};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::CertificateValueT;
use crate::data_types::LiteValue;

/// Wrapper type around hashed instance of `T` type.
#[derive(Debug)]
pub struct Hashed<T> {
    value: T,
    /// Hash of the value (used as key for storage).
    hash: CryptoHash,
}

impl<T> Hashed<T> {
    /// Creates an instance of [`Hashed`] with the given `hash` value.
    ///
    /// Note on usage: This method is unsafe because it allows the caller to create a Hashed
    /// with a hash that doesn't match the value. This is necessary for the rewrite state when
    /// signers sign over old `Certificate` type.
    pub fn unchecked_new(value: T, hash: CryptoHash) -> Self {
        Self { value, hash }
    }

    /// Creates an instance of [`Hashed`] with the given `value`.
    ///
    /// Note: Contrary to its `unchecked_new` counterpart, this method is safe because it
    /// calculates the hash from the value.
    pub fn new(value: T) -> Self
    where
        T: BcsHashable,
    {
        let hash = CryptoHash::new(&value);
        Self { value, hash }
    }

    pub fn hash(&self) -> CryptoHash {
        self.hash
    }

    pub fn inner(&self) -> &T {
        &self.value
    }

    pub fn into_inner(self) -> T {
        self.value
    }

    pub fn lite(&self) -> LiteValue
    where
        T: CertificateValueT,
    {
        LiteValue {
            value_hash: self.hash,
            chain_id: self.value.chain_id(),
            kind: T::kind(&self.value),
        }
    }
}

impl<T: Serialize> Serialize for Hashed<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.value.serialize(serializer)
    }
}

impl<'de, T: DeserializeOwned + BcsHashable> Deserialize<'de> for Hashed<T> {
    fn deserialize<D>(deserializer: D) -> Result<Hashed<T>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Hashed::new(T::deserialize(deserializer)?))
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

impl<T: async_graphql::OutputType> async_graphql::TypeName for Hashed<T> {
    fn type_name() -> Cow<'static, str> {
        format!("Hashed{}", T::type_name(),).into()
    }
}

#[cfg(with_testing)]
impl<T> PartialEq for Hashed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.hash() == other.hash()
    }
}

#[cfg(with_testing)]
impl<T> Eq for Hashed<T> {}
