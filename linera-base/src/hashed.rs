// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A wrapper for hashable types to memoize the hash.

use std::borrow::Cow;

use custom_debug_derive::Debug;
use serde::{Deserialize, Serialize};

use crate::crypto::{BcsHashable, CryptoHash};

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
    pub fn new<'de>(value: T) -> Self
    where
        T: BcsHashable<'de>,
    {
        let hash = CryptoHash::new(&value);
        Self { value, hash }
    }

    /// Returns the hash.
    pub fn hash(&self) -> CryptoHash {
        self.hash
    }

    /// Returns a reference to the value, without the hash.
    pub fn inner(&self) -> &T {
        &self.value
    }

    /// Consumes the hashed value and returns the value without the hash.
    pub fn into_inner(self) -> T {
        self.value
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

impl<'de, T: BcsHashable<'de>> Deserialize<'de> for Hashed<T> {
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
        format!("Hashed{}", T::type_name()).into()
    }
}

#[async_graphql::Object(cache_control(no_cache), name_type)]
impl<T: async_graphql::OutputType + Clone> Hashed<T> {
    #[graphql(derived(name = "hash"))]
    async fn _hash(&self) -> CryptoHash {
        self.hash()
    }

    #[graphql(derived(name = "value"))]
    async fn _value(&self) -> T {
        self.inner().clone()
    }
}

impl<T> PartialEq for Hashed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.hash() == other.hash()
    }
}

impl<T> Eq for Hashed<T> {}
