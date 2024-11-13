// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Debug, Formatter};

use linera_base::crypto::{BcsHashable, CryptoHash};

/// Wrapper type around hashed instance of `T` type.
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
}

impl<T: Debug> Debug for Hashed<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashedT")
            .field("value", &self.value)
            .field("hash", &self.hash())
            .finish()
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
