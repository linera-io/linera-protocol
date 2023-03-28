// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Extension traits with some common functionality.

use serde::de::DeserializeOwned;

/// Extension trait to deserialize a type from a vector of bytes using [`bcs`].
pub trait FromBcsBytes: Sized {
    /// Deserializes itself from a vector of bytes using [`bcs`].
    fn from_bcs_bytes(bytes: &[u8]) -> Result<Self, bcs::Error>;
}

impl<T> FromBcsBytes for T
where
    T: DeserializeOwned,
{
    fn from_bcs_bytes(bytes: &[u8]) -> Result<Self, bcs::Error> {
        bcs::from_bytes(bytes)
    }
}
