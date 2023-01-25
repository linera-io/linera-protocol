// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::de::DeserializeOwned;

pub trait FromBcsBytes: Sized {
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
