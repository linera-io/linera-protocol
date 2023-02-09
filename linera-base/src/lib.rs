// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

pub mod committee;
pub mod crypto;
pub mod data_types;
pub mod ensure;
mod graphql;

/// Prints a vector of bytes in hexadecimal.
pub fn hex_debug<T: AsRef<[u8]>>(bytes: &T, f: &mut fmt::Formatter) -> fmt::Result {
    for byte in bytes.as_ref() {
        write!(f, "{:02x}", byte)?;
    }
    Ok(())
}
