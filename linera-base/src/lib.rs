// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides a common set of types and library functions that are shared
//! between the Linera protocol (compiled from Rust to native code) and Linera
//! applications (compiled from Rust to Wasm).

pub mod crypto;
pub mod data_types;
mod graphql;

/// A macro for asserting that a condition is true, returning an error if it is not.
///
/// # Examples
///
/// ```
/// # use linera_base::ensure;
/// fn divide(x: i32, y: i32) -> Result<i32, String> {
///     ensure!(y != 0, "division by zero");
///     Ok(x / y)
/// }
///
/// assert_eq!(divide(10, 2), Ok(5));
/// assert_eq!(divide(10, 0), Err(String::from("division by zero")));
/// ```
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            return Err($e.into());
        }
    };
}

/// Formats a byte sequence as a hexadecimal string.
///
/// This function is intended to be used with the `#[debug(with = "hex_debug")]` field
/// annotation of `custom_debug_derive::Debug`.
///
/// # Examples
///
/// ```
/// # use linera_base::hex_debug;
/// use custom_debug_derive::Debug;
///
/// #[derive(Debug)]
/// struct Message {
///     #[debug(with = "hex_debug")]
///     bytes: Vec<u8>,
/// }
///
/// let msg = Message {
///     bytes: vec![0x12, 0x34, 0x56, 0x78],
/// };
///
/// assert_eq!(format!("{:?}", msg), "Message { bytes: 12345678 }");
/// ```
pub fn hex_debug<T: AsRef<[u8]>>(bytes: &T, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    for byte in bytes.as_ref() {
        write!(f, "{:02x}", byte)?;
    }
    Ok(())
}
