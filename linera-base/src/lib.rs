// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides a common set of types and library functions that are shared
//! between the Linera protocol (compiled from Rust to native code) and Linera
//! applications (compiled from Rust to Wasm).

pub mod crypto;
pub mod data_types;
mod graphql;
pub mod identifiers;

#[doc(hidden)]
pub use {async_graphql, bcs, hex};

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

/// Formats a byte sequence as a hexadecimal string, and elides bytes in the middle if it is longer
/// than 32 bytes.
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
///
/// let long_msg = Message {
///     bytes: b"        10        20        30        40        50".to_vec(),
/// };
///
/// assert_eq!(
///     format!("{:?}", long_msg),
///     "Message { bytes: 20202020202020203130202020202020..20202020343020202020202020203530 }"
/// );
/// ```
pub fn hex_debug<T: AsRef<[u8]>>(bytes: &T, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    const ELIDE_AFTER: usize = 16;
    let bytes = bytes.as_ref();
    if bytes.len() <= 2 * ELIDE_AFTER {
        write!(f, "{}", hex::encode(bytes))?;
    } else {
        write!(
            f,
            "{}..{}",
            hex::encode(&bytes[..ELIDE_AFTER]),
            hex::encode(&bytes[(bytes.len() - ELIDE_AFTER)..])
        )?;
    }
    Ok(())
}
