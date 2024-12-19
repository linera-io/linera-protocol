// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides a common set of types and library functions that are shared
//! between the Linera protocol (compiled from Rust to native code) and Linera
//! applications (compiled from Rust to Wasm).

#![deny(missing_docs)]
#![deny(clippy::large_futures)]

use std::fmt;

#[doc(hidden)]
pub use async_trait::async_trait;

pub mod abi;
#[cfg(not(target_arch = "wasm32"))]
pub mod command;
pub mod crypto;
pub mod data_types;
pub mod dyn_convert;
mod graphql;
pub mod hashed;
pub mod identifiers;
mod limited_writer;
pub mod ownership;
#[cfg(not(target_arch = "wasm32"))]
pub mod port;
#[cfg(with_metrics)]
pub mod prometheus_util;
#[cfg(not(chain))]
pub mod task;
#[cfg(not(chain))]
pub use task::Blocking;
pub mod time;
#[cfg_attr(web, path = "tracing_web.rs")]
pub mod tracing;
#[cfg(test)]
mod unit_tests;

pub use graphql::BcsHexParseError;
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
pub fn hex_debug<T: AsRef<[u8]>>(bytes: &T, f: &mut fmt::Formatter) -> fmt::Result {
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

/// Applies `hex_debug` to a slice of byte vectors.
///
///  # Examples
///
/// ```
/// # use linera_base::hex_vec_debug;
/// use custom_debug_derive::Debug;
///
/// #[derive(Debug)]
/// struct Messages {
///     #[debug(with = "hex_vec_debug")]
///     byte_vecs: Vec<Vec<u8>>,
/// }
///
/// let msgs = Messages {
///     byte_vecs: vec![vec![0x12, 0x34, 0x56, 0x78], vec![0x9A]],
/// };
///
/// assert_eq!(
///     format!("{:?}", msgs),
///     "Messages { byte_vecs: [12345678, 9a] }"
/// );
/// ```
#[allow(clippy::ptr_arg)] // This only works with custom_debug_derive if it's &Vec.
pub fn hex_vec_debug(list: &Vec<Vec<u8>>, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[")?;
    for (i, bytes) in list.iter().enumerate() {
        if i != 0 {
            write!(f, ", ")?;
        }
        hex_debug(bytes, f)?;
    }
    write!(f, "]")
}
