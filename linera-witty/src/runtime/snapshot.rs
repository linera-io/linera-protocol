// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Serializable representation of WebAssembly numeric values, used to capture
//! mutable globals when snapshotting an instance.

use serde::{Deserialize, Serialize};

/// A WebAssembly numeric value suitable for inclusion in a serialized snapshot.
///
/// Floats are stored as their IEEE 754 bit patterns so that the encoding is
/// canonical (and so that backends like `bcs`, which intentionally don't support
/// floats, can still serialize the value).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NumericVal {
    /// A 32-bit integer.
    I32(i32),
    /// A 64-bit integer.
    I64(i64),
    /// A 32-bit float, encoded as its IEEE 754 bit pattern.
    F32(u32),
    /// A 64-bit float, encoded as its IEEE 754 bit pattern.
    F64(u64),
    /// A 128-bit SIMD vector.
    V128(u128),
}
