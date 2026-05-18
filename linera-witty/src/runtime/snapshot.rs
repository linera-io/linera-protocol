// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Serializable representation of WebAssembly numeric values, used to capture
//! mutable globals when snapshotting an instance.

use serde::{Deserialize, Serialize};
use thiserror::Error;

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

/// Errors that can be raised by the snapshot/restore path of a Wasm instance.
///
/// These replace the panic/expect statements that the `create_snapshot` and
/// `restore_snapshot` methods used to rely on. Backend-specific runtime
/// errors are flattened to a string so the public API does not depend on
/// the underlying engine type.
#[derive(Debug, Error)]
pub enum SnapshotError {
    /// Reading the bytes of an exported linear memory failed.
    #[error("Failed to copy bytes from Wasm memory `{name}`: {message}")]
    MemoryCopy {
        /// Export name of the memory that could not be read.
        name: String,
        /// Backend-specific error message.
        message: String,
    },
    /// Growing the live linear memory to fit the snapshot failed.
    #[error("Failed to grow Wasm memory `{name}` to fit snapshot: {message}")]
    MemoryGrow {
        /// Export name of the memory that could not be grown.
        name: String,
        /// Backend-specific error message.
        message: String,
    },
    /// Writing snapshot bytes back into the live linear memory failed.
    #[error("Failed to write Wasm memory `{name}` from snapshot: {message}")]
    MemoryWrite {
        /// Export name of the memory that could not be written.
        name: String,
        /// Backend-specific error message.
        message: String,
    },
    /// Setting a mutable global from the snapshot failed.
    #[error("Failed to set Wasm global `{name}` from snapshot: {message}")]
    GlobalSet {
        /// Export name of the global that could not be set.
        name: String,
        /// Backend-specific error message.
        message: String,
    },
    /// An exported table changed size between the snapshot and the live
    /// instance. Linera contracts must not mutate tables post-instantiation.
    #[error(
        "Wasm table `{name}` size changed: snapshot has {snapshot}, fresh instance has \
         {current}; Linera contracts must not mutate tables after instantiation"
    )]
    TableSizeMismatch {
        /// Export name of the table whose size changed.
        name: String,
        /// Element count recorded in the snapshot.
        snapshot: u64,
        /// Element count of the live instance.
        current: u64,
    },
    /// The instance exports a shared memory; Linera does not use the Wasm
    /// threading proposal, and shared memories cannot be snapshotted.
    #[error("Wasm shared memory `{name}` is not supported by snapshotting")]
    SharedMemoryUnsupported {
        /// Export name of the shared memory.
        name: String,
    },
    /// A mutable global holds a non-numeric (reference) type, which cannot
    /// be carried across the snapshot boundary.
    #[error("Wasm reference-typed mutable global `{name}` cannot be snapshotted")]
    ReferenceTypedGlobal {
        /// Export name of the offending global.
        name: String,
    },
}
