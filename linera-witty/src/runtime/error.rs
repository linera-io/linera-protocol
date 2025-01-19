// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Common error type for usage of different Wasm runtimes.

use std::{num::TryFromIntError, string::FromUtf8Error};

use thiserror::Error;

/// Errors that can occur when using a Wasm runtime.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// Attempt to allocate a buffer larger than `i32::MAX`.
    #[error("Requested allocation size is too large")]
    AllocationTooLarge,

    /// Attempt to allocate a buffer that's aligned to an invalid boundary.
    #[error("Requested allocation alignment is invalid")]
    InvalidAlignment,

    /// Call to `cabi_realloc` returned a negative value instead of a valid address.
    #[error("Memory allocation failed")]
    AllocationFailed,

    /// Attempt to deallocate an address that's after `i32::MAX`.
    #[error("Attempt to deallocate an invalid address")]
    DeallocateInvalidAddress,

    /// Attempt to load a function not exported from a module.
    #[error("Function `{_0}` could not be found in the module's exports")]
    FunctionNotFound(String),

    /// Attempt to load a function with a name that's used for a different import in the module.
    #[error("Export `{_0}` is not a function")]
    NotAFunction(String),

    /// Attempt to load the memory export from a module that doesn't export it.
    #[error("Failed to load `memory` export")]
    MissingMemory,

    /// Attempt to load the memory export from a module that exports it as something else.
    #[error("Unexpected type for `memory` export")]
    NotMemory,

    /// Attempt to load a string from a sequence of bytes that doesn't contain a UTF-8 string.
    #[error("Failed to load string from non-UTF-8 bytes: {0}")]
    InvalidString(#[from] FromUtf8Error),

    /// Attempt to create a `GuestPointer` from an invalid address representation.
    #[error("Invalid address read: {0}")]
    InvalidNumber(#[from] TryFromIntError),

    /// Attempt to load an `enum` type but the discriminant doesn't match any of the variants.
    #[error("Unexpected variant discriminant {discriminant} for `{type_name}`")]
    InvalidVariant {
        /// The `enum` type that failed being loaded.
        type_name: &'static str,
        /// The invalid discriminant that was received.
        discriminant: i64,
    },

    /// A custom error reported by one of the Wasm host's function handlers.
    #[error("Error reported by host function handler: {_0}")]
    Custom(#[source] anyhow::Error),

    /// Wasmer runtime error.
    #[cfg(with_wasmer)]
    #[error(transparent)]
    Wasmer(#[from] wasmer::RuntimeError),

    /// Attempt to access an invalid memory address using Wasmer.
    #[cfg(with_wasmer)]
    #[error(transparent)]
    WasmerMemory(#[from] wasmer::MemoryAccessError),

    /// Wasmtime error.
    #[cfg(with_wasmtime)]
    #[error(transparent)]
    Wasmtime(anyhow::Error),

    /// Wasmtime trap during execution.
    #[cfg(with_wasmtime)]
    #[error(transparent)]
    WasmtimeTrap(#[from] wasmtime::Trap),
}
