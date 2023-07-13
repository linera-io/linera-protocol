// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Common error type for usage of different Wasm runtimes.

use thiserror::Error;

/// Errors that can occur when using a Wasm runtime.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// Attempt to load a function not exported from a module.
    #[error("Function `{_0}` could not be found in the module's exports")]
    FunctionNotFound(String),

    /// Attempt to load a function with a name that's used for a different import in the module.
    #[error("Export `{_0}` is not a function")]
    NotAFunction(String),
}
