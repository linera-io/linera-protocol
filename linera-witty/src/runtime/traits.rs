// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Abstractions over different Wasm runtime implementations.

/// A Wasm runtime.
///
/// Shared types between different guest instances that use the same runtime.
pub trait Runtime: Sized {
    /// A handle to something exported from a guest Wasm module.
    type Export;
}

/// An active guest Wasm module.
pub trait Instance: Sized {
    /// The runtime this instance is running in.
    type Runtime: Runtime;

    /// Loads an export from the guest module.
    fn load_export(&mut self, name: &str) -> Option<<Self::Runtime as Runtime>::Export>;
}
