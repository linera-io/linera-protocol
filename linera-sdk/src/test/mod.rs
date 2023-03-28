// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types for writing integration tests for WebAssembly applications.
//!
//! Integration tests are usually written in the `tests` directory in the root of the crate's
//! directory (i.e., beside the `src` directory). Linera application integration tests should be
//! executed targeting the host architecture, instead of targeting `wasm32-unknown-unknown` like
//! done for unit tests.

#![cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "wasmer", feature = "wasmtime")
))]

mod validator;

pub use self::validator::TestValidator;
