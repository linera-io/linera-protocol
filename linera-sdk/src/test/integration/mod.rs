// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types for writing integration tests for WebAssembly applications.
//!
//! Integration tests are usually written in the `tests` directory in the root of the crate's
//! directory (i.e., beside the `src` directory). Linera application integration tests should be
//! executed targeting the host architecture, instead of targeting `wasm32-unknown-unknown` like
//! done for unit tests.

#![cfg(any(with_testing, feature = "wasmer", feature = "wasmtime"))]

#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
mod block;
#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
mod chain;
mod mock_stubs;
#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
mod validator;

#[cfg(with_testing)]
pub use self::mock_stubs::*;
#[cfg(any(feature = "wasmer", feature = "wasmtime"))]
pub use self::{block::BlockBuilder, chain::ActiveChain, validator::TestValidator};
use crate::{Contract, ContractRuntime, Service, ServiceRuntime};

/// Creates a [`ContractRuntime`] to use in tests.
pub fn test_contract_runtime<Application: Contract>() -> ContractRuntime<Application> {
    ContractRuntime::new()
}

/// Creates a [`ServiceRuntime`] to use in tests.
pub fn test_service_runtime<Application: Service>() -> ServiceRuntime<Application> {
    ServiceRuntime::new()
}
