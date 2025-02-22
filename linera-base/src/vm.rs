// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The virtual machines being supported.

use std::str::FromStr;

use async_graphql::scalar;
use derive_more::Display;
use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The runtime to use for running the application.
#[derive(
    Clone,
    Copy,
    Display,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    WitType,
    WitStore,
    WitLoad,
    Debug,
)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
pub enum WasmRuntime {
    /// The choice of the Wasmer runtime for WebAssembly
    #[display("wasmer")]
    Wasmer,
    /// The choice of the Wasmtime runtime for WebAssembly
    #[display("wasmtime")]
    Wasmtime,
    /// The choice of the Wasmer with sanitizer runtime for WebAssembly
    WasmerWithSanitizer,
    /// The choice of the Wasmtime with sanitizer runtime for WebAssembly
    WasmtimeWithSanitizer,
}

impl WasmRuntime {
    /// Returns whether we need a stabilizer or not.
    pub fn needs_sanitizer(self) -> bool {
        matches!(
            self,
            WasmRuntime::WasmerWithSanitizer | WasmRuntime::WasmtimeWithSanitizer
        )
    }
}

impl FromStr for WasmRuntime {
    type Err = InvalidVmRuntime;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        match string {
            "wasmer" => Ok(WasmRuntime::Wasmer),
            "wasmtime" => Ok(WasmRuntime::Wasmtime),
            unknown => Err(InvalidVmRuntime(unknown.to_owned())),
        }
    }
}

scalar!(WasmRuntime);

#[derive(
    Clone,
    Copy,
    Debug,
    Display,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    WitType,
    WitStore,
    WitLoad,
)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
/// The choice of runtime for handling the Evm.
pub enum EvmRuntime {
    /// The choice of the Revm runtime for Evm.
    Revm,
}

scalar!(EvmRuntime);

#[derive(
    Clone,
    Copy,
    Display,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    WitType,
    WitStore,
    WitLoad,
    Debug,
)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
/// The virtual machine runtime
pub enum VmRuntime {
    /// The Wasm for the virtual machine
    Wasm(WasmRuntime),
    /// The Evm for the virtual machine
    Evm(EvmRuntime),
}

impl FromStr for VmRuntime {
    type Err = InvalidVmRuntime;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        match string {
            "wasmer" => Ok(VmRuntime::Wasm(WasmRuntime::Wasmer)),
            "wasmtime" => Ok(VmRuntime::Wasm(WasmRuntime::Wasmtime)),
            "revm" => Ok(VmRuntime::Evm(EvmRuntime::Revm)),
            unknown => Err(InvalidVmRuntime(unknown.to_owned())),
        }
    }
}

scalar!(VmRuntime);

#[cfg(with_testing)]
impl Default for VmRuntime {
    fn default() -> Self {
        cfg_if::cfg_if! {
            if #[cfg(with_wasmer)] {
                VmRuntime::Wasm(WasmRuntime::Wasmer)
            } else if #[cfg(with_wasmtime)] {
                VmRuntime::Wasm(WasmRuntime::Wasmtime)
            } else if #[cfg(with_revm)] {
                VmRuntime::Evm(EvmRuntime::Revm)
            } else {
                panic!("Cannot call default  for VmRuntime without wasmer, or wasmtime or revm features");
            }
        }
    }
}

/// Attempts to create an invalid [`VmRuntime`] instance from a string.
#[derive(Clone, Debug, Error)]
#[error("{0:?} is not a valid virtual machine runtime")]
pub struct InvalidVmRuntime(String);
