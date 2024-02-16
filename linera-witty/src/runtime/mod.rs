// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code to interface with different runtimes.

mod borrowed_instance;
mod error;
mod memory;
#[cfg(with_testing)]
mod test;
mod traits;
#[cfg(with_wasmer)]
pub mod wasmer;
#[cfg(with_wasmtime)]
pub mod wasmtime;

#[cfg(with_testing)]
pub use self::test::{MockExportedFunction, MockInstance, MockResults, MockRuntime};
pub use self::{
    error::RuntimeError,
    memory::{GuestPointer, Memory, RuntimeMemory},
    traits::{Instance, InstanceWithFunction, InstanceWithMemory, Runtime},
};
