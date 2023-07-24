// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code to interface with different runtimes.

mod error;
mod memory;
#[cfg(any(test, feature = "test"))]
mod test;
mod traits;

#[cfg(any(test, feature = "test"))]
pub use self::test::FakeRuntime;
pub use self::{
    error::RuntimeError,
    memory::{GuestPointer, Memory, RuntimeMemory},
    traits::{Instance, InstanceWithFunction, InstanceWithMemory, Runtime},
};
