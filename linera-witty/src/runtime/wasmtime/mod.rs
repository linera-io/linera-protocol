// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for the [Wasmtime](https://wasmtime.dev) runtime.

use super::traits::Runtime;
use wasmtime::{Extern, Memory};

/// Representation of the [Wasmtime](https://wasmtime.dev) runtime.
pub struct Wasmtime;

impl Runtime for Wasmtime {
    type Export = Extern;
    type Memory = Memory;
}
