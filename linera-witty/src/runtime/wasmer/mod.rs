// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for the [Wasmer](https://wasmer.io) runtime.

use super::traits::Runtime;
use wasmer::{Extern, Memory};

/// Representation of the [Wasmer](https://wasmer.io) runtime.
pub struct Wasmer;

impl Runtime for Wasmer {
    type Export = Extern;
    type Memory = Memory;
}
