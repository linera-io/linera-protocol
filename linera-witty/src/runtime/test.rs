// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A dummy runtime implementation useful for tests.
//!
//! No WebAssembly bytecode can be executed, but it allows calling the canonical ABI functions
//! related to memory allocation.

use super::{Instance, Runtime};
use std::sync::{Arc, Mutex};

/// A fake Wasm runtime.
pub struct FakeRuntime;

impl Runtime for FakeRuntime {
    type Export = ();
    type Memory = Arc<Mutex<Vec<u8>>>;
}

/// A fake Wasm instance.
///
/// Only contains exports for the memory and the canonical ABI allocation functions.
#[derive(Default)]
pub struct FakeInstance {
    memory: Arc<Mutex<Vec<u8>>>,
}

impl Instance for FakeInstance {
    type Runtime = FakeRuntime;

    fn load_export(&mut self, name: &str) -> Option<()> {
        match name {
            "memory" | "cabi_realloc" | "cabi_free" => Some(()),
            _ => None,
        }
    }
}
