// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A dummy runtime implementation useful for tests.
//!
//! No WebAssembly bytecode can be executed, but it allows calling the canonical ABI functions
//! related to memory allocation.

use super::{GuestPointer, Instance, InstanceWithFunction, Runtime, RuntimeError};
use frunk::{hlist, hlist_pat, HList};
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

// Support for `cabi_free`.
impl InstanceWithFunction<HList![i32], HList![]> for FakeInstance {
    type Function = ();

    fn function_from_export(
        &mut self,
        (): <Self::Runtime as Runtime>::Export,
    ) -> Result<Option<Self::Function>, RuntimeError> {
        Ok(Some(()))
    }

    fn call(
        &mut self,
        _function: &Self::Function,
        _: HList![i32],
    ) -> Result<HList![], RuntimeError> {
        Ok(hlist![])
    }
}

// Support for `cabi_realloc`.
impl InstanceWithFunction<HList![i32, i32, i32, i32], HList![i32]> for FakeInstance {
    type Function = ();

    fn function_from_export(
        &mut self,
        (): <Self::Runtime as Runtime>::Export,
    ) -> Result<Option<Self::Function>, RuntimeError> {
        Ok(Some(()))
    }

    fn call(
        &mut self,
        _function: &Self::Function,
        hlist_pat![_old_address, _old_size, alignment, new_size]: HList![i32, i32, i32, i32],
    ) -> Result<HList![i32], RuntimeError> {
        let allocation_size =
            usize::try_from(new_size).expect("Failed to allocate a negative amount of memory");

        let mut memory = self
            .memory
            .lock()
            .expect("Panic while holding a lock to a `FakeInstance`'s memory");

        let address = GuestPointer(memory.len().try_into()?).aligned_at(alignment as u32);

        memory.resize(address.0 as usize + allocation_size, 0);

        assert!(
            memory.len() <= i32::MAX as usize,
            "No more memory for allocations"
        );

        Ok(hlist![address.0 as i32])
    }
}
