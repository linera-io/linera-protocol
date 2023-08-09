// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A dummy runtime implementation useful for tests.
//!
//! No WebAssembly bytecode can be executed, but it allows calling the canonical ABI functions
//! related to memory allocation.

use super::{
    GuestPointer, Instance, InstanceWithFunction, InstanceWithMemory, Runtime, RuntimeError,
    RuntimeMemory,
};
use frunk::{hlist, hlist_pat, HList};
use std::{
    any::Any,
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// A fake Wasm runtime.
pub struct MockRuntime;

impl Runtime for MockRuntime {
    type Export = String;
    type Memory = Arc<Mutex<Vec<u8>>>;
}

/// A closure for handling calls to mocked exported guest functions.
pub type ExportedFunctionHandler = Box<dyn Fn(Box<dyn Any>) -> Result<Box<dyn Any>, RuntimeError>>;

/// A fake Wasm instance.
///
/// Only contains exports for the memory and the canonical ABI allocation functions.
#[derive(Default)]
pub struct MockInstance {
    memory: Arc<Mutex<Vec<u8>>>,
    exported_functions: HashMap<String, ExportedFunctionHandler>,
}

impl MockInstance {
    /// Adds a mock exported function to this [`MockInstance`].
    ///
    /// The `handler` will be called whenever the exported function is called.
    pub fn with_exported_function<Parameters, Results, Handler>(
        mut self,
        name: impl Into<String>,
        handler: Handler,
    ) -> Self
    where
        Parameters: 'static,
        Results: 'static,
        Handler: Fn(Parameters) -> Result<Results, RuntimeError> + 'static,
    {
        self.add_exported_function(name, handler);
        self
    }

    /// Adds a mock exported function to this [`MockInstance`].
    ///
    /// The `handler` will be called whenever the exported function is called.
    pub fn add_exported_function<Parameters, Results, Handler>(
        &mut self,
        name: impl Into<String>,
        handler: Handler,
    ) -> &mut Self
    where
        Parameters: 'static,
        Results: 'static,
        Handler: Fn(Parameters) -> Result<Results, RuntimeError> + 'static,
    {
        self.exported_functions.insert(
            name.into(),
            Box::new(move |boxed_parameters| {
                let parameters = boxed_parameters
                    .downcast()
                    .expect("Incorrect parameters used to call handler for exported function");

                handler(*parameters).map(|results| Box::new(results) as Box<dyn Any>)
            }),
        );
        self
    }
}

impl Instance for MockInstance {
    type Runtime = MockRuntime;

    fn load_export(&mut self, name: &str) -> Option<String> {
        match name {
            "memory" | "cabi_realloc" | "cabi_free" => Some(name.to_owned()),
            _ => None,
        }
    }
}

// Support for `cabi_free`.
impl InstanceWithFunction<HList![i32], HList![]> for MockInstance {
    type Function = ();

    fn function_from_export(
        &mut self,
        _: <Self::Runtime as Runtime>::Export,
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
impl InstanceWithFunction<HList![i32, i32, i32, i32], HList![i32]> for MockInstance {
    type Function = ();

    fn function_from_export(
        &mut self,
        _: <Self::Runtime as Runtime>::Export,
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
            .expect("Panic while holding a lock to a `MockInstance`'s memory");

        let address = GuestPointer(memory.len().try_into()?).aligned_at(alignment as u32);

        memory.resize(address.0 as usize + allocation_size, 0);

        assert!(
            memory.len() <= i32::MAX as usize,
            "No more memory for allocations"
        );

        Ok(hlist![address.0 as i32])
    }
}

impl RuntimeMemory<MockInstance> for Arc<Mutex<Vec<u8>>> {
    fn read<'instance>(
        &self,
        instance: &'instance MockInstance,
        location: GuestPointer,
        length: u32,
    ) -> Result<Cow<'instance, [u8]>, RuntimeError> {
        let memory = instance
            .memory
            .lock()
            .expect("Panic while holding a lock to a `MockInstance`'s memory");

        let start = location.0 as usize;
        let end = start + length as usize;

        Ok(Cow::Owned(memory[start..end].to_owned()))
    }

    fn write(
        &mut self,
        instance: &mut MockInstance,
        location: GuestPointer,
        bytes: &[u8],
    ) -> Result<(), RuntimeError> {
        let mut memory = instance
            .memory
            .lock()
            .expect("Panic while holding a lock to a `MockInstance`'s memory");

        let start = location.0 as usize;
        let end = start + bytes.len();

        memory[start..end].copy_from_slice(bytes);

        Ok(())
    }
}

impl InstanceWithMemory for MockInstance {
    fn memory_from_export(
        &self,
        export: String,
    ) -> Result<Option<Arc<Mutex<Vec<u8>>>>, RuntimeError> {
        if export == "memory" {
            Ok(Some(self.memory.clone()))
        } else {
            Err(RuntimeError::NotMemory)
        }
    }
}
