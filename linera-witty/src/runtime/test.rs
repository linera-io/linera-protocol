// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A dummy runtime implementation useful for tests.
//!
//! No WebAssembly bytecode can be executed, but it allows calling the canonical ABI functions
//! related to memory allocation.

use std::{
    any::Any,
    borrow::Cow,
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

use frunk::{hlist, hlist_pat, HList};

use super::{
    GuestPointer, Instance, InstanceWithFunction, InstanceWithMemory, Runtime, RuntimeError,
    RuntimeMemory,
};
use crate::{memory_layout::FlatLayout, ExportFunction, WitLoad, WitStore};

/// A fake Wasm runtime.
pub struct MockRuntime;

impl Runtime for MockRuntime {
    type Export = String;
    type Memory = Arc<Mutex<Vec<u8>>>;
}

/// A closure for handling calls to mocked functions.
pub type FunctionHandler<UserData> =
    Arc<dyn Fn(MockInstance<UserData>, Box<dyn Any>) -> Result<Box<dyn Any>, RuntimeError>>;

/// A fake Wasm instance.
///
/// Only contains exports for the memory and the canonical ABI allocation functions.
pub struct MockInstance<UserData> {
    memory: Arc<Mutex<Vec<u8>>>,
    exported_functions: HashMap<String, FunctionHandler<UserData>>,
    imported_functions: HashMap<String, FunctionHandler<UserData>>,
    user_data: Arc<Mutex<UserData>>,
}

impl<UserData> Default for MockInstance<UserData>
where
    UserData: Default,
{
    fn default() -> Self {
        MockInstance::new(UserData::default())
    }
}

impl<UserData> Clone for MockInstance<UserData> {
    fn clone(&self) -> Self {
        MockInstance {
            memory: self.memory.clone(),
            exported_functions: self.exported_functions.clone(),
            imported_functions: self.imported_functions.clone(),
            user_data: self.user_data.clone(),
        }
    }
}

impl<UserData> MockInstance<UserData> {
    /// Creates a new [`MockInstance`] using the provided `user_data`.
    pub fn new(user_data: UserData) -> Self {
        let memory = Arc::new(Mutex::new(Vec::new()));

        MockInstance {
            memory: memory.clone(),
            exported_functions: HashMap::new(),
            imported_functions: HashMap::new(),
            user_data: Arc::new(Mutex::new(user_data)),
        }
        .with_exported_function("cabi_free", |_, _: HList![i32]| Ok(hlist![]))
        .with_exported_function(
            "cabi_realloc",
            move |_,
                  hlist_pat![_old_address, _old_size, alignment, new_size]: HList![
                i32, i32, i32, i32
            ]| {
                let allocation_size = usize::try_from(new_size)
                    .expect("Failed to allocate a negative amount of memory");

                let mut memory = memory
                    .lock()
                    .expect("Panic while holding a lock to a `MockInstance`'s memory");

                let address = GuestPointer(memory.len().try_into()?).aligned_at(alignment as u32);

                memory.resize(address.0 as usize + allocation_size, 0);

                assert!(
                    memory.len() <= i32::MAX as usize,
                    "No more memory for allocations"
                );

                Ok(hlist![address.0 as i32])
            },
        )
    }
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
        Handler: Fn(MockInstance<UserData>, Parameters) -> Result<Results, RuntimeError> + 'static,
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
        Handler: Fn(MockInstance<UserData>, Parameters) -> Result<Results, RuntimeError> + 'static,
    {
        self.exported_functions.insert(
            name.into(),
            Arc::new(move |caller, boxed_parameters| {
                let parameters = boxed_parameters
                    .downcast()
                    .expect("Incorrect parameters used to call handler for exported function");

                handler(caller, *parameters).map(|results| Box::new(results) as Box<dyn Any>)
            }),
        );
        self
    }

    /// Calls a function that the mock instance imported from the host.
    pub fn call_imported_function<Parameters, Results>(
        &self,
        function: &str,
        parameters: Parameters,
    ) -> Result<Results, RuntimeError>
    where
        Parameters: WitStore + 'static,
        Results: WitLoad + 'static,
    {
        let handler = self
            .imported_functions
            .get(function)
            .unwrap_or_else(|| panic!("Missing function imported from host: {function:?}"));

        let flat_parameters = parameters.lower(&mut self.clone().memory()?)?;
        let boxed_flat_results = handler(self.clone(), Box::new(flat_parameters))?;
        let flat_results = *boxed_flat_results
            .downcast()
            .expect("Expected an incorrect results type from imported host function");

        Results::lift_from(flat_results, &self.clone().memory()?)
    }

    /// Returns a copy of the current memory contents.
    pub fn memory_contents(&self) -> Vec<u8> {
        self.memory.lock().unwrap().clone()
    }
}

impl<UserData> Instance for MockInstance<UserData> {
    type Runtime = MockRuntime;
    type UserData = UserData;
    type UserDataReference<'a> = MutexGuard<'a, UserData>
    where
        Self::UserData: 'a,
        Self: 'a;
    type UserDataMutReference<'a> = MutexGuard<'a, UserData>
    where
        Self::UserData: 'a,
        Self: 'a;

    fn load_export(&mut self, name: &str) -> Option<String> {
        if name == "memory" || self.exported_functions.contains_key(name) {
            Some(name.to_owned())
        } else {
            None
        }
    }

    fn user_data(&self) -> Self::UserDataReference<'_> {
        self.user_data
            .try_lock()
            .expect("Unexpected reentrant access to user data in `MockInstance`")
    }

    fn user_data_mut(&mut self) -> Self::UserDataMutReference<'_> {
        self.user_data
            .try_lock()
            .expect("Unexpected reentrant access to user data in `MockInstance`")
    }
}

impl<Parameters, Results, UserData> InstanceWithFunction<Parameters, Results>
    for MockInstance<UserData>
where
    Parameters: FlatLayout + 'static,
    Results: FlatLayout + 'static,
{
    type Function = String;

    fn function_from_export(
        &mut self,
        name: <Self::Runtime as Runtime>::Export,
    ) -> Result<Option<Self::Function>, RuntimeError> {
        Ok(Some(name))
    }

    fn call(
        &mut self,
        function: &Self::Function,
        parameters: Parameters,
    ) -> Result<Results, RuntimeError> {
        let handler = self
            .exported_functions
            .get(function)
            .ok_or_else(|| RuntimeError::FunctionNotFound(function.clone()))?;

        let results = handler(self.clone(), Box::new(parameters))?;

        Ok(*results.downcast().unwrap_or_else(|_| {
            panic!("Incorrect results type expected from handler of expected function: {function}")
        }))
    }
}

impl<UserData> RuntimeMemory<MockInstance<UserData>> for Arc<Mutex<Vec<u8>>> {
    fn read<'instance>(
        &self,
        instance: &'instance MockInstance<UserData>,
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
        instance: &mut MockInstance<UserData>,
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

impl<UserData> InstanceWithMemory for MockInstance<UserData> {
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

impl<Handler, Parameters, Results, UserData> ExportFunction<Handler, Parameters, Results>
    for MockInstance<UserData>
where
    Handler: Fn(MockInstance<UserData>, Parameters) -> Result<Results, RuntimeError> + 'static,
    Parameters: 'static,
    Results: 'static,
{
    fn export(
        &mut self,
        module_name: &str,
        function_name: &str,
        handler: Handler,
    ) -> Result<(), RuntimeError> {
        let name = format!("{module_name}#{function_name}");

        self.imported_functions.insert(
            name.clone(),
            Arc::new(move |instance, boxed_parameters| {
                let parameters = boxed_parameters.downcast().unwrap_or_else(|_| {
                    panic!(
                        "Incorrect parameters used to call handler for exported function {name:?}"
                    )
                });

                let results = handler(instance, *parameters)?;

                Ok(Box::new(results))
            }),
        );

        Ok(())
    }
}

/// A helper trait to serve as an equivalent to `crate::wasmer::WasmerResults` and
/// `crate::wasmtime::WasmtimeResults` for the [`MockInstance`].
///
/// This is in order to help with writing tests generic over the Wasm guest instance type.
pub trait MockResults {
    /// The mock native type of the results for the [`MockInstance`].
    type Results;
}

impl<T> MockResults for T {
    type Results = T;
}

/// A helper type to verify how many times an exported function is called.
pub struct MockExportedFunction<Parameters, Results, UserData> {
    name: String,
    call_counter: Arc<AtomicUsize>,
    expected_calls: usize,
    handler: Arc<dyn Fn(MockInstance<UserData>, Parameters) -> Result<Results, RuntimeError>>,
}

impl<Parameters, Results, UserData> MockExportedFunction<Parameters, Results, UserData>
where
    Parameters: 'static,
    Results: 'static,
    UserData: 'static,
{
    /// Creates a new [`MockExportedFunction`] for the exported function with the provided `name`.
    ///
    /// Every call to the exported function is called is forwarded to the `handler` and an internal
    /// counter is incremented. When the [`MockExportedFunction`] instance is dropped (which should
    /// be done at the end of the test), it asserts that the function was called `expected_calls`
    /// times.
    pub fn new(
        name: impl Into<String>,
        handler: impl Fn(MockInstance<UserData>, Parameters) -> Result<Results, RuntimeError> + 'static,
        expected_calls: usize,
    ) -> Self {
        MockExportedFunction {
            name: name.into(),
            call_counter: Arc::default(),
            expected_calls,
            handler: Arc::new(handler),
        }
    }

    /// Registers this [`MockExportedFunction`] with the mock `instance`.
    pub fn register(&self, instance: &mut MockInstance<UserData>) {
        let call_counter = self.call_counter.clone();
        let handler = self.handler.clone();

        instance.add_exported_function(self.name.clone(), move |caller, parameters: Parameters| {
            call_counter.fetch_add(1, Ordering::AcqRel);
            handler(caller, parameters)
        });
    }
}

impl<Parameters, Results, UserData> Drop for MockExportedFunction<Parameters, Results, UserData> {
    fn drop(&mut self) {
        assert_eq!(
            self.call_counter.load(Ordering::Acquire),
            self.expected_calls,
            "Unexpected number of calls to `{}`",
            self.name
        );
    }
}
