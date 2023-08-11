// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for the [Wasmer](https://wasmer.io) runtime.

mod function;
mod memory;
mod parameters;
mod results;

use super::traits::{Instance, Runtime};
use std::sync::{Arc, Mutex};
use wasmer::{
    AsStoreMut, AsStoreRef, Engine, Extern, FunctionEnv, FunctionEnvMut, Imports,
    InstantiationError, Memory, Module, Store, StoreMut, StoreRef,
};
use wasmer_vm::StoreObjects;

/// Representation of the [Wasmer](https://wasmer.io) runtime.
pub struct Wasmer;

impl Runtime for Wasmer {
    type Export = Extern;
    type Memory = Memory;
}

/// Helper to create Wasmer [`Instance`] implementations.
pub struct InstanceBuilder {
    store: Store,
    imports: Imports,
    environment: InstanceSlot,
}

impl InstanceBuilder {
    /// Creates a new [`InstanceBuilder`].
    pub fn new(engine: Engine) -> Self {
        InstanceBuilder {
            store: Store::new(engine),
            imports: Imports::default(),
            environment: InstanceSlot::new(None),
        }
    }

    /// Returns a reference to the [`Store`] used in this [`InstanceBuilder`].
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Creates a [`FunctionEnv`] representing the instance of this [`InstanceBuilder`].
    ///
    /// This can be used when exporting host functions that may perform reentrant calls.
    pub fn environment(&mut self) -> FunctionEnv<InstanceSlot> {
        FunctionEnv::new(&mut self.store, self.environment.clone())
    }

    /// Defines a new import for the Wasm guest instance.
    pub fn define(&mut self, namespace: &str, name: &str, value: impl Into<Extern>) {
        self.imports.define(namespace, name, value);
    }

    /// Creates an [`EntrypointInstance`] from this [`InstanceBuilder`].
    #[allow(clippy::result_large_err)]
    pub fn instantiate(
        mut self,
        module: &Module,
    ) -> Result<EntrypointInstance, InstantiationError> {
        let instance = wasmer::Instance::new(&mut self.store, module, &self.imports)?;

        *self
            .environment
            .instance
            .try_lock()
            .expect("Unexpected usage of instance before it was initialized") = Some(instance);

        Ok(EntrypointInstance {
            store: self.store,
            instance: self.environment,
        })
    }
}

/// Necessary data for implementing an entrypoint [`Instance`].
pub struct EntrypointInstance {
    store: Store,
    instance: InstanceSlot,
}

impl AsStoreRef for EntrypointInstance {
    fn as_store_ref(&self) -> StoreRef<'_> {
        self.store.as_store_ref()
    }
}

impl AsStoreMut for EntrypointInstance {
    fn as_store_mut(&mut self) -> StoreMut<'_> {
        self.store.as_store_mut()
    }

    fn objects_mut(&mut self) -> &mut StoreObjects {
        self.store.objects_mut()
    }
}

impl Instance for EntrypointInstance {
    type Runtime = Wasmer;

    fn load_export(&mut self, name: &str) -> Option<Extern> {
        self.instance.load_export(name)
    }
}

/// Alias for the [`Instance`] implementation made available inside host functions called by the
/// guest.
pub type ReentrantInstance<'a> = FunctionEnvMut<'a, InstanceSlot>;

impl Instance for ReentrantInstance<'_> {
    type Runtime = Wasmer;

    fn load_export(&mut self, name: &str) -> Option<Extern> {
        self.data_mut().load_export(name)
    }
}

/// A slot to store a [`wasmer::Instance`] in a way that can be shared with reentrant calls.
#[derive(Clone)]
pub struct InstanceSlot {
    instance: Arc<Mutex<Option<wasmer::Instance>>>,
}

impl InstanceSlot {
    /// Creates a new [`InstanceSlot`] using the optionally provided `instance`.
    fn new(instance: impl Into<Option<wasmer::Instance>>) -> Self {
        InstanceSlot {
            instance: Arc::new(Mutex::new(instance.into())),
        }
    }

    /// Loads an export from the current instance.
    ///
    /// # Panics
    ///
    /// If the underlying instance is accessed concurrently or if the slot is empty.
    fn load_export(&mut self, name: &str) -> Option<Extern> {
        self.instance
            .try_lock()
            .expect("Unexpected reentrant access to data")
            .as_mut()
            .expect("Unexpected attempt to load an export before instance is created")
            .exports
            .get_extern(name)
            .cloned()
    }
}
