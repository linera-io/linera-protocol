// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for the [Wasmer](https://wasmer.io) runtime.

mod export_function;
mod function;
mod memory;
mod parameters;
mod results;

pub use self::{parameters::WasmerParameters, results::WasmerResults};
use super::traits::{Instance, Runtime};
use std::sync::{Arc, Mutex, MutexGuard};
pub use wasmer::FunctionEnvMut;
use wasmer::{
    AsStoreMut, AsStoreRef, Engine, Extern, FunctionEnv, Imports, InstantiationError,
    Memory, Module, Store, StoreMut, StoreRef,
};
use wasmer_vm::StoreObjects;

/// Representation of the [Wasmer](https://wasmer.io) runtime.
pub struct Wasmer;

impl Runtime for Wasmer {
    type Export = Extern;
    type Memory = Memory;
}

/// Helper to create Wasmer [`Instance`] implementations.
pub struct InstanceBuilder<UserData> {
    store: Store,
    imports: Imports,
    environment: InstanceSlot<UserData>,
}

impl<UserData> InstanceBuilder<UserData>
where
    UserData: Send + 'static,
{
    /// Creates a new [`InstanceBuilder`].
    pub fn new(engine: Engine, user_data: UserData) -> Self {
        InstanceBuilder {
            store: Store::new(engine),
            imports: Imports::default(),
            environment: InstanceSlot::new(None, user_data),
        }
    }

    /// Returns a reference to the [`Store`] used in this [`InstanceBuilder`].
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Creates a [`FunctionEnv`] representing the instance of this [`InstanceBuilder`].
    ///
    /// This can be used when exporting host functions that may perform reentrant calls.
    pub fn environment(&mut self) -> FunctionEnv<InstanceSlot<UserData>> {
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
    ) -> Result<EntrypointInstance<UserData>, InstantiationError> {
        let instance = wasmer::Instance::new(&mut self.store, module, &self.imports)?;

        *self
            .environment
            .instance
            .try_lock()
            .expect("Unexpected usage of instance before it was initialized") =
            Some(instance);

        Ok(EntrypointInstance {
            store: self.store,
            instance: self.environment,
        })
    }
}

impl<UserData> AsStoreRef for InstanceBuilder<UserData> {
    fn as_store_ref(&self) -> StoreRef<'_> {
        self.store.as_store_ref()
    }
}

impl<UserData> AsStoreMut for InstanceBuilder<UserData> {
    fn as_store_mut(&mut self) -> StoreMut<'_> {
        self.store.as_store_mut()
    }

    fn objects_mut(&mut self) -> &mut StoreObjects {
        self.store.objects_mut()
    }
}

/// Necessary data for implementing an entrypoint [`Instance`].
pub struct EntrypointInstance<UserData> {
    store: Store,
    instance: InstanceSlot<UserData>,
}

impl<UserData> AsStoreRef for EntrypointInstance<UserData> {
    fn as_store_ref(&self) -> StoreRef<'_> {
        self.store.as_store_ref()
    }
}

impl<UserData> AsStoreMut for EntrypointInstance<UserData> {
    fn as_store_mut(&mut self) -> StoreMut<'_> {
        self.store.as_store_mut()
    }

    fn objects_mut(&mut self) -> &mut StoreObjects {
        self.store.objects_mut()
    }
}

impl<UserData> Instance for EntrypointInstance<UserData> {
    type Runtime = Wasmer;
    type UserData = UserData;
    type UserDataReference<'a> = MutexGuard<'a, UserData>
    where
        Self::UserData: 'a,
        Self: 'a;
    type UserDataMutReference<'a> = MutexGuard<'a, UserData>
    where
        Self::UserData: 'a,
        Self: 'a;

    fn load_export(&mut self, name: &str) -> Option<Extern> {
        self.instance.load_export(name)
    }

    fn user_data(&self) -> Self::UserDataReference<'_> {
        self.instance.user_data()
    }

    fn user_data_mut(&mut self) -> Self::UserDataMutReference<'_> {
        self.instance.user_data()
    }
}

/// Alias for the [`Instance`] implementation made available inside host functions called by the
/// guest.
pub type ReentrantInstance<'a, UserData> = FunctionEnvMut<'a, InstanceSlot<UserData>>;

impl<UserData> Instance for ReentrantInstance<'_, UserData>
where
    UserData: Send + 'static,
{
    type Runtime = Wasmer;
    type UserData = UserData;
    type UserDataReference<'a> = MutexGuard<'a, UserData>
    where
        Self::UserData: 'a,
        Self: 'a;
    type UserDataMutReference<'a> = MutexGuard<'a, UserData>
    where
        Self::UserData: 'a,
        Self: 'a;

    fn load_export(&mut self, name: &str) -> Option<Extern> {
        self.data_mut().load_export(name)
    }

    fn user_data(&self) -> Self::UserDataReference<'_> {
        FunctionEnvMut::data(self).user_data()
    }

    fn user_data_mut(&mut self) -> Self::UserDataMutReference<'_> {
        FunctionEnvMut::data_mut(self).user_data()
    }
}

/// A slot to store a [`wasmer::Instance`] in a way that can be shared with reentrant calls.
pub struct InstanceSlot<UserData> {
    instance: Arc<Mutex<Option<wasmer::Instance>>>,
    user_data: Arc<Mutex<UserData>>,
}

impl<UserData> InstanceSlot<UserData> {
    /// Creates a new [`InstanceSlot`] using the optionally provided `instance`.
    fn new(instance: impl Into<Option<wasmer::Instance>>, user_data: UserData) -> Self {
        InstanceSlot {
            instance: Arc::new(Mutex::new(instance.into())),
            user_data: Arc::new(Mutex::new(user_data)),
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

    /// Returns a reference to the `UserData` stored in this [`InstanceSlot`].
    fn user_data(&self) -> MutexGuard<'_, UserData> {
        self.user_data
            .try_lock()
            .expect("Unexpected reentrant access to data")
    }
}

impl<UserData> Clone for InstanceSlot<UserData> {
    fn clone(&self) -> Self {
        InstanceSlot {
            instance: self.instance.clone(),
            user_data: self.user_data.clone(),
        }
    }
}
