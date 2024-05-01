// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for the [Wasmer](https://wasmer.io) runtime.

mod export_function;
mod function;
mod memory;
mod parameters;
mod results;

use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

pub use wasmer::FunctionEnvMut;
use wasmer::{
    AsStoreMut, AsStoreRef, Engine, Extern, FunctionEnv, Imports, InstantiationError, Memory,
    Module, Store, StoreMut, StoreObjects, StoreRef,
};

pub use self::{parameters::WasmerParameters, results::WasmerResults};
use super::traits::{Instance, Runtime};

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
    environment: Environment<UserData>,
}

impl<UserData: 'static> InstanceBuilder<UserData> {
    /// Creates a new [`InstanceBuilder`].
    pub fn new(engine: Engine, user_data: UserData) -> Self {
        InstanceBuilder {
            store: Store::new(engine),
            imports: Imports::default(),
            environment: Environment::new(user_data),
        }
    }

    /// Returns a reference to the [`Store`] used in this [`InstanceBuilder`].
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Creates a [`FunctionEnv`] representing the instance of this [`InstanceBuilder`].
    ///
    /// This can be used when exporting host functions that may perform reentrant calls.
    pub fn environment(&mut self) -> FunctionEnv<Environment<UserData>> {
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
        self.environment
            .exports
            .set(instance.exports.clone())
            .expect("Environment already initialized");
        Ok(EntrypointInstance {
            store: self.store,
            instance,
            instance_slot: self.environment,
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
    instance: wasmer::Instance,
    instance_slot: Environment<UserData>,
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

impl<UserData> EntrypointInstance<UserData> {
    /// Returns mutable references to the [`Store`] and the [`wasmer::Instance`] stored inside this
    /// [`EntrypointInstance`].
    pub fn as_store_and_instance_mut(&mut self) -> (StoreMut, &mut wasmer::Instance) {
        (self.store.as_store_mut(), &mut self.instance)
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
        self.instance_slot.load_export(name)
    }

    fn user_data(&self) -> Self::UserDataReference<'_> {
        self.instance_slot.user_data()
    }

    fn user_data_mut(&mut self) -> Self::UserDataMutReference<'_> {
        self.instance_slot.user_data()
    }
}

/// Alias for the [`Instance`] implementation made available inside host functions called by the
/// guest.
pub type ReentrantInstance<'a, UserData> = FunctionEnvMut<'a, Environment<UserData>>;

impl<UserData: 'static> Instance for ReentrantInstance<'_, UserData> {
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
pub struct Environment<UserData> {
    exports: Arc<OnceLock<wasmer::Exports>>,
    user_data: Arc<Mutex<UserData>>,
}

impl<UserData> Environment<UserData> {
    /// Creates a new [`Environment`] with no associated instance.
    fn new(user_data: UserData) -> Self {
        Environment {
            exports: Arc::new(OnceLock::new()),
            user_data: Arc::new(Mutex::new(user_data)),
        }
    }

    /// Loads an export from the current instance.
    ///
    /// # Panics
    ///
    /// If the slot is empty.
    fn load_export(&mut self, name: &str) -> Option<Extern> {
        self.exports
            .get()
            .expect("Attempted to get export before instance is loaded")
            .get_extern(name)
            .cloned()
    }

    /// Returns a reference to the `UserData` stored in this [`Environment`].
    fn user_data(&self) -> MutexGuard<'_, UserData> {
        self.user_data
            .try_lock()
            .expect("Unexpected reentrant access to data")
    }
}

impl<UserData> Clone for Environment<UserData> {
    fn clone(&self) -> Self {
        Environment {
            exports: self.exports.clone(),
            user_data: self.user_data.clone(),
        }
    }
}
