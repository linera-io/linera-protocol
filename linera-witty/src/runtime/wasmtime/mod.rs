// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for the [Wasmtime](https://wasmtime.dev) runtime.

mod export_function;
mod function;
mod memory;
mod parameters;
mod results;

pub use anyhow;
use wasmtime::{AsContext, AsContextMut, Extern, Memory, Store, StoreContext, StoreContextMut};
pub use wasmtime::{Caller, Linker};

pub use self::{parameters::WasmtimeParameters, results::WasmtimeResults};
use super::traits::{Instance, Runtime};

/// Representation of the [Wasmtime](https://wasmtime.dev) runtime.
pub struct Wasmtime;

impl Runtime for Wasmtime {
    type Export = Extern;
    type Memory = Memory;
}

/// Necessary data for implementing an entrypoint [`Instance`].
pub struct EntrypointInstance<UserData> {
    instance: wasmtime::Instance,
    store: Store<UserData>,
}

impl<UserData> EntrypointInstance<UserData> {
    /// Creates a new [`EntrypointInstance`] with the guest module
    /// [`Instance`][`wasmtime::Instance`] and [`Store`].
    pub fn new(instance: wasmtime::Instance, store: Store<UserData>) -> Self {
        EntrypointInstance { instance, store }
    }
}

impl<UserData> AsContext for EntrypointInstance<UserData> {
    type Data = UserData;

    fn as_context(&self) -> StoreContext<UserData> {
        self.store.as_context()
    }
}

impl<UserData> AsContextMut for EntrypointInstance<UserData> {
    fn as_context_mut(&mut self) -> StoreContextMut<UserData> {
        self.store.as_context_mut()
    }
}

impl<UserData> Instance for EntrypointInstance<UserData> {
    type Runtime = Wasmtime;
    type UserData = UserData;
    type UserDataReference<'a> = &'a UserData
    where
        Self: 'a,
        UserData: 'a;
    type UserDataMutReference<'a> = &'a mut UserData
    where
        Self: 'a,
        UserData: 'a;

    fn load_export(&mut self, name: &str) -> Option<Extern> {
        self.instance.get_export(&mut self.store, name)
    }

    fn user_data(&self) -> Self::UserDataReference<'_> {
        self.store.data()
    }

    fn user_data_mut(&mut self) -> Self::UserDataMutReference<'_> {
        self.store.data_mut()
    }
}

/// Alias for the [`Instance`] implementation made available inside host functions called by the
/// guest.
pub type ReentrantInstance<'a, UserData> = Caller<'a, UserData>;

impl<UserData> Instance for Caller<'_, UserData> {
    type Runtime = Wasmtime;
    type UserData = UserData;
    type UserDataReference<'a> = &'a UserData
    where
        Self: 'a,
        UserData: 'a;
    type UserDataMutReference<'a> = &'a mut UserData
    where
        Self: 'a,
        UserData: 'a;

    fn load_export(&mut self, name: &str) -> Option<Extern> {
        Caller::get_export(self, name)
    }

    fn user_data(&self) -> Self::UserDataReference<'_> {
        Caller::data(self)
    }

    fn user_data_mut(&mut self) -> Self::UserDataMutReference<'_> {
        Caller::data_mut(self)
    }
}
