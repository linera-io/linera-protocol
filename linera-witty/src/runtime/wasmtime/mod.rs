// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for the [Wasmtime](https://wasmtime.dev) runtime.

mod export_function;
mod function;
mod memory;
mod parameters;
mod results;

pub use self::{parameters::WasmtimeParameters, results::WasmtimeResults};
use super::traits::{Instance, Runtime};
pub use anyhow;
use wasmtime::{AsContext, AsContextMut, Extern, Memory, Store, StoreContext, StoreContextMut};
pub use wasmtime::{Caller, Linker};

/// Representation of the [Wasmtime](https://wasmtime.dev) runtime.
pub struct Wasmtime;

impl Runtime for Wasmtime {
    type Export = Extern;
    type Memory = Memory;
}

/// Necessary data for implementing an entrypoint [`Instance`].
pub struct EntrypointInstance<UserData = ()> {
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

impl AsContext for EntrypointInstance {
    type Data = ();

    fn as_context(&self) -> StoreContext<()> {
        self.store.as_context()
    }
}

impl AsContextMut for EntrypointInstance {
    fn as_context_mut(&mut self) -> StoreContextMut<()> {
        self.store.as_context_mut()
    }
}

impl Instance for EntrypointInstance {
    type Runtime = Wasmtime;

    fn load_export(&mut self, name: &str) -> Option<Extern> {
        self.instance.get_export(&mut self.store, name)
    }
}

/// Alias for the [`Instance`] implementation made available inside host functions called by the
/// guest.
pub type ReentrantInstance<'a> = Caller<'a, ()>;

impl Instance for Caller<'_, ()> {
    type Runtime = Wasmtime;

    fn load_export(&mut self, name: &str) -> Option<Extern> {
        Caller::get_export(self, name)
    }
}
