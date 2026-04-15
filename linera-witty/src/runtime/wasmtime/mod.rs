// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for the [Wasmtime](https://wasmtime.dev) runtime.

mod export_function;
mod function;
mod memory;
mod parameters;
mod results;

pub use anyhow;
use wasmtime::{
    AsContext, AsContextMut, Extern, Memory, Mutability, Store, StoreContext, StoreContextMut, Val,
};
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

/// Snapshot of a Wasmtime instance's mutable state (linear memory and mutable globals).
pub struct WasmInstanceSnapshot {
    memory_data: Vec<u8>,
    globals: Vec<(String, Val)>,
}

impl<UserData> EntrypointInstance<UserData> {
    /// Creates a new [`EntrypointInstance`] with the guest module
    /// [`Instance`][`wasmtime::Instance`] and [`Store`].
    pub fn new(instance: wasmtime::Instance, store: Store<UserData>) -> Self {
        EntrypointInstance { instance, store }
    }

    /// Creates a snapshot of the Wasm instance's mutable state (memory and globals).
    pub fn create_snapshot(&mut self) -> WasmInstanceSnapshot {
        let mut memory_data = Vec::new();
        let mut globals = Vec::new();

        let exports = self
            .instance
            .exports(&mut self.store)
            .map(|export| (export.name().to_string(), export.into_extern()))
            .collect::<Vec<_>>();

        for (name, ext) in exports {
            match ext {
                Extern::Memory(mem) => {
                    if memory_data.is_empty() {
                        memory_data = mem.data(&self.store).to_vec();
                    }
                }
                Extern::Global(global) => {
                    if global.ty(&self.store).mutability() == Mutability::Var {
                        globals.push((name, global.get(&mut self.store)));
                    }
                }
                _ => {}
            }
        }

        WasmInstanceSnapshot {
            memory_data,
            globals,
        }
    }

    /// Restores the Wasm instance's mutable state from a snapshot.
    pub fn restore_snapshot(&mut self, snapshot: &WasmInstanceSnapshot) {
        let exports = self
            .instance
            .exports(&mut self.store)
            .map(|export| (export.name().to_string(), export.into_extern()))
            .collect::<Vec<_>>();

        let mut memory_restored = false;
        for (name, ext) in exports {
            match ext {
                Extern::Memory(mem) if !memory_restored => {
                    mem.data_mut(&mut self.store)[..snapshot.memory_data.len()]
                        .copy_from_slice(&snapshot.memory_data);
                    memory_restored = true;
                }
                Extern::Global(global) => {
                    if let Some((_, val)) = snapshot.globals.iter().find(|(n, _)| n == &name) {
                        global
                            .set(&mut self.store, *val)
                            .expect("Failed to restore Wasm global from snapshot");
                    }
                }
                _ => {}
            }
        }
    }
}

impl<UserData> AsContext for EntrypointInstance<UserData> {
    type Data = UserData;

    fn as_context(&self) -> StoreContext<'_, UserData> {
        self.store.as_context()
    }
}

impl<UserData> AsContextMut for EntrypointInstance<UserData> {
    fn as_context_mut(&mut self) -> StoreContextMut<'_, UserData> {
        self.store.as_context_mut()
    }
}

impl<UserData> Instance for EntrypointInstance<UserData> {
    type Runtime = Wasmtime;
    type UserData = UserData;
    type UserDataReference<'a>
        = &'a UserData
    where
        Self: 'a,
        UserData: 'a;
    type UserDataMutReference<'a>
        = &'a mut UserData
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
    type UserDataReference<'a>
        = &'a UserData
    where
        Self: 'a,
        UserData: 'a;
    type UserDataMutReference<'a>
        = &'a mut UserData
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
