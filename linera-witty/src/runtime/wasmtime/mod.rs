// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for the [Wasmtime](https://wasmtime.dev) runtime.

mod export_function;
mod function;
mod memory;
mod parameters;
mod results;

pub use anyhow;
use serde::{Deserialize, Serialize};
use wasmtime::{
    AsContext, AsContextMut, Extern, Memory, Mutability, Store, StoreContext, StoreContextMut,
};
pub use wasmtime::{Caller, Linker};

pub use self::{parameters::WasmtimeParameters, results::WasmtimeResults};
use super::{
    snapshot::NumericVal,
    traits::{Instance, Runtime},
};

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

/// Snapshot of a Wasmtime instance's mutable state.
///
/// Captures the data that can change at runtime for each kind of [`Extern`]:
///
/// - [`Extern::Memory`]: the bytes of every exported linear memory, by
///   export name. Standard Linera modules export a single memory, but the
///   data type permits the Wasm `multi-memory` proposal.
/// - [`Extern::Global`]: the value of every exported mutable global.
/// - [`Extern::Table`]: the element count of every exported table. Tables
///   are initialised from the module's `elem` section and Linera contracts
///   do not mutate them after instantiation, so only the size is tracked
///   and the same size is required of the instance at restore time.
///
/// The remaining variants carry no snapshottable state:
///
/// - [`Extern::Func`] holds an immutable reference to compiled code.
/// - [`Extern::SharedMemory`] is rejected (Linera does not use the Wasm
///   threading proposal); attempting to snapshot or restore one panics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmInstanceSnapshot {
    memories: Vec<(String, Vec<u8>)>,
    globals: Vec<(String, NumericVal)>,
    table_sizes: Vec<(String, u64)>,
}

fn wasmtime_val_to_numeric(val: &wasmtime::Val) -> NumericVal {
    match val {
        wasmtime::Val::I32(v) => NumericVal::I32(*v),
        wasmtime::Val::I64(v) => NumericVal::I64(*v),
        wasmtime::Val::F32(bits) => NumericVal::F32(*bits),
        wasmtime::Val::F64(bits) => NumericVal::F64(*bits),
        wasmtime::Val::V128(v) => NumericVal::V128(v.as_u128()),
        wasmtime::Val::FuncRef(_) | wasmtime::Val::ExternRef(_) | wasmtime::Val::AnyRef(_) => {
            panic!("Reference-typed mutable globals cannot be snapshotted")
        }
    }
}

fn numeric_to_wasmtime_val(val: &NumericVal) -> wasmtime::Val {
    match val {
        NumericVal::I32(v) => wasmtime::Val::I32(*v),
        NumericVal::I64(v) => wasmtime::Val::I64(*v),
        NumericVal::F32(bits) => wasmtime::Val::F32(*bits),
        NumericVal::F64(bits) => wasmtime::Val::F64(*bits),
        NumericVal::V128(v) => wasmtime::Val::V128(wasmtime::V128::from(*v)),
    }
}

impl<UserData> EntrypointInstance<UserData> {
    /// Creates a new [`EntrypointInstance`] with the guest module
    /// [`Instance`][`wasmtime::Instance`] and [`Store`].
    pub fn new(instance: wasmtime::Instance, store: Store<UserData>) -> Self {
        EntrypointInstance { instance, store }
    }

    /// Returns mutable references to the [`Store`] and the [`wasmtime::Instance`] stored
    /// inside this [`EntrypointInstance`].
    pub fn as_store_and_instance_mut(
        &mut self,
    ) -> (StoreContextMut<'_, UserData>, &mut wasmtime::Instance) {
        (self.store.as_context_mut(), &mut self.instance)
    }

    /// Creates a snapshot of the Wasm instance's mutable state (memories, globals,
    /// table sizes).
    pub fn create_snapshot(&mut self) -> WasmInstanceSnapshot {
        let mut memories = Vec::new();
        let mut globals = Vec::new();
        let mut table_sizes = Vec::new();

        let exports = self
            .instance
            .exports(&mut self.store)
            .map(|export| (export.name().to_string(), export.into_extern()))
            .collect::<Vec<_>>();

        for (name, ext) in exports {
            match ext {
                Extern::Memory(mem) => {
                    memories.push((name, mem.data(&self.store).to_vec()));
                }
                Extern::Global(global) => {
                    // Const globals are part of the module and need no snapshot.
                    if global.ty(&self.store).mutability() == Mutability::Var {
                        globals.push((name, wasmtime_val_to_numeric(&global.get(&mut self.store))));
                    }
                }
                Extern::Table(table) => {
                    table_sizes.push((name, u64::from(table.size(&self.store))));
                }
                Extern::SharedMemory(_) => {
                    panic!("Wasm shared memories are not supported by snapshotting");
                }
                // Functions are immutable code references; nothing to snapshot.
                Extern::Func(_) => {}
            }
        }

        WasmInstanceSnapshot {
            memories,
            globals,
            table_sizes,
        }
    }

    /// Restores the Wasm instance's mutable state from a snapshot.
    pub fn restore_snapshot(&mut self, snapshot: &WasmInstanceSnapshot) {
        let exports = self
            .instance
            .exports(&mut self.store)
            .map(|export| (export.name().to_string(), export.into_extern()))
            .collect::<Vec<_>>();

        for (name, ext) in exports {
            match ext {
                Extern::Memory(mem) => {
                    if let Some((_, bytes)) =
                        snapshot.memories.iter().find(|(n, _)| n == &name)
                    {
                        // Grow the live memory to fit the snapshot if the snapshot
                        // was captured after a `memory.grow`.
                        let needed = bytes.len();
                        let current = mem.data_size(&self.store);
                        if needed > current {
                            let page_size = mem.page_size(&self.store) as usize;
                            let extra_pages = needed.div_ceil(page_size) - current / page_size;
                            mem.grow(&mut self.store, extra_pages as u64)
                                .expect("Failed to grow Wasm memory to match snapshot");
                        }
                        mem.data_mut(&mut self.store)[..needed].copy_from_slice(bytes);
                    }
                }
                Extern::Global(global) => {
                    if let Some((_, val)) = snapshot.globals.iter().find(|(n, _)| n == &name) {
                        global
                            .set(&mut self.store, numeric_to_wasmtime_val(val))
                            .expect("Failed to restore Wasm global from snapshot");
                    }
                }
                Extern::Table(table) => {
                    if let Some((_, size)) = snapshot.table_sizes.iter().find(|(n, _)| n == &name)
                    {
                        let current = u64::from(table.size(&self.store));
                        assert_eq!(
                            current, *size,
                            "Wasm table `{name}` size changed: snapshot has {size}, fresh \
                             instance has {current}; Linera contracts must not mutate tables \
                             after instantiation",
                        );
                    }
                }
                Extern::SharedMemory(_) => {
                    panic!("Wasm shared memories are not supported by snapshotting");
                }
                // Functions are immutable code references; nothing to restore.
                Extern::Func(_) => {}
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
