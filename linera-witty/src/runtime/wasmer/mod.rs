// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for the [Wasmer](https://wasmer.io) runtime.

mod export_function;
mod function;
mod memory;
mod parameters;
mod results;

use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use serde::{Deserialize, Serialize};
pub use wasmer::FunctionEnvMut;
use wasmer::{
    AsStoreMut, AsStoreRef, Engine, Extern, FunctionEnv, Imports, InstantiationError, Memory,
    Module, Mutability, Store, StoreMut, StoreObjects, StoreRef,
};

pub use self::{parameters::WasmerParameters, results::WasmerResults};
use super::{
    snapshot::{NumericVal, SnapshotError},
    traits::{Instance, Runtime},
};

fn wasmer_value_to_numeric(value: &wasmer::Value) -> Option<NumericVal> {
    match value {
        wasmer::Value::I32(v) => Some(NumericVal::I32(*v)),
        wasmer::Value::I64(v) => Some(NumericVal::I64(*v)),
        wasmer::Value::F32(v) => Some(NumericVal::F32(v.to_bits())),
        wasmer::Value::F64(v) => Some(NumericVal::F64(v.to_bits())),
        wasmer::Value::V128(v) => Some(NumericVal::V128(*v)),
        wasmer::Value::FuncRef(_) | wasmer::Value::ExternRef(_) => None,
    }
}

fn numeric_to_wasmer_value(val: &NumericVal) -> wasmer::Value {
    match val {
        NumericVal::I32(v) => wasmer::Value::I32(*v),
        NumericVal::I64(v) => wasmer::Value::I64(*v),
        NumericVal::F32(bits) => wasmer::Value::F32(f32::from_bits(*bits)),
        NumericVal::F64(bits) => wasmer::Value::F64(f64::from_bits(*bits)),
        NumericVal::V128(v) => wasmer::Value::V128(*v),
    }
}

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

/// Snapshot of a Wasmer instance's mutable state.
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
/// The remaining variant carries no snapshottable state:
///
/// - [`Extern::Function`] holds an immutable reference to compiled code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmInstanceSnapshot {
    memories: Vec<(String, Vec<u8>)>,
    globals: Vec<(String, NumericVal)>,
    table_sizes: Vec<(String, u64)>,
}

impl<UserData> EntrypointInstance<UserData> {
    /// Returns mutable references to the [`Store`] and the [`wasmer::Instance`] stored inside this
    /// [`EntrypointInstance`].
    pub fn as_store_and_instance_mut(&mut self) -> (StoreMut<'_>, &mut wasmer::Instance) {
        (self.store.as_store_mut(), &mut self.instance)
    }

    /// Creates a snapshot of the Wasm instance's mutable state (memories, globals,
    /// table sizes).
    pub fn create_snapshot(&mut self) -> Result<WasmInstanceSnapshot, SnapshotError> {
        let mut memories = Vec::new();
        let mut globals = Vec::new();
        let mut table_sizes = Vec::new();

        let exports: Vec<(String, Extern)> = self
            .instance
            .exports
            .iter()
            .map(|(name, ext)| (name.clone(), ext.clone()))
            .collect();

        for (name, ext) in exports {
            match ext {
                Extern::Memory(memory) => {
                    let bytes = memory.view(&self.store).copy_to_vec().map_err(|e| {
                        SnapshotError::MemoryCopy {
                            name: name.clone(),
                            message: e.to_string(),
                        }
                    })?;
                    memories.push((name, bytes));
                }
                Extern::Global(global) => {
                    // Const globals are part of the module and need no snapshot.
                    if global.ty(&self.store).mutability == Mutability::Var {
                        let val = wasmer_value_to_numeric(&global.get(&mut self.store))
                            .ok_or_else(|| SnapshotError::ReferenceTypedGlobal {
                                name: name.clone(),
                            })?;
                        globals.push((name, val));
                    }
                }
                Extern::Table(table) => {
                    table_sizes.push((name, u64::from(table.size(&self.store))));
                }
                // Functions are immutable code references; nothing to snapshot.
                Extern::Function(_) => {}
            }
        }

        Ok(WasmInstanceSnapshot {
            memories,
            globals,
            table_sizes,
        })
    }

    /// Restores the Wasm instance's mutable state from a snapshot.
    pub fn restore_snapshot(
        &mut self,
        snapshot: &WasmInstanceSnapshot,
    ) -> Result<(), SnapshotError> {
        let exports: Vec<(String, Extern)> = self
            .instance
            .exports
            .iter()
            .map(|(name, ext)| (name.clone(), ext.clone()))
            .collect();

        for (name, ext) in exports {
            match ext {
                Extern::Memory(memory) => {
                    if let Some((_, bytes)) =
                        snapshot.memories.iter().find(|(n, _)| n == &name)
                    {
                        // Grow the live memory to fit the snapshot if the snapshot
                        // was captured after a `memory.grow`.
                        let needed = bytes.len() as u64;
                        let current = memory.view(&self.store).data_size();
                        if needed > current {
                            let page_size = wasmer::WASM_PAGE_SIZE as u64;
                            let extra_pages = needed.div_ceil(page_size) - current / page_size;
                            memory
                                .grow(&mut self.store, extra_pages as u32)
                                .map_err(|e| SnapshotError::MemoryGrow {
                                    name: name.clone(),
                                    message: e.to_string(),
                                })?;
                        }
                        memory.view(&self.store).write(0, bytes).map_err(|e| {
                            SnapshotError::MemoryWrite {
                                name: name.clone(),
                                message: e.to_string(),
                            }
                        })?;
                    }
                }
                Extern::Global(global) => {
                    if let Some((_, value)) =
                        snapshot.globals.iter().find(|(n, _)| n == &name)
                    {
                        global
                            .set(&mut self.store, numeric_to_wasmer_value(value))
                            .map_err(|e| SnapshotError::GlobalSet {
                                name: name.clone(),
                                message: e.to_string(),
                            })?;
                    }
                }
                Extern::Table(table) => {
                    if let Some((_, size)) = snapshot.table_sizes.iter().find(|(n, _)| n == &name)
                    {
                        let current = u64::from(table.size(&self.store));
                        if current != *size {
                            return Err(SnapshotError::TableSizeMismatch {
                                name,
                                snapshot: *size,
                                current,
                            });
                        }
                    }
                }
                // Functions are immutable code references; nothing to restore.
                Extern::Function(_) => {}
            }
        }
        Ok(())
    }
}

impl<UserData> Instance for EntrypointInstance<UserData> {
    type Runtime = Wasmer;
    type UserData = UserData;
    type UserDataReference<'a>
        = MutexGuard<'a, UserData>
    where
        Self::UserData: 'a,
        Self: 'a;
    type UserDataMutReference<'a>
        = MutexGuard<'a, UserData>
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
    type UserDataReference<'a>
        = MutexGuard<'a, UserData>
    where
        Self::UserData: 'a,
        Self: 'a;
    type UserDataMutReference<'a>
        = MutexGuard<'a, UserData>
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
    fn load_export(&self, name: &str) -> Option<Extern> {
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
