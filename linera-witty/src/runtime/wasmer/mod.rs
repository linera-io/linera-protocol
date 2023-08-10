// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support for the [Wasmer](https://wasmer.io) runtime.

use super::traits::{Instance, Runtime};
use std::sync::{Arc, Mutex};
use wasmer::{AsStoreMut, AsStoreRef, Extern, Memory, Store, StoreMut, StoreRef};
use wasmer_vm::StoreObjects;

/// Representation of the [Wasmer](https://wasmer.io) runtime.
pub struct Wasmer;

impl Runtime for Wasmer {
    type Export = Extern;
    type Memory = Memory;
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
