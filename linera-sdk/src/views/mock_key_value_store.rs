// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A mock system API for interfacing with the key-value store.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Mutex,
    },
};

use futures::FutureExt;
use linera_views::{
    batch::Batch,
    common::{ReadableKeyValueStore, WritableKeyValueStore},
    memory::{create_memory_store, MemoryStore},
};

/// A mock [`KeyValueStore`] implementation using a [`MemoryStore`].
pub(super) struct MockKeyValueStore {
    store: MemoryStore,
    contains_key_promises: PromiseRegistry<bool>,
    read_multi_promises: PromiseRegistry<Vec<Option<Vec<u8>>>>,
    read_single_promises: PromiseRegistry<Option<Vec<u8>>>,
    find_keys_promises: PromiseRegistry<Vec<Vec<u8>>>,
    find_key_values_promises: PromiseRegistry<Vec<(Vec<u8>, Vec<u8>)>>,
}

impl Default for MockKeyValueStore {
    fn default() -> Self {
        MockKeyValueStore {
            store: create_memory_store(),
            contains_key_promises: PromiseRegistry::default(),
            read_multi_promises: PromiseRegistry::default(),
            read_single_promises: PromiseRegistry::default(),
            find_keys_promises: PromiseRegistry::default(),
            find_key_values_promises: PromiseRegistry::default(),
        }
    }
}

/// Helper type to keep track of created promises by one of the functions.
#[derive(Default)]
struct PromiseRegistry<T> {
    promises: Mutex<BTreeMap<u32, T>>,
    id_counter: AtomicU32,
}

impl<T> PromiseRegistry<T> {
    /// Creates a new promise tracking the internal `value`.
    pub fn register(&self, value: T) -> u32 {
        let id = self.id_counter.fetch_add(1, Ordering::AcqRel);
        self.promises
            .try_lock()
            .expect("Unit-tests should run in a single-thread")
            .insert(id, value);
        id
    }

    /// Retrieves a tracked promise by its ID.
    pub fn take(&self, id: u32) -> T {
        self.promises
            .try_lock()
            .expect("Unit-tests should run in a single-thread")
            .remove(&id)
            .expect("Use of an invalid promise ID")
    }
}

impl MockKeyValueStore {
    /// Checks if `key` is present in the storage, returning a promise to retrieve the final
    /// value.
    pub(crate) fn contains_key_new(&self, key: &[u8]) -> u32 {
        self.contains_key_promises.register(
            self.store
                .contains_key(key)
                .now_or_never()
                .expect("Memory store should never wait for anything")
                .expect("Memory store should never fail"),
        )
    }

    /// Returns if the key used in the respective call to [`contains_key_new`] is present.
    pub(crate) fn contains_key_wait(&self, promise: u32) -> bool {
        self.contains_key_promises.take(promise)
    }

    /// Reads the values addressed by `keys` from the store, returning a promise to retrieve
    /// the final value.
    pub(crate) fn read_multi_values_bytes_new(&self, keys: &[Vec<u8>]) -> u32 {
        self.read_multi_promises.register(
            self.store
                .read_multi_values_bytes(keys.to_vec())
                .now_or_never()
                .expect("Memory store should never wait for anything")
                .expect("Memory store should never fail"),
        )
    }

    /// Returns the values read from storage by the respective
    /// [`read_multi_values_bytes_new`] call.
    pub(crate) fn read_multi_values_bytes_wait(&self, promise: u32) -> Vec<Option<Vec<u8>>> {
        self.read_multi_promises.take(promise)
    }

    /// Reads a value addressed by `key` from the storage, returning a promise to retrieve the
    /// final value.
    pub(crate) fn read_value_bytes_new(&self, key: &[u8]) -> u32 {
        self.read_single_promises.register(
            self.store
                .read_value_bytes(key)
                .now_or_never()
                .expect("Memory store should never wait for anything")
                .expect("Memory store should never fail"),
        )
    }

    /// Returns the value read from storage by the respective [`read_value_bytes_new`] call.
    pub(crate) fn read_value_bytes_wait(&self, promise: u32) -> Option<Vec<u8>> {
        self.read_single_promises.take(promise)
    }

    /// Finds keys in the storage that start with `key_prefix`, returning a promise to
    /// retrieve the final value.
    pub(crate) fn find_keys_new(&self, key_prefix: &[u8]) -> u32 {
        self.find_keys_promises.register(
            self.store
                .find_keys_by_prefix(key_prefix)
                .now_or_never()
                .expect("Memory store should never wait for anything")
                .expect("Memory store should never fail"),
        )
    }

    /// Returns the keys found in storage by the respective [`find_keys_new`] call.
    pub(crate) fn find_keys_wait(&self, promise: u32) -> Vec<Vec<u8>> {
        self.find_keys_promises.take(promise)
    }

    /// Finds key-value pairs in the storage in which the key starts with `key_prefix`,
    /// returning a promise to retrieve the final value.
    pub(crate) fn find_key_values_new(&self, key_prefix: &[u8]) -> u32 {
        self.find_key_values_promises.register(
            self.store
                .find_key_values_by_prefix(key_prefix)
                .now_or_never()
                .expect("Memory store should never wait for anything")
                .expect("Memory store should never fail"),
        )
    }

    /// Returns the key-value pairs found in storage by the respective [`find_key_values_new`]
    /// call.
    pub(crate) fn find_key_values_wait(&self, promise: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.find_key_values_promises.take(promise)
    }

    /// Writes a `batch` of operations to storage.
    pub(crate) fn write_batch(&self, batch: Batch) {
        self.store
            .write_batch(batch, &[])
            .now_or_never()
            .expect("Memory store should never wait for anything")
            .expect("Memory store should never fail");
    }
}
