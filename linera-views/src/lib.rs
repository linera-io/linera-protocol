// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module is used in the Linera protocol to map complex data structures onto a
//! key-value store. The central notion is a [`view::View`](crate::view::View) which can
//! be loaded from storage, modified in memory, then committed (i.e. the changes are
//! atomically persisted in storage).
//!
//! The package provides essentially two functionalities:
//! * An abstraction to access databases.
//! * Several containers named views for storing data modeled on classical ones.
//!
//! See `DESIGN.md` for more details.
//!
//! ## The supported databases.
//!
//! The databases supported are of the NoSQL variety and they are key-value stores.
//!
//! We provide support for the following databases:
//! * `MemoryStore` is using the memory
//! * `RocksDbStore` is a disk-based key-value store
//! * `DynamoDbStore` is the AWS-based DynamoDB service.
//! * `ScyllaDbStore` is a cloud based Cassandra compatible database.
//!
//! The corresponding type in the code is the `KeyValueStore`.
//! A context is the combination of a client and a path (named `base_key` which is
//! of type `Vec<u8>`).
//!
//! ## Views.
//!
//! A view is a container whose data lies in one of the above-mentioned databases.
//! When the container is modified the modification lies first in the view before
//! being committed to the database. In technical terms, a view implements the trait `View`.
//!
//! The specific functionalities of the trait `View` are the following:
//! * `load` for loading the view from a specific context.
//! * `rollback` for canceling all modifications that were not committed thus far.
//! * `clear` for clearing the view, in other words for reverting it to its default state.
//! * `flush` for persisting the changes to storage.
//! * `delete` for deleting the changes from the database.

#![deny(missing_docs)]

/// The definition of the batches for writing in the database.
pub mod batch;

/// The definitions used for the `KeyValueStore` and `Context`.
pub mod common;

/// The code for handling big values by splitting them into several small ones.
pub mod value_splitting;

/// The definition of the `View` and related traits.
pub mod view;

/// The implementations of `View` that we provide.
pub mod views;

/// The LRU (least recently used) caching.
#[cfg(not(target_arch = "wasm32"))]
pub mod lru_caching;

/// Wrapping a view to compute a hash.
#[cfg(not(target_arch = "wasm32"))]
pub mod hashable_wrapper;

/// The implemented containers
pub mod store;

/// Helper types for tests.
#[cfg(any(test, feature = "test"))]
#[cfg(not(target_arch = "wasm32"))]
pub mod test_utils;

/// Re-exports used by the derive macros of this library.
#[doc(hidden)]
pub use {
    async_lock,
    async_trait::async_trait,
    futures, generic_array,
    linera_base::crypto,
    once_cell::sync::Lazy,
    prometheus::{register_int_counter_vec, IntCounterVec},
    serde, sha3,
};

/// Does nothing. Use the metrics feature to enable.
#[cfg(not(feature = "metrics"))]
pub fn increment_counter(_counter: &Lazy<IntCounterVec>, _struct_name: &str, _base_key: &[u8]) {}

/// Increments the metrics counter with the given name, with the struct and base key as labels.
#[cfg(feature = "metrics")]
pub fn increment_counter(counter: &Lazy<IntCounterVec>, struct_name: &str, base_key: &[u8]) {
    let base_key = hex::encode(base_key);
    let labels = [struct_name, &base_key];
    counter.with_label_values(&labels).inc();
}

/// The metric counting how often a view is read from storage.
pub static LOAD_VIEW_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "load_view",
        "The metric counting how often a view is read from storage",
        &["type", "base_key"]
    )
    .expect("Counter creation should not fail")
});
/// The metric counting how often a view is written from storage.
pub static SAVE_VIEW_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "save_view",
        "The metric counting how often a view is written from storage",
        &["type", "base_key"]
    )
    .expect("Counter creation should not fail")
});

#[cfg(all(feature = "aws", target_arch = "wasm32"))]
compile_error!("Cannot build AWS features for the Wasm target");

#[cfg(all(feature = "rocksdb", target_arch = "wasm32"))]
compile_error!("Cannot build RocksDB features for the Wasm target");

#[cfg(all(feature = "scylladb", target_arch = "wasm32"))]
compile_error!("Cannot build ScyllaDB features for the Wasm target");
