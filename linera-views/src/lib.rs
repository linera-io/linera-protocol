// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module is used in the Linera protocol to map complex data structures onto a
//! key-value store. The central notion is a [`views::View`](crate::views::View) which can
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
//! * `MemoryClient` is using the memory
//! * `RocksDbClient` is a disk-based key-value store
//! * `DynamoDbClient` is the AWS-based DynamoDB service.
//!
//! The corresponding type in the code is the `KeyValueStoreClient`.
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
//!
//! The following views implement the `View` trait:
//! * `RegisterView` implements the storing of a single data.
//! * `LogView` implements a log, which is a list of entries that can be expanded.
//! * `QueueView` implements a queue, which is a list of entries that can be expanded and reduced.
//! * `MapView` implements a map with keys and values.
//! * `SetView` implements a set with keys.
//! * `CollectionView` implements a map whose values are views themselves.
//! * `ReentrantCollectionView` implements a map for which different keys can be accessed independently.
//! * `ViewContainer<C>` implements a `KeyValueStoreClient` and is used internally.
//!
//! The `LogView` can be seen as an analog of `VecDeque` while `MapView` is an analog of `BTreeMap`.

#![deny(missing_docs)]

/// The definition of the batches for writing in the database.
pub mod batch;

/// The definitions used for the `KeyValueStoreClient` and `Context`.
pub mod common;

/// The code for handling big values by splitting them into several small ones.
pub mod value_splitting;

/// The definition of the `View` and related traits.
pub mod views;

/// The `RegisterView` implements a register for a single value.
pub mod register_view;

/// The `LogView` implements a log list that can be pushed.
pub mod log_view;

/// The `QueueView` implements a queue that can push on the back and delete on the front.
pub mod queue_view;

/// The `MapView` implements a map with ordered keys.
pub mod map_view;

/// The `SetView` implements a set with ordered entries.
pub mod set_view;

/// The `CollectionView` implements a map structure whose keys are ordered and the values are views.
pub mod collection_view;

/// Helper definitions for in-memory storage.
pub mod memory;

/// The LRU (least recently used) caching.
#[cfg(not(target_arch = "wasm32"))]
pub mod lru_caching;

/// The `ReentrantCollectionView` implements a map structure whose keys are ordered and the values are views with concurrent access.
#[cfg(not(target_arch = "wasm32"))]
pub mod reentrant_collection_view;

/// The implementation of a key-value store view.
#[cfg(not(target_arch = "wasm32"))]
pub mod key_value_store_view;

/// Wrapping a view to compute a hash.
#[cfg(not(target_arch = "wasm32"))]
pub mod hashable_wrapper;

/// A storage backend for views based on ScyllaDB
#[cfg(feature = "scylladb")]
#[cfg(not(target_arch = "wasm32"))]
pub mod scylla_db;

/// A storage backend for views based on RocksDB
#[cfg(feature = "rocksdb")]
#[cfg(not(target_arch = "wasm32"))]
pub mod rocks_db;

/// A storage backend for views based on DynamoDB
#[cfg(feature = "aws")]
#[cfg(not(target_arch = "wasm32"))]
pub mod dynamo_db;

/// Helper types for interfacing with a LocalStack instance.
#[cfg(feature = "aws")]
#[cfg(not(target_arch = "wasm32"))]
pub mod localstack;

/// Helper types for tests.
#[cfg(any(test, feature = "test"))]
#[cfg(not(target_arch = "wasm32"))]
pub mod test_utils;

/// Macros used for the library.
#[doc(hidden)]
pub use {async_trait::async_trait, futures, generic_array, linera_base::crypto, serde, sha3};

/// Does nothing. Use the metrics feature to enable.
#[cfg(not(feature = "metrics"))]
pub fn increment_counter(_name: &str, _struct_name: &str, _base_key: &[u8]) {}

/// Increments the metrics counter with the given name, with the struct and base key as labels.
#[cfg(feature = "metrics")]
pub fn increment_counter(name: &'static str, struct_name: &str, base_key: &[u8]) {
    let base_key = hex::encode(base_key);
    let labels = [("type", struct_name.into()), ("base_key", base_key)];
    metrics::increment_counter!(name, &labels,);
}

/// The metric counting how often a view is read from storage.
pub const LOAD_VIEW_COUNTER: &str = "load_view";
/// The metric counting how often a view is written from storage.
pub const SAVE_VIEW_COUNTER: &str = "save_view";

#[cfg(all(feature = "aws", target_arch = "wasm32"))]
compile_error!("Cannot build AWS features for the WASM target");
