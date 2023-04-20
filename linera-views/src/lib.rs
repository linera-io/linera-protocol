// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module aims to help the mapping of complex data-structures onto a key-value
//! store. The central notion is a [`views::View`] which can be loaded from storage, modified in
//! memory, then committed (i.e. the changes are atomically persisted in storage).

#![deny(missing_docs)]

/// The definition of the batches for writing in the datatbase.
pub mod batch;

/// The definitions used for the memory/rocksdb/dynamo_db.
pub mod common;

/// The definition of the `View` and related traits.
pub mod views;

/// The `RegisterView` allows to implement a register for a single value.
pub mod register_view;

/// The `LogView` implements a log list that can be pushed.
pub mod log_view;

/// The `QueueView` implements a queue that can pushed_backed and deleted on the front.
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
pub mod lru_caching;

/// The `ReentrantCollectionView` implements a map structure whose keys are ordered and the values are views with concurrent access.
#[cfg(not(target_arch = "wasm32"))]
pub mod reentrant_collection_view;

/// The implementation of a key value store view.
#[cfg(not(target_arch = "wasm32"))]
pub mod key_value_store_view;

/// Wrapping a view to compute a hash.
#[cfg(not(target_arch = "wasm32"))]
pub mod hashable_wrapper;

/// Helper definitions for Rocksdb storage.
#[cfg(not(target_arch = "wasm32"))]
pub mod rocksdb;

/// Helper definitions for DynamoDB storage.
#[cfg(feature = "aws")]
pub mod dynamo_db;

/// Helper types for interfacing with a LocalStack instance.
#[cfg(feature = "aws")]
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
