// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module aims to help the mapping of complex data-structures onto a key-value
//! store. The central notion is a [`views::View`] which can be loaded from storage, modified in
//! memory, then committed (i.e. the changes are atomically persisted in storage).

#![deny(missing_docs)]

/// The definitions used for the memory/rocksdb/dynamo_db
pub mod common;

/// The main definitions.
pub mod views;

/// The register_view
pub mod register_view;

/// The log_view
pub mod log_view;

/// The queue_view
pub mod queue_view;

/// The map_view
pub mod map_view;

/// The map_view
pub mod set_view;

/// The collection_view
pub mod collection_view;

/// The reentrant_collection_view
#[cfg(not(target_arch = "wasm32"))]
pub mod reentrant_collection_view;

/// The key value store view
#[cfg(not(target_arch = "wasm32"))]
pub mod key_value_store_view;

/// Helper definitions for in-memory storage.
#[cfg(not(target_arch = "wasm32"))]
pub mod memory;

/// Helper definitions for Rocksdb storage.
#[cfg(not(target_arch = "wasm32"))]
pub mod rocksdb;

/// Helper definitions for DynamoDB storage.
#[cfg(not(target_arch = "wasm32"))]
pub mod dynamo_db;

/// Helper types for interfacing with a LocalStack instance.
#[cfg(not(target_arch = "wasm32"))]
pub mod localstack;

/// Helper types for tests.
#[cfg(any(test, feature = "test"))]
#[cfg(not(target_arch = "wasm32"))]
pub mod test_utils;

/// For macros.
#[doc(hidden)]
pub use {async_trait::async_trait, generic_array, serde, sha2};

#[doc(hidden)]
#[cfg(not(target_arch = "wasm32"))]
pub use linera_base::crypto;
