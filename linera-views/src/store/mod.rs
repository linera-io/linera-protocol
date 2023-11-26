// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Helper definitions for in-memory storage.
pub mod memory;

/// A storage backend for views based on RocksDB
#[cfg(feature = "rocksdb")]
#[cfg(not(target_arch = "wasm32"))]
pub mod rocks_db;

/// A storage backend for views based on DynamoDB
#[cfg(feature = "aws")]
#[cfg(not(target_arch = "wasm32"))]
pub mod dynamo_db;

/// A storage backend for views based on ScyllaDB
#[cfg(feature = "scylladb")]
#[cfg(not(target_arch = "wasm32"))]
pub mod scylla_db;

