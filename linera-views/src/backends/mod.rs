// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// The code to turn a `DirectKeyValueStore` into a `KeyValueStore` by adding journaling.
pub mod journaling;

/// The code for encapsulating one key_value store into another that does metric
#[cfg(with_metrics)]
pub mod metering;

/// The code for handling big values by splitting them into several small ones.
pub mod value_splitting;

/// Helper definitions for in-memory storage.
pub mod memory;

/// The LRU (least recently used) caching.
pub mod lru_caching;

/// A storage backend for views based on ScyllaDB
#[cfg(with_scylladb)]
pub mod scylla_db;

/// A storage backend for views based on RocksDB
#[cfg(with_rocksdb)]
pub mod rocks_db;

/// A storage backend for views based on DynamoDB
#[cfg(with_dynamodb)]
pub mod dynamo_db;

/// A storage backend for views in the browser based on IndexedDB
#[cfg(with_indexeddb)]
pub mod indexed_db;
