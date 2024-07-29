// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
This module is used in the Linera protocol to map complex data structures onto a
key-value store. The central notion is a [`views::View`](https://docs.rs/linera-views/latest/linera_views/views/trait.View.html)
which can be loaded from storage, modified in memory, and then committed (i.e. the changes are atomically persisted in storage).

The package provides essentially two functionalities:
* An abstraction to access databases.
* Several containers named views for storing data modeled on classical ones.

See `DESIGN.md` for more details.

## The supported databases.

The databases supported are of the NoSQL variety and they are key-value stores.

We provide support for the following databases:
* `MemoryStore` is using the memory
* `RocksDbStore` is a disk-based key-value store
* `DynamoDbStore` is the AWS-based DynamoDB service.
* `ScyllaDbStore` is a cloud-based Cassandra-compatible database.
* `ServiceStoreClient` is a gRPC-based storage that uses either memory or RocksDB. It is available in `linera-storage-service`.

The corresponding trait in the code is the [`common::KeyValueStore`](https://docs.rs/linera-views/latest/linera_views/common/trait.KeyValueStore.html).
The trait decomposes into a [`common::ReadableKeyValueStore`](https://docs.rs/linera-views/latest/linera_views/common/trait.ReadableKeyValueStore.html)
and a [`common::WritableKeyValueStore`](https://docs.rs/linera-views/latest/linera_views/common/trait.WritableKeyValueStore.html).
In addition, there is a [`common::AdminKeyValueStore`](https://docs.rs/linera-views/latest/linera_views/common/trait.AdminKeyValueStore.html)
which gives some functionalities for working with stores.
A context is the combination of a client and a base key (of type `Vec<u8>`).

## Views.

A view is a container whose data lies in one of the above-mentioned databases.
When the container is modified the modification lies first in the view before
being committed to the database. In technical terms, a view implements the trait `View`.

The specific functionalities of the trait `View` are the following:
* `context` for obtaining a reference to the storage context of the view.
* `load` for loading the view from a specific context.
* `rollback` for canceling all modifications that were not committed thus far.
* `clear` for clearing the view, in other words for reverting it to its default state.
* `flush` for persisting the changes to storage.

The following views implement the `View` trait:
* `RegisterView` implements the storing of a single data.
* `LogView` implements a log, which is a list of entries that can be expanded.
* `QueueView` implements a queue, which is a list of entries that can be expanded and reduced.
* `MapView` implements a map with keys and values.
* `SetView` implements a set with keys.
* `CollectionView` implements a map whose values are views themselves.
* `ReentrantCollectionView` implements a map for which different keys can be accessed independently.
* `ViewContainer<C>` implements a `KeyValueStore` and is used internally.

The `LogView` can be seen as an analog of `VecDeque` while `MapView` is an analog of `BTreeMap`.
*/

#![deny(missing_docs)]
#![deny(clippy::large_futures)]

#[cfg(with_metrics)]
use std::sync::LazyLock;

#[cfg(with_metrics)]
pub use linera_base::prometheus_util;
#[cfg(with_metrics)]
use prometheus::IntCounterVec;

/// The definition of the batches for writing in the database.
pub mod batch;

/// The definitions used for the `KeyValueStore` and `Context`.
pub mod common;

/// The code to turn a `DirectKeyValueStore` into a `KeyValueStore` by adding journaling.
pub mod journaling;

/// The code for encapsulating one key_value store into another that does metric
#[cfg(with_metrics)]
pub mod metering;

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

mod graphql;

/// The `CollectionView` implements a map structure whose keys are ordered and the values are views.
pub mod collection_view;

/// Helper definitions for in-memory storage.
pub mod memory;

/// The LRU (least recently used) caching.
pub mod lru_caching;

/// The `ReentrantCollectionView` implements a map structure whose keys are ordered and the values are views with concurrent access.
pub mod reentrant_collection_view;

/// The implementation of a key-value store view.
pub mod key_value_store_view;

/// Wrapping a view to compute a hash.
pub mod hashable_wrapper;

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

/// Helper types for tests.
#[cfg(with_testing)]
pub mod test_utils;

/// Re-exports used by the derive macros of this library.
#[doc(hidden)]
pub use {
    async_lock, async_trait::async_trait, futures, generic_array, linera_base::crypto, serde, sha3,
};

/// Increments the metrics counter with the given name, with the struct and base key as labels.
#[cfg(with_metrics)]
pub fn increment_counter(counter: &LazyLock<IntCounterVec>, struct_name: &str, base_key: &[u8]) {
    let base_key = hex::encode(base_key);
    let labels = [struct_name, &base_key];
    counter.with_label_values(&labels).inc();
}

/// The metric tracking the latency of the loading of views.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static LOAD_VIEW_LATENCY: LazyLock<prometheus::HistogramVec> = LazyLock::new(|| {
    use prometheus::register_histogram_vec;
    register_histogram_vec!("load_view_latency", "Load view latency", &[])
        .expect("Load view latency should not fail")
});

/// The metric counting how often a view is read from storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static LOAD_VIEW_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus_util::register_int_counter_vec(
        "load_view",
        "The metric counting how often a view is read from storage",
        &["type", "base_key"],
    )
    .expect("Counter creation should not fail")
});
/// The metric counting how often a view is written from storage.
#[cfg(with_metrics)]
#[doc(hidden)]
pub static SAVE_VIEW_COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus_util::register_int_counter_vec(
        "save_view",
        "The metric counting how often a view is written from storage",
        &["type", "base_key"],
    )
    .expect("Counter creation should not fail")
});
