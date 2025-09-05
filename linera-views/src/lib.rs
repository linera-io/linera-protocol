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
* `MemoryDatabase` is using the memory
* `RocksDbDatabase` is a disk-based key-value store
* `DynamoDbDatabase` is the AWS-based DynamoDB service.
* `ScyllaDbDatabase` is a cloud-based Cassandra-compatible database.
* `StorageServiceDatabase` is a gRPC-based storage that uses either memory or RocksDB. It is available in `linera-storage-service`.

The corresponding trait in the code is the [`crate::store::KeyValueDatabase`](https://docs.rs/linera-views/latest/linera_views/store/trait.KeyValueDatabase.html).
as well as [`crate::store::KeyValueStore`](https://docs.rs/linera-views/latest/linera_views/store/trait.KeyValueStore.html).

The latter trait decomposes into a [`store::ReadableKeyValueStore`](https://docs.rs/linera-views/latest/linera_views/store/trait.ReadableKeyValueStore.html)
and a [`store::WritableKeyValueStore`](https://docs.rs/linera-views/latest/linera_views/store/trait.WritableKeyValueStore.html).

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
// These traits have `Send` variants where possible.
#![allow(async_fn_in_trait)]

/// The definition of the batches for writing in the database.
pub mod batch;

/// The `KeyValueDatabase` and `KeyValueStore` traits and related definitions.
pub mod store;

/// The `Context` trait and related definitions.
pub mod context;

/// Common definitions used for views and backends.
pub mod common;

mod error;
pub use error::ViewError;

mod future_sync_ext;
#[doc(hidden)]
pub use future_sync_ext::FutureSyncExt;

/// Elementary data-structures implementing the [`views::View`] trait.
pub mod views;

/// Backend implementing the [`crate::store::KeyValueStore`] trait.
pub mod backends;

/// Support for metrics.
#[cfg(with_metrics)]
pub mod metrics;

/// GraphQL implementations.
#[cfg(with_graphql)]
mod graphql;

/// Functions for random generation
#[cfg(with_testing)]
pub mod random;

/// Helper types for tests.
#[cfg(with_testing)]
pub mod test_utils;

#[cfg(with_dynamodb)]
pub use backends::dynamo_db;
#[cfg(with_indexeddb)]
pub use backends::indexed_db;
#[cfg(with_metrics)]
pub use backends::metering;
#[cfg(with_rocksdb)]
pub use backends::rocks_db;
#[cfg(with_scylladb)]
pub use backends::scylla_db;
pub use backends::{journaling, lru_caching, memory, value_splitting};
pub use views::{
    bucket_queue_view, collection_view, hashable_wrapper, key_value_store_view, log_view, map_view,
    queue_view, reentrant_collection_view, register_view, set_view,
};
/// Re-exports used by the derive macros of this library.
#[doc(hidden)]
pub use {generic_array, sha3};
