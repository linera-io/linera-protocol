<!-- cargo-rdme start -->

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

The corresponding trait in the code is the [`crate::common::KeyValueStore`](https://docs.rs/linera-views/latest/linera_views/common/trait.KeyValueStore.html).
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

<!-- cargo-rdme end -->

## Contributing

See the [CONTRIBUTING](../CONTRIBUTING.md) file for how to help out.

## License

This project is available under the terms of the [Apache 2.0 license](../LICENSE).
