<!-- cargo-rdme start -->

This module provides an SDK for developing Linera applications using Rust.

A Linera application consists of two WebAssembly binaries: a contract and a service.
In both binaries, there should be a shared application state. The state is a type that
represents what the application would like to persist in storage across blocks, and
must implement the [`Contract`](https://docs.rs/linera-sdk/latest/linera_sdk/trait.Contract.html) trait in the contract binary and the
[`Service`](https://docs.rs/linera-sdk/latest/linera_sdk/trait.Service.html) trait in the service binary.

The application can select between two storage backends to use. Selecting the storage
backend is done by specifying both the `Contract::Storage`
and the `Service::Storage` associated types.

The [`SimpleStateStorage`](https://docs.rs/linera-sdk/latest/linera_sdk/struct.SimpleStateStorage.html) backend stores the application's
state type by serializing it into binary blob. This allows the entire contents of the
state to be persisted and made available to the application when it is executed.

The [`ViewStateStorage`](https://docs.rs/linera-sdk/latest/linera_sdk/struct.ViewStateStorage.html) backend stores the application's
state using the
[`linera-views`](https://docs.rs/linera-views/latest/linera_views/index.html), a
framework that allows loading selected parts of the state. This is useful if the
application's state is large and doesn't need to be loaded in its entirety for every
execution.

The contract binary should use the `contract!` macro to export the application's contract
endpoints implemented via the [`Contract`](https://docs.rs/linera-sdk/latest/linera_sdk/trait.Contract.html) trait implementation.

The service binary should use the `service!` macro to export the application's service
endpoints implemented via the [`Service`](https://docs.rs/linera-sdk/latest/linera_sdk/trait.Service.html) trait implementation.

# Examples

The [`linera-examples`](https://github.com/linera-io/linera-protocol/tree/main/linera-examples)
directory contains some example applications.

<!-- cargo-rdme end -->

## Contributing

See the [CONTRIBUTING](../CONTRIBUTING.md) file for how to help out.

## License

This project is available under the terms of either the [Apache 2.0 license](../LICENSE).
