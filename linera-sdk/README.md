<!-- cargo-rdme start -->

This module provides an SDK for developing Linera applications using Rust.

A Linera application consists of two WebAssembly binaries: a contract and a service.
Both binaries have access to the same application and chain specific storage. The service only
has read-only access, while the contract can write to it. The storage should be used to store
the application state, which is persisted across blocks. The state can be a custom type that
uses [`linera-views`](https://docs.rs/linera-views/latest/linera_views/index.html), a framework
that allows lazily loading selected parts of the state. This is useful if the application's
state is large and doesn't need to be loaded in its entirety for every execution.

The contract binary should create a type to implement the [`Contract`](https://docs.rs/linera-sdk/latest/linera_sdk/trait.Contract.html) trait.
The type can store the [`ContractRuntime`](contract::ContractRuntime) and the state, and must
have its implementation exported by using the `contract!` macro.

The service binary should create a type to implement the [`Service`](https://docs.rs/linera-sdk/latest/linera_sdk/trait.Service.html) trait.
The type can store the [`ServiceRuntime`](service::ServiceRuntime) and the state, and must have
its implementation exported by using the `service!` macro.

# Examples

The [`examples`](https://github.com/linera-io/linera-protocol/tree/main/examples)
directory contains some example applications.

<!-- cargo-rdme end -->

## Contributing

See the [CONTRIBUTING](../CONTRIBUTING.md) file for how to help out.

## License

This project is available under the terms of the [Apache 2.0 license](../LICENSE).
