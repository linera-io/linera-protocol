<!-- cargo-rdme start -->

This module provides an SDK for developing Linera applications using Rust.

A Linera application consists of two WebAssembly binaries: a contract and a service.
In both binaries, there should be a shared application state. The state is a type that
represents what the application would like to persist in storage across blocks, and
must implement [`State`](https://docs.rs/linera-sdk/latest/linera_sdk/trait.State.html) trait in order to specify how the state should be loaded
from and stored to the persistent key-value storage. An alternative is to use the
[`linera-views`](https://docs.rs/linera-views/latest/linera_views/index.html), a framework that
allows loading selected parts of the state. This is useful if the application's state is large
and doesn't need to be loaded in its entirety for every execution. By deriving
[`RootView`](views::RootView) on the state type it automatically implements the [`State`]
trait.

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
