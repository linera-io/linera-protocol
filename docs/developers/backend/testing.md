# Writing Tests

Linera applications can be tested using normal Rust unit tests or integration
tests. Unit tests use a mock runtime for execution, so it's useful for testing
the application as if it were running by itself on a single chain. Integration
tests use a simulated validator for testing. This allows creating chains and
adding blocks to them in order to test interactions between multiple microchains
and multiple applications.

Applications should consider having both types of tests. Unit tests should be
used to focus on the application's internals and core functionality. Integration
tests should be used to test how the application behaves on a more complex
environment that's closer to the real network. Both types of test are running in
native Rust.

> For Rust tests, the `cargo test` command can be used to run both the unit and
> integration tests.

## Unit tests

Unit tests are written beside the application's source code (i.e., inside the
`src` directory of the project). The main purpose of a unit test is to test
parts of the application in an isolated environment. Anything that's external is
usually mocked. When the `linera-sdk` is compiled with the `test` feature
enabled, the `ContractRuntime` and `SystemRuntime` types are actually mock
runtimes, and can be configured to return specific values for different tests.

### Example

A simple unit test is shown below, which tests if the method `execute_operation`
method changes the application state of the `Counter` application.

```rust,ignore
{{#include ../../../examples/counter/src/contract.rs:counter_test}}
```

## Integration tests

Integration tests are usually written separately from the application's source
code (i.e., inside a `tests` directory that's beside the `src` directory).

Integration tests use the helper types from `linera_sdk::test` to set up a
simulated Linera network, and publish blocks to microchains in order to execute
the application.

### Example

A simple integration test that execution a block containing an operation for the
`Counter` application is shown below.

```rust,ignore
{{#include ../../../examples/counter/tests/single_chain.rs:counter_integration_test}}
```
