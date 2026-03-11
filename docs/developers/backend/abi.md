# Defining the ABI

The Application Binary Interface (ABI) of a Linera application defines how to
interact with this application from other parts of the system. It includes the
data structures, data types, and functions exposed by on-chain contracts and
services.

ABIs are usually defined in `src/lib.rs` and compiled across all architectures
(Wasm and native).

For a reference guide, check out the
[documentation of the crate](https://docs.rs/linera-base/latest/linera_base/abi/).

## Defining a marker struct

The library part of your application (generally in `src/lib.rs`) must define a
public empty struct that implements the `Abi` trait.

```rust
struct CounterAbi;
```

The `Abi` trait combines the `ContractAbi` and `ServiceAbi` traits to include
the types that your application exports.

```rust,ignore
{{#include ../../../linera-base/src/abi.rs:abi}}
```

Next, we're going to implement each of the two traits.

## Contract ABI

The `ContractAbi` trait defines the data types that your application uses in a
contract. Each type represents a specific part of the contract's behavior:

```rust,ignore
{{#include ../../../linera-base/src/abi.rs:contract_abi}}
```

All these types must implement the `Serialize`, `DeserializeOwned`, `Send`,
`Sync`, `Debug` traits, and have a `'static` lifetime.

In our example, we would like to change our `Operation` to `u64`, like so:

```rust,ignore
{{#include ../../../examples/counter/src/lib.rs:contract_abi}}
```

## Service ABI

The `ServiceAbi` is in principle very similar to the `ContractAbi`, just for the
service component of your application.

The `ServiceAbi` trait defines the types used by the service part of your
application:

```rust,ignore
{{#include ../../../linera-base/src/abi.rs:service_abi}}
```

For our `Counter` example, we'll be using GraphQL to query our application so
our `ServiceAbi` should reflect that:

```rust,ignore
use async_graphql::{Request, Response};

{{#include ../../../examples/counter/src/lib.rs:service_abi}}
```

## References

- The full trait definition of `Abi` can be found
  [here](https://github.com/linera-io/linera-protocol/blob/{{#include
  ../../RELEASE_HASH}}/linera-base/src/abi.rs).

- The full `Counter` example application can be found
  [here](https://github.com/linera-io/linera-protocol/blob/{{#include
  ../../RELEASE_HASH}}/examples/counter).
