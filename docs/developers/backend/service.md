# Writing the Service Binary

The service binary is the second component of a Linera application. It is
compiled into a separate Bytecode from the contract and is run independently. It
is not metered (meaning that querying an application's service does not consume
gas), and can be thought of as a read-only view into your application.

Application states can be arbitrarily complex, and most of the time you don't
want to expose this state in its entirety to those who would like to interact
with your app. Instead, you might prefer to define a distinct set of queries
that can be made against your application.

The `Service` trait is how you define the interface into your application. The
`Service` trait is defined as follows:

```rust,ignore
{{#include ../../../linera-sdk/src/lib.rs:service}}
```

Let's implement `Service` for our counter application.

First, we create a new type for the service, similarly to the contract:

```rust,ignore
{{#include ../../../examples/counter/src/service.rs:service_struct}}
```

Just like with the `CounterContract` type, this type usually has two types: the
application `state` and the `runtime`. We can omit the fields if we don't use
them, so in this example we're omitting the `runtime` field, since its only used
when constructing the `CounterService` type.

As before, the macro `service!` generates the necessary boilerplate for
implementing the service
[WIT interface](https://component-model.bytecodealliance.org/design/wit.html),
exporting the necessary resource types and functions so that the service can be
executed.

Next, we need to implement the `Service` trait for `CounterService` type. The
first step is to define the `Service`'s associated type, which is the global
parameters specified when the application is instantiated. In our case, the
global parameters aren't used, so we can just specify the unit type:

```rust,ignore
impl Service for CounterService {
    type Parameters = ();

    // ...
}
```

Also like in contracts, we must implement a `load` constructor when implementing
the `Service` trait. The constructor receives the runtime handle and should use
it to load the application state:

```rust,ignore
{{#include ../../../examples/counter/src/service.rs:new}}
```

Services don't have a `store` method because they are read-only and can't
persist any changes back to the storage.

The actual functionality of the service starts in the `handle_query` method. We
will accept GraphQL queries and handle them using the
[`async-graphql` crate](https://github.com/async-graphql/async-graphql). To
forward the queries to custom GraphQL handlers we will implement in the next
section, we use the following code:

```rust,ignore
{{#include ../../../examples/counter/src/service.rs:handle_query}}
```

Finally, as before, the following code is needed to incorporate the ABI
definitions into your `Service` implementation:

```rust,ignore
{{#include ../../../examples/counter/src/service.rs:declare_abi}}
```

## Adding GraphQL compatibility

Finally, we want our application to have GraphQL compatibility. To achieve this
we need a `QueryRoot` to respond to queries and a `MutationRoot` for creating
serialized `Operation` values that can be placed in blocks.

In the `QueryRoot`, we only create a single `value` query that returns the
counter's value:

```rust,ignore
{{#include ../../../examples/counter/src/service.rs:query}}
```

In the `MutationRoot`, we only create one `increment` method that returns a
serialized operation to increment the counter by the provided `value`:

```rust,ignore
{{#include ../../../examples/counter/src/service.rs:mutation}}
```

We haven't included the imports in the above code. If you want the full source
code and associated tests check out the [examples
section](https://github.com/linera-io/linera-protocol/blob/{{#include
../../RELEASE_HASH}}/examples/counter/src/service.rs) on GitHub.

## References

- The full trait definition of `Service` can be found
  [here](https://github.com/linera-io/linera-protocol/blob/{{#include
  ../../RELEASE_HASH}}/linera-sdk/src/lib.rs).

- The full `Counter` example application can be found
  [here](https://github.com/linera-io/linera-protocol/blob/{{#include
  ../../RELEASE_HASH}}/examples/counter).
