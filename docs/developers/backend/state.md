# Creating the Application State

The state of a Linera application consists of onchain data that are persisted
between transactions.

The `struct` which defines your application's state can be found in
`src/state.rs`. To represent our counter, we're going to use a `u64` integer.

While we could use a plain data-structure for the entire application state:

```rust
struct Counter {
  value: u64
}
```

in general, we prefer to manage persistent data using the concept of "views":

> [Views](https://docs.rs/linera-views/latest/linera_views/) allow an
> application to load persistent data in memory and stage modifications in a
> flexible way.
>
> Views resemble the persistent objects of an
> [ORM](https://en.wikipedia.org/wiki/Object%E2%80%93relational_mapping)
> framework, except that they are stored as a set of key-value pairs (instead of
> a SQL row).

In this case, the struct in `src/state.rs` should be replaced by

```rust
# extern crate linera_sdk;
# extern crate async_graphql;
# use linera_sdk::linera_base_types::*;
# use linera_sdk::*;
# use std::collections::HashSet;
# use linera_sdk::views::{linera_views, RegisterView, RootView, ViewStorageContext};
# use crate::linera_sdk::views::View as _;
/// The application state.
#[derive(RootView, async_graphql::SimpleObject)]
#[view(context = ViewStorageContext)]
pub struct Counter {
    pub value: RegisterView<u64>,
    // Additional fields here will get their own key in storage.
}
```

and the occurrences of `Application` in the rest of the project should be
replaced by `Counter`.

The derive macro `async_graphql::SimpleObject` is related to GraphQL queries
discussed in the [next section](service.md).

A `RegisterView<T>` supports modifying a single value of type `T`. Other data
structures available in the library
[`linera_views`](https://docs.rs/linera-views/latest/linera_views/) include:

- `LogView` for a growing vector of values;
- `QueueView` for queues;
- `MapView` and `CollectionView` for associative maps; specifically, `MapView`
  in the case of static values, and `CollectionView` when values are other
  views.

For an exhaustive list of the different constructions, refer to the crate
[documentation](https://docs.rs/linera-views/latest/linera_views/).
