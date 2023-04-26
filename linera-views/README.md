<!-- cargo-rdme start -->

This module is used in the Linera protocol to map complex data-structures onto a
key-value store. The central notion is a [`views::View`](https://docs.rs/linera-views/latest/linera_views/views/trait.View.html) which can
be loaded from storage, modified in memory, then committed (i.e. the changes are
atomically persisted in storage).

<!-- cargo-rdme end -->

## Contributing

See the [CONTRIBUTING](../CONTRIBUTING.md) file for how to help out.

## License

This project is available under the terms of either the [Apache 2.0 license](../LICENSE).
