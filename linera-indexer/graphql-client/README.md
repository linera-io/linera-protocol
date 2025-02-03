# Linera indexer GraphQL client

<!-- cargo-rdme start -->

A GraphQL client for the indexer.

<!-- cargo-rdme end -->

## Generate schema

To generate the indexer GraphQL schema:
```bash
cargo run --bin linera-indexer schema > linera-indexer/graphql-client/gql/indexer_schema.graphql
```

To generate the indexer operations GraphQL schema:
```bash
cargo run --bin linera-indexer schema operations > linera-indexer/graphql-client/gql/operations_schema.graphql
```

## Contributing

See the [CONTRIBUTING](../../CONTRIBUTING.md) file for how to help out.

## License

This project is available under the terms of the [Apache 2.0 license](../../LICENSE).
