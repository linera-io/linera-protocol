# Linera service and indexer GraphQL client

<!-- cargo-rdme start -->

A GraphQL client for the node service and the indexer.

<!-- cargo-rdme end -->

## Generate schema

To generate the linera service GraphQL schema, a binary `linera-export-schema` is available:
```bash
cargo run --bin linera-schema-export > linera-graphql-client/graphql/service_schema.graphql
```

To generate the indexer GraphQL schema:
```bash
cargo run --bin linera-indexer schema > linera-graphql-client/graphql/indexer_schema.graphql
```

To generate the indexer operations GraphQL schema:
```bash
cargo run --bin linera-indexer schema operations > linera-graphql-client/graphql/operations_schema.graphql
```

## Contributing

See the [CONTRIBUTING](../CONTRIBUTING.md) file for how to help out.

## License

This project is available under the terms of the [Apache 2.0 license](../LICENSE).
