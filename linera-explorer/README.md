# Linera Explorer

<!-- cargo-rdme start -->

This module provides web files to run a block explorer from linera service node.

<!-- cargo-rdme end -->

## Build instructions

After building linera with `cargo`, run
```bash
npm i
```
and then
```
webpack
```
It will create a `dist` folder from where you can run an HTTP server

## Generate schema

To generate the linera service schema:
- install `get-graphql-schema`:
```bash
npm i get-graphql-schema
```
- start a local network and service (port 8080 for example)
- generate the schema with get-graphql-schema:
```bash
get-graphql-schema http://localhost:8080 > linera-explorer/graphql/schema.graphql
```

## Contributing

See the [CONTRIBUTING](../CONTRIBUTING.md) file for how to help out.

## License

This project is available under the terms of the [Apache 2.0 license](../LICENSE).
