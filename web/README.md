<!-- cargo-rdme start -->

# `linera-web`

This module defines the JavaScript bindings to the client API.

It is compiled to Wasm, with a JavaScript wrapper to inject its imports, and published on
NPM as `@linera/client`.

There is a supplementary package `@linera/signer`, contained within the `signer`
subdirectory, that defines signer implementations for different transaction-signing
policies, including in-memory keys and signing using an existing MetaMask wallet.

<!-- cargo-rdme end -->

## Building

Make sure your `cargo` invokes a nightly `rustc`. Then, you can build
the package with `pnpm build` and publish it with `pnpm publish`.

## Contributing

See the [CONTRIBUTING](../CONTRIBUTING.md) file for how to help out.

## License

This project is available under the terms of the [Apache 2.0 license](../LICENSE).
