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

## Signers

This package ships two reference `Signer` implementations.

### `PrivateKey` (testing / development only)

`PrivateKey` keeps an in-memory secp256k1 key (EIP-191 signing). Intended for tests and local
development — do not use in production where the key must persist or where the page is
exposed to untrusted code.

### `WebCryptoEd25519` (browser, production-ready for session keys)

`WebCryptoEd25519` holds the private half of an Ed25519 keypair as a non-extractable
`CryptoKey` from the Web Crypto API. The raw bytes never enter JavaScript: the key is
generated with `extractable: false` and the `CryptoKey` handle is persisted in IndexedDB
so it survives reloads. An in-page attacker can request signatures while the tab is open,
but cannot exfiltrate the key.

```typescript
import * as linera from "@linera/client";

await linera.initialize();
const signer = await linera.signer.WebCryptoEd25519.loadOrCreate("my-app-key");
const owner = signer.address(); // "0x" + 64 hex chars (AccountOwner::Address32)
```

Browser support: Chrome 137+, Firefox 129+, Safari 17+ — Web Crypto Ed25519 is
available in all current stable browsers.

## Contributing

See the [CONTRIBUTING](../../../CONTRIBUTING.md) file for how to help out.

## License

This project is available under the terms of the [Apache 2.0 license](../../../LICENSE).
