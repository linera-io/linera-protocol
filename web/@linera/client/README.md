<!-- cargo-rdme start -->

# `linera-web`

This module defines the JavaScript bindings to the client API.

It is compiled to Wasm, with a JavaScript wrapper to inject its imports, and published on
NPM as `@linera/client`.

There is a supplementary package `@linera/signer`, contained within the `signer`
subdirectory, that defines signer implementations for different transaction-signing
policies, including in-memory keys and signing using an existing MetaMask wallet.

<!-- cargo-rdme end -->

## Cross-Origin Isolation

This package uses `SharedArrayBuffer` for WebAssembly threading support.
Browsers require [cross-origin isolation](https://web.dev/articles/cross-origin-isolation-guide)
to enable `SharedArrayBuffer`, which means the page serving the wasm module
must include these HTTP headers:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

Without these headers, the wasm module will fail at runtime when the async
executor attempts to use `Atomics.waitAsync` on non-shared memory.

Example server configurations:

**Nginx:**
```nginx
add_header Cross-Origin-Opener-Policy same-origin;
add_header Cross-Origin-Embedder-Policy require-corp;
```

**Caddy:**
```
header Cross-Origin-Opener-Policy same-origin
header Cross-Origin-Embedder-Policy require-corp
```

**Express:**
```js
app.use((req, res, next) => {
  res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
  res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
  next();
});
```

**Note:** `Cross-Origin-Embedder-Policy: require-corp` means all
subresources (images, scripts, fonts) must be same-origin or served with
a `Cross-Origin-Resource-Policy` header. This may require adjustments
when loading third-party assets.

## Building

Make sure your `cargo` invokes a nightly `rustc`. Then, you can build
the package with `pnpm build` and publish it with `pnpm publish`.

## Contributing

See the [CONTRIBUTING](../../../CONTRIBUTING.md) file for how to help out.

## License

This project is available under the terms of the [Apache 2.0 license](../../../LICENSE).
