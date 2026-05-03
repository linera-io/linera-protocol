# Linera Explorer

<!-- cargo-rdme start -->

This module provides web files to run a block explorer from Linera service node and Linera indexer.

<!-- cargo-rdme end -->

## Build instructions

```bash
npm install
```
and then
```bash
npm run full
```

Then, to serve the web application on a local server, run:
```bash
npm run serve
```

The URL to access the Block Explorer will be printed.

## Testing the BCS introspection feature

The explorer can decode raw BCS payloads (operations, messages, events) for a
user application as JSON, provided the application's
[`Formats`](../linera-sdk/src/formats.rs) value has been registered in a
deployed `formats-registry` application. This section walks through testing
that feature end to end.

### Overview

There are two pieces of configuration the explorer needs in order to look up
formats for a given application:

- `formats_registry_chain` — the chain ID where the `formats-registry`
  application lives.
- `formats_registry_app_id` — the application ID of the `formats-registry`
  application itself.

When both are set, the explorer will, for each user application it observes,
issue `query { get(moduleId: "<hex>") }` against the registry. If the registry
returns a payload, the explorer deserializes it as `Formats` and uses it to
decode BCS bytes shown in the Applications and block detail views.

### Configuring the registry

The two values can be supplied in either of two ways.

#### Option A — set them at build time via `.env.local`

Create a file `linera-explorer/.env.local` (gitignored) containing:

```
VITE_FORMATS_REGISTRY_CHAIN=<chain-id-hex>
VITE_FORMATS_REGISTRY_APP_ID=<app-id-hex>
```

These values are picked up by Vite at build time (`npm run full` /
`npm run serve`). On the first load the explorer copies them into its
runtime config (persisted to `localStorage`); they are pre-filled in the
navbar inputs but can still be overridden there.

#### Option B — set them at runtime in the navbar

Open the explorer in a browser and use the two inputs in the navbar
(`formats registry: chain id` and `formats registry: application id`).
Each change is saved to `localStorage` and survives a page reload. Clearing
an input writes `null` and effectively disables the lookup.

### End-to-end flow on a local network

The steps below assume the standard local-net setup from
[`examples/formats-registry/README.md`](../examples/formats-registry/README.md);
refer to that file for the full sequence of `linera net up`, wallet init and
chain request. The summary here highlights the explorer-specific steps.

1. **Start a local network and a wallet.** Follow
   [`examples/formats-registry/README.md`](../examples/formats-registry/README.md)
   up to the point where `$CHAIN` and `$LINERA_WALLET` are populated.

2. **Deploy the `formats-registry` application.** From the same README,
   building and publishing it yields:

   ```bash
   cd examples/formats-registry
   cargo build --release --target wasm32-unknown-unknown
   FORMATS_REGISTRY_APP_ID=$(linera publish-and-create \
     ../target/wasm32-unknown-unknown/release/formats_registry_{contract,service}.wasm)
   ```

   Note the chain it was created on (`$CHAIN`) — that is your
   `formats_registry_chain`. The returned application ID is your
   `formats_registry_app_id`.

3. **Deploy a user application whose formats get registered atomically.**
   The CLI command `linera publish-bcs-module` publishes a module *and*
   writes its `Formats` to the registry in a single block. It expects an
   insta SNAP file containing the YAML serialization of `Formats`.

   For applications under `examples/`, that SNAP file is produced by the
   `format.rs` integration test (e.g.
   `examples/fungible/tests/snapshots/format__format.snap`). Generate or
   refresh it with:

   ```bash
   cd examples/fungible
   cargo test --test format
   ```

   Then publish the module along with its formats and create an
   application instance:

   ```bash
   MODULE_ID=$(linera publish-bcs-module \
     ../target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm \
     examples/fungible/tests/snapshots/format__format.snap \
     $FORMATS_REGISTRY_APP_ID)

   APP_ID=$(linera create-application $MODULE_ID --json-argument '...')
   ```

   (Substitute the appropriate `--json-argument` for the chosen example.)

4. **Run a node service and an indexer.** The explorer talks to a
   `linera service` node and (optionally) a `linera-indexer` instance:

   ```bash
   linera service --port 8080 &
   linera-indexer --port 8081 &
   ```

5. **Build and serve the explorer.** From `linera-explorer/`:

   ```bash
   npm install
   npm run full
   npm run serve
   ```

   On first load, set the navbar inputs (or `.env.local`) to
   `$CHAIN` and `$FORMATS_REGISTRY_APP_ID`.

### What to verify

On the **Applications** page for the chain that hosts your user app:

- The user application's row shows a green check (`bi-check-circle`) in the
  *Formats* column. Clicking it opens a modal with the registered `Formats`
  JSON.
- The `formats-registry` row itself should show a disabled X
  (`bi-x-circle`) — there is no entry registered for the registry's own
  module, which is expected.
- If the navbar registry fields are empty, every row shows a disabled X
  with the tooltip *“Set the formats registry chain and app id in the
  navbar”*.
- If the registry exists but has no entry for an application, that row
  shows a disabled X with the tooltip *“No formats registered for this
  module”*.
- While lookups are in flight, a small spinner is shown.

In the **block / transaction views**:

- Operations, messages and events for the registered application have
  their raw BCS payload displayed as decoded JSON (using the registered
  `Formats`).
- Applications without a registered `Formats` continue to display the
  raw bytes.

### Troubleshooting

- The browser devtools console logs the registry POST URL, request body
  and response for every lookup (search for `fetch_formats:` and
  `fetch_user_app_formats`). If a request looks malformed or returns
  `errors`, the registry config is wrong or the registry app is not
  reachable on the node.
- A `null` `data.get` in the response is normal — it means the registry
  has no entry for that module.
- If the Applications page shows X for an app whose registry response
  *does* contain bytes, look for a serialization error logged from
  `fetch_user_app_formats_js`; this typically indicates a mismatch
  between the registered `Formats` shape and the `linera-sdk` version
  the explorer was built against.

## Contributing

See the [CONTRIBUTING](../CONTRIBUTING.md) file for how to help out.

## License

This project is available under the terms of the [Apache 2.0 license](../LICENSE).
