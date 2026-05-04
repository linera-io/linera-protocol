# Formats Registry Example Application

This example application maintains an on-chain registry that maps a
[`ModuleId`](https://docs.rs/linera-base/latest/linera_base/identifiers/struct.ModuleId.html)
to an arbitrary blob of bytes — typically the BCS serialization of an application's
[`Formats`](../../linera-sdk/src/formats.rs) value (i.e. its
[`serde_reflection`](https://docs.rs/serde-reflection) registry plus the four top-level
formats for `Operation`, `Response`, `Message` and `EventValue`).

The intended consumer is the `linera-explorer`: when it observes an operation, message
or event for some application, it can look up the application's `ModuleId` in this
registry, fetch the registered bytes, and use them to decode raw BCS payloads as JSON
for display.

## How It Works

The application has a single operation:

```rust
enum Operation {
    Write { module_id: ModuleId, value: Vec<u8> },
}
```

When `Write` is executed, the contract:

1. Asserts that no entry exists yet for `module_id` (entries are **immutable** —
   first-write-wins; a `ModuleId` cannot be overwritten).
2. Publishes `value` as an immutable [`DataBlob`](../../linera-base/src/data_types.rs)
   via `runtime.create_data_blob(value)`.
3. Stores the resulting `DataBlobHash` in a `MapView<ModuleId, DataBlobHash>` keyed by
   `module_id`.

The state is therefore compact (one hash per `ModuleId`), and every registered value
benefits from the usual content-addressed deduplication of the blob layer.

The service exposes:

- `query { get(moduleId: "...") }` — returns the bytes registered for `moduleId`, or
  `null` if none. Internally, the service reads the hash from the map and then calls
  `runtime.read_data_blob(hash)` to fetch the blob.
- `mutation { write(moduleId: "...", value: [u8]) }` — schedules a `Write` operation.

## Setup and Deployment

Before getting started, make sure that the binary tools `linera*` corresponding to
your version of `linera-sdk` are in your PATH. For scripting purposes, we also assume
that the BASH function `linera_spawn` is defined.

From the root of the Linera repository, this can be achieved as follows:

```bash
export PATH="$PWD/target/debug:$PATH"
eval "$(linera net helper 2>/dev/null)"
```

Next, start the local Linera network and run a faucet:

```bash
LINERA_FAUCET_PORT=8079
LINERA_FAUCET_URL=http://localhost:$LINERA_FAUCET_PORT
linera_spawn linera net up --with-faucet --faucet-port $LINERA_FAUCET_PORT
```

Create the user wallet and request a chain from the faucet:

```bash
export LINERA_WALLET="$LINERA_TMP_DIR/wallet.json"
export LINERA_KEYSTORE="$LINERA_TMP_DIR/keystore.json"
export LINERA_STORAGE="rocksdb:$LINERA_TMP_DIR/client.db"

linera wallet init --faucet $LINERA_FAUCET_URL

INFO=($(linera wallet request-chain --faucet $LINERA_FAUCET_URL))
CHAIN="${INFO[0]}"
OWNER="${INFO[1]}"
```

Build the WebAssembly binaries and deploy the application:

```bash
cd examples/formats-registry
cargo build --release --target wasm32-unknown-unknown

LINERA_APPLICATION_ID=$(linera publish-and-create \
  ../target/wasm32-unknown-unknown/release/formats_registry_{contract,service}.wasm)
```

## Connecting with the GraphQL Client

Start a node service for the wallet:

```bash
PORT=8080
linera service --port $PORT &
echo "http://localhost:$PORT/chains/$CHAIN/applications/$LINERA_APPLICATION_ID"
```

Open the printed URL to land in a GraphiQL session connected to the registry.

To register the bytes for a module (replace `<MODULE_ID_HEX>` and `<BYTES>` accordingly;
`ModuleId` is encoded as a hex string and `value` is a JSON array of byte values):

```gql,uri=http://localhost:8080/chains/$CHAIN/applications/$LINERA_APPLICATION_ID
mutation {
  write(
    moduleId: "<MODULE_ID_HEX>",
    value: [1, 2, 3, 4]
  )
}
```

To read it back:

```gql,uri=http://localhost:8080/chains/$CHAIN/applications/$LINERA_APPLICATION_ID
query {
  get(moduleId: "<MODULE_ID_HEX>")
}
```

A second `write` for the same `moduleId` will fail — registered entries are
immutable.
