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

The application's operations all carry the `owner` on whose behalf they run:

```rust
enum Operation {
    Write { owner: AccountOwner, module_id: ModuleId, blob_hash: DataBlobHash },
    SetAdmins { owner: AccountOwner, admins: Option<Vec<AccountOwner>> },
}
```

`Write` is intentionally kept as the first variant with the exact fields of
[`linera_sdk::abis::formats_registry::Operation::Write`](../../linera-sdk/src/abis/formats_registry.rs),
so that the operation produced by `linera publish-module-with-formats` decodes
correctly here. Extra, implementation-specific admin commands (here `SetAdmins`) are
appended after it.

### Authorization and remote registration

A module can be registered from any chain, but every mutation is ultimately applied on
the application's **creation chain**, where a single admin policy is enforced:

1. On the chain where the operation is submitted, the contract calls
   `runtime.check_account_permission(owner)` to verify the declared `owner` really
   signed (or is the caller of) the operation.
2. If the current chain is the creation chain, the operation is executed locally.
   Otherwise it is forwarded to the creation chain as a cross-chain `Message`.
3. On the creation chain, the security policy is applied before any state change:
   - if an admin set has been configured, `owner` must be one of the admins;
   - if no admin set exists yet, the operation is allowed only when it originates
     locally — remote requests are refused until admins are configured.

This mirrors the policy used by `examples/controller`. To bootstrap, an admin first
runs `SetAdmins` locally on the creation chain; afterwards, the listed admins can
register modules remotely from their own chains.

`Write` carries the [`DataBlobHash`](../../linera-base/src/identifiers.rs) of an
immutable [`DataBlob`](../../linera-base/src/data_types.rs) holding the formats
description. The caller publishes that data blob (e.g. `linera
publish-module-with-formats` emits a `PublishDataBlob` operation in the same block);
the contract `assert`s the blob exists and records its hash in a
`MapView<ModuleId, DataBlobHash>` keyed by `module_id`, after checking that no entry
exists yet — entries are **immutable** (first-write-wins; a `ModuleId` cannot be
overwritten). Only the 32-byte hash travels to the creation chain inside the `Message`,
so state stays compact (one hash per `ModuleId`) and identical values are
content-addressed and deduplicated by the blob layer.

The service exposes:

- `query { read(moduleId: "...") }` — returns the bytes registered for `moduleId`, or
  `null` if none. Internally it reads the stored hash and fetches the data blob.
- `query { admins }` — returns the configured admin accounts, or `null` if none.
- `mutation { write(owner: "...", moduleId: "...", blobHash: "...") }` — schedules a
  `Write` operation (the data blob must be published separately).
- `mutation { setAdmins(owner: "...", admins: ["..."]) }` — schedules a `SetAdmins`
  operation (pass `null` to clear the set).

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

To register a module's formats, first publish the formats description as a data blob
(e.g. `linera publish-data-blob <FILE>`, which prints the `<BLOB_HASH>`). The blob file
is the BCS serialization of the application's `Formats`; the `extract-formats` binary in
this example turns an app's SNAP snapshot into such a file ready for
`linera publish-data-blob`:

```bash
cargo run --bin extract-formats -- ../counter/tests/snapshots/format__format.snap counter-formats.bcs
```

Then bind the published blob to the module with the mutation below (replace
`<MODULE_ID_HEX>`, `<OWNER>` and `<BLOB_HASH>` accordingly; `ModuleId`, `AccountOwner`
and `DataBlobHash` are encoded as strings). `<OWNER>` must be the chain's signer; once
an admin set is configured it must also be one of the admins:

```gql,uri=http://localhost:8080/chains/$CHAIN/applications/$LINERA_APPLICATION_ID
mutation {
  write(
    owner: "<OWNER>",
    moduleId: "<MODULE_ID_HEX>",
    blobHash: "<BLOB_HASH>"
  )
}
```

To read it back:

```gql,uri=http://localhost:8080/chains/$CHAIN/applications/$LINERA_APPLICATION_ID
query {
  read(moduleId: "<MODULE_ID_HEX>")
}
```

A second `write` for the same `moduleId` will fail — registered entries are
immutable.
