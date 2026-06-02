# Linera Playground

A browser-based GraphQL playground for Linera applications. Enter a faucet URL, a
chain ID and an application ID, type a GraphQL query, and run it. The whole
client runs in the browser via the `@linera/client` WASM package: it spins up a
`Client`, creates a `ChainClient` for the chosen chain, synchronizes it from the
validators, and queries the application's GraphQL service. Mutations go through
the same path and are signed by a connected signer.

See [DESIGN.md](./DESIGN.md) for the architecture.

## Requirements

- Node.js and `pnpm` (the repo uses pnpm workspaces).
- The `@linera/client` and `@linera/metamask` packages under `../web` are
  consumed as workspace packages. `pnpm install` builds them if needed (this
  compiles the WASM client and requires the Rust toolchain from
  `../web/rust-toolchain.toml`).

## Install and run

```bash
cd linera-playground
pnpm install
pnpm dev
```

Open the printed URL (default <http://localhost:5173>).

## Using it

1. **Faucet URL** — the network to connect to (provides the genesis config and
   validators). For a local network this is the faucet started by
   `linera net up --with-faucet` (see below).
2. **Chain ID** and **Application ID** — the application to query.
3. Click **Apply** to load the application's GraphQL schema into the editor (this
   powers the docs explorer and autocomplete). Use the editor's refresh-schema
   button, or click **Apply** again, after changing the target.
4. Type a query and run it with the ▶ button.

### Mutations

Reads work with no signer connected (the playground uses a throwaway key). To run
a mutation you must connect a signer that **owns the target chain**:

- **Use key** — paste a private key (hex) or mnemonic.
- **Connect MetaMask** — sign with the MetaMask extension.

A mutation builds and signs a block; the validators reject it if the connected
owner is not an owner of the chain.

## Manual end-to-end test (local network)

From the root of the repository, start a local network with a faucet and deploy
an example application (here, the counter):

```bash
export PATH="$PWD/target/debug:$PATH"
eval "$(linera net helper 2>/dev/null)"

LINERA_FAUCET_PORT=8079
export LINERA_FAUCET_URL=http://localhost:$LINERA_FAUCET_PORT
linera_spawn linera net up --with-faucet --faucet-port $LINERA_FAUCET_PORT

# In another shell, set up a wallet from the faucet and deploy the counter app,
# following examples/counter/README.md. Note the resulting CHAIN ID and
# APPLICATION ID.
```

Then in the playground:

1. Faucet URL: `http://localhost:8079`
2. Chain ID / Application ID: the values from the deployment.
3. **Apply**, then run `query { value }` — it should return the counter value.
4. **Use key** with the chain owner's key, then run
   `mutation { increment(value: 1) }`, and re-run `query { value }` to see it
   change.
