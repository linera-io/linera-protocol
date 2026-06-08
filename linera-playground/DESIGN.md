# Linera Playground — Design

## Motivation

Developers building Linera applications need a quick way to run GraphQL queries
and mutations against a deployed application without wiring up a custom
front-end or running a local `linera service` node. The built-in GraphiQL that
`linera service` serves talks to a node process over HTTP; this tool instead
runs the whole client in the browser via the `@linera/client` WASM package, so
there is nothing to install or run server-side.

The Linera Playground is a browser app: you enter a faucet URL, a chain ID, an
application ID, type a GraphQL query, and hit run. Under the hood it spins up an
in-browser `Client`, creates a `ChainClient` for the chosen chain, synchronizes
it from the validators, and queries the application's GraphQL service. Mutations
go through the same path and are signed by a connected signer.

## Decisions

- **Network config:** a faucet URL input field. `Faucet.createWallet()`
  bootstraps the network/genesis config; queries can then target any chain on
  that network.
- **Identity / signer:** the user pastes a private key (or mnemonic) or connects
  MetaMask. Both are EVM secp256k1 signers; the owner identity is the EVM
  address. With no signer connected, the playground uses an ephemeral random key
  so reads work; mutations naturally fail at the validator (it owns nothing).
- **Placement / stack:** top-level `linera-playground/`, React 18 + Vite 7 +
  TypeScript, consuming `@linera/client` and `@linera/metamask` as workspace
  packages (its own `pnpm-workspace.yaml`, mirroring `examples/`).
- **Editor:** the off-the-shelf `graphiql` React component with a custom
  `fetcher` that routes every operation (including the introspection query that
  powers the docs explorer and autocomplete) through the WASM client's
  `Application.query()`.
- **Client lifecycle:** one persistent `Client` per (faucet URL, signer),
  memoized; `Chain` and `Application` handles are cached. Rebuilt only when the
  faucet URL or the signer changes.
- **Chain sync:** `@linera/client` did not expose a standalone synchronize, and
  read-only `query_application` only reads the local node. We add a
  `Chain.synchronize()` method to `@linera/client` (`synchronize_from_validators`
  + `update_wallet`) and call it before each query. Mutations already sync via
  `prepare_chain` inside `apply_client_command`, but reads need this.

## Architecture

```
React app (linera-playground/)
  ConfigBar           SignerControls            GraphiQL (graphiql pkg)
   faucet URL          paste key / MetaMask       query editor + docs explorer
   chain id            shows active owner         run -> response pane
   application id
        \                    |                          | fetcher(params)
         \___________________|__________________________|
                             v
                    LineraClientManager  <----  makeFetcher(manager, getTarget)
                    (plain TS, no React)
                     ensureClient(faucetUrl, signer)  -> memoized Client
                     ensureChain(chainId, owner)      -> memoized Chain
                     ensureApp(chainId, appId)        -> memoized Application
                     query(target, request):
                       chain.synchronize(); JSON.parse(app.query(JSON(request)))
                             |
                  @linera/client (WASM)            @linera/metamask
                             v
   Faucet -> Wallet -> Client -> ChainClient -> synchronize -> Application.query
                                                    -> (mutation) sign + propose block
```

## Data flow

A run from GraphiQL's ▶ button calls `fetcher({query, variables, operationName})`:

1. If faucet URL / chain ID / application ID are not all set, return a friendly
   GraphQL error so GraphiQL's auto-introspection on mount does not error noisily.
2. `manager.query(target, request)`:
   - `ensureClient`: build `new Faucet(url)` -> `createWallet()` ->
     `new Client(wallet, signer)` if not cached for this (url, signer).
   - `ensureChain`: `client.chain(chainId, { owner })` (owner omitted for the
     read-only ephemeral signer).
   - `chain.synchronize()` — pull latest certificates from validators.
   - `ensureApp`: `chain.application(appId)`.
   - `JSON.parse(await app.query(JSON.stringify(request)))`.
3. The `Application.query()` call runs the app's GraphQL service against the
   synced local state. If the operation mutates, the WASM client builds, signs
   (via the connected signer), and proposes a block before returning.
4. Thrown WASM errors (bad faucet, sync failure, validator rejection of an
   unauthorized mutation) are mapped to `{ errors: [...] }` so they render in the
   response pane instead of crashing the app.

GraphiQL is remounted (via a `key` of `faucet|chain|app|owner`) when the target
changes, so it re-introspects the new application's schema.

## Components (each independently understandable)

- `src/linera/clientManager.ts` — `LineraClientManager`: owns the WASM lifecycle
  and memoization; `setSigner`, `query`, internal `ensureClient/Chain/App` and
  `teardown` (frees cached handles, then `asyncDispose`s the client so the
  IndexedDB wallet lock is released before a new client is built).
- `src/linera/signers.ts` — `ephemeralSigner()`, `privateKeySigner(secret)`,
  `metaMaskSigner()`; each returns `{ id, signer, owner }`.
- `src/linera/fetcher.ts` — `makeFetcher(manager, getTarget)`: adapts the WASM
  query to GraphiQL's fetcher contract and maps errors.
- `src/App.tsx` — holds config + active-signer state, wires the toolbar to the
  manager, hosts `<GraphiQL>`. Inputs are persisted to `localStorage`.
- `src/components/ConfigBar.tsx`, `SignerControls.tsx` — the toolbar.

## `@linera/client` change

Add to `web/@linera/client/src/chain/mod.rs`:

```rust
pub async fn synchronize(&self) -> JsResult<()> {
    self.chain_client.synchronize_from_validators().await?;
    let mut client = self.client.context.lock().await;
    client.update_wallet(&self.chain_client).await?;
    Ok(())
}
```

Requires rebuilding the package (`bash build.bash --release`).

## Scope / non-goals

- **In:** typed chain + app + query, reads, auto-signed mutations, private-key
  and MetaMask signers, docs explorer via introspection.
- **Out:** GraphQL subscriptions (the WASM `Application.query()` is
  request/response only), wallet/chain management UI, saved-query libraries,
  multi-network tabs.

## Testing

No automated tests for now (per Mat) — verified manually. The README documents
an end-to-end smoke test against a local `linera net up --with-faucet`.
