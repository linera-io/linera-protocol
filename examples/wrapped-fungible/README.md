# Wrapped Fungible Token Example Application

This example application implements a wrapped (bridged) fungible token, used by the EVM↔Linera bridge. It extends the base `fungible` token with `MintAndTransfer` and `Burn` operations whose authority is restricted to a single registered application — the *authorized caller* — on a designated mint chain. In the bridge, that authorized caller is the `evm-bridge` application.

## How It Works

The wrapped fungible token shares the same account model as the `fungible` example: individual chains have accounts with owners and balances. It adds supply control (`MintAndTransfer`/`Burn`) gated to a registered authorized caller.

### Authorization

`MintAndTransfer` and `Burn` are driven only by the registered authorized caller, and only on the designated mint chain. Two checks, both mandatory:

- The cross-application caller must equal the application registered via `RegisterAuthorizedCaller`.
- The operation must run on `mint_chain_id` (a `WrappedParameters` field).

`RegisterAuthorizedCaller` is itself restricted to the mint chain — the only chain where the authorized caller is consulted — and requires an authenticated signer. The caller is registered after creation rather than passed as a parameter, so an authorized caller that takes this token's id as a creation parameter can be created first (see Deployment).

There is no signer check on `MintAndTransfer`/`Burn`: the authorized caller is trusted to validate its own input, so whoever relays the request is irrelevant. This keeps relaying permissionless.

### Operations

All standard fungible operations are supported (`Transfer`, `TransferFrom`, `Approve`, `Claim`, `Balance`, `TickerSymbol`), plus:

- `MintAndTransfer { target_account, amount }` — creates new tokens; authorized caller only, on the mint chain.
- `Burn { owner, amount }` — burns tokens from an account; authorized caller only, on the mint chain.
- `RegisterAuthorizedCaller { app_id }` — registers the application allowed to `MintAndTransfer`/`Burn`; mint chain only, requires an authenticated signer.

## Deployment

The wrapped fungible token is deployed as part of the EVM↔Linera bridge. Because the `evm-bridge` application takes this token's id as a creation parameter, the token is created first:

1. Deploy `wrapped-fungible`.
2. Deploy the `evm-bridge` application, pointing it at the wrapped-fungible app id.
3. On the mint chain, register the bridge as the authorized caller via `RegisterAuthorizedCaller`.

See `examples/bridge-demo/setup.sh` for the complete deployment script.
