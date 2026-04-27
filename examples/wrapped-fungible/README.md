# Wrapped Fungible Token Example Application

This example application implements a wrapped (bridged) fungible token for the EVM<>Linera bridge. It extends the base fungible token with minting, burning, and bridge-specific authorization.

Tokens are minted when a deposit proof from EVM is verified by the `evm-bridge` application, and burned automatically when transferred cross-chain to an Ethereum address (`Address20`) on the bridge chain.

## How It Works

The wrapped fungible token shares the same account model as the `fungible` example: individual chains have accounts with owners and balances. It adds three bridge-specific features:

### Minting

Minting creates new wrapped tokens backed by locked ERC-20 tokens on EVM. Authorization is configurable via `WrappedParameters`:

- `bridge_app_id` (optional): if set, minting is only allowed via cross-application call from the bridge app
- `minter` (optional): if set, the block signer must match this account
- `mint_chain_id` (optional): if set, minting is restricted to this chain

Each check is enforced only when the corresponding parameter is `Some`. A fully unconfigured token allows unrestricted minting.

### Auto-Burning

When tokens are transferred cross-chain to an `Address20` target (Ethereum address) on the designated bridge chain, they are automatically burned instead of credited. The contract emits a `BurnEvent` on the `"burns"` stream, which the off-chain relayer detects and forwards to the EVM bridge contract to release the corresponding ERC-20 tokens.

This replaces the previous flow where the relayer had to propose a separate `Burn` operation.

### Operations

All standard fungible operations are supported (`Transfer`, `TransferFrom`, `Approve`, `Claim`, `Balance`, `TickerSymbol`), plus:

- `Mint { target_account, amount }` - Creates new tokens (subject to authorization checks)
- `Burn { owner, amount }` - Rejected at the contract level. Burning happens automatically via cross-chain transfer to an Address20 on the bridge chain.

## Deployment

The wrapped fungible token is deployed as part of the EVM<>Linera bridge. The deployment order is:

1. Deploy the `evm-bridge` Linera application
2. Deploy `wrapped-fungible` with `bridge_app_id` pointing to the bridge app
3. Register the wrapped-fungible app ID in the bridge via `RegisterFungibleApp`

See `examples/bridge-demo/setup.sh` for the complete deployment script.
