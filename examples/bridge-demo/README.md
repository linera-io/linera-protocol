# EVM-Linera Bridge Demo

A cross-chain token bridge between an EVM chain and Linera. Deposits lock
ERC-20 tokens on the EVM side and mint wrapped tokens on Linera; withdrawals
burn wrapped tokens and release ERC-20s back to the user.

## Architecture

Three components cooperate:

| Component | EVM side | Linera side |
|-----------|----------|-------------|
| **Contracts / Apps** | `LightClient` (verifies Linera blocks), `FungibleBridge` (holds ERC-20s, emits deposit events) | `wrapped-fungible` (mints/burns wrapped tokens), `evm-bridge` (coordinates messaging) |
| **Relay** | Watches for `DepositInitiated` events, submits receipt proofs | Manages a Linera chain (persistent state), forwards blocks to EVM for withdrawals |
| **Frontend** | MetaMask for EVM transactions | `@linera/client` for Linera queries and signing |

## Quick start (Docker)

Everything runs in containers -- no Foundry, Linera CLI, or Solidity toolchain needed
on the host.

### Prerequisites

- Docker with Compose v2
- pnpm (for the frontend)
- [MetaMask](https://metamask.io/) browser extension
- The repo checked out with submodules

### Steps

From the `linera-bridge/` directory:

```bash
# 1. Build Wasm binaries and Docker images
make demo

# 2. Start the frontend (separate terminal)
make demo-frontend
```

`make demo` is a convenience target that runs:

| Target | What it does |
|--------|-------------|
| `make build-wasm` | Compile `fungible`, `wrapped-fungible`, and `evm-bridge` Wasm binaries |
| `make build-all` | Build Docker images (linera-test, linera-exporter, linera-bridge, foundry-jq) |
| `make up` | Start Anvil, ScyllaDB, Linera validator+faucet, block exporter, and relay |
| `make demo-setup` | Deploy contracts and Linera apps, write `.env.local` |

The frontend starts on <http://localhost:5173>.

### MetaMask setup for local Anvil

The demo runs a local Anvil node that MetaMask doesn't know about. You need
to configure three things:

#### 1. Add the Anvil network

MetaMask → **Settings → Networks → Add Network → Add a network manually**:

| Field            | Value                   |
|------------------|-------------------------|
| Network name     | Anvil                   |
| RPC URL          | `http://localhost:8545`  |
| Chain ID         | `31337`                 |
| Currency symbol  | ETH                     |

Save and switch to the new network.

#### 2. Import the deployer account

The setup script deploys the MockERC20 token from Anvil's default account
(account 0), which holds the entire token supply (1000 TT). You need to
import this account into MetaMask to have tokens available for deposits.

MetaMask → **Account menu → Import account → Paste private key**:

```
0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
```

This is Anvil's well-known dev key -- never use it on a real network.

#### 3. Import the ERC-20 token

After setup completes, the token contract address is printed in the terminal
and written to `.env.local` as `LINERA_TOKEN_ADDRESS`. To see your balance in
MetaMask:

MetaMask → **Tokens → Import tokens**:

| Field           | Value                                        |
|-----------------|----------------------------------------------|
| Token address   | the `LINERA_TOKEN_ADDRESS` from `.env.local` |
| Token symbol    | TT                                           |
| Decimals        | 18                                           |

You should see a balance of **1000 TT**.

### Other useful targets

```bash
make demo-logs   # tail relay logs
make down        # stop everything and remove volumes
```

## Testnet / direct mode

For real network deployments, run everything directly on the host instead of
through Docker.

### Prerequisites

- [Foundry](https://book.getfoundry.sh/getting-started/installation) (`forge`, `cast`)
- `linera` CLI (built from this repo or installed)
- `linera-bridge` CLI: `cargo build -p linera-bridge --features relay`
- pnpm (for the frontend)
- A funded EVM account (private key with ETH for gas)
- An EVM RPC endpoint (e.g. Base Sepolia)
- A running Linera testnet with faucet

### 1. Build the Wasm binaries

From the `linera-bridge/` directory:

```bash
make build-wasm
```

This compiles `fungible`, `wrapped-fungible`, and `evm-bridge` to
`examples/target/wasm32-unknown-unknown/release/`.

### 2. Initialize wallet and claim a bridge chain

```bash
export FAUCET_URL=https://faucet.testnet-conway.linera.net

# Initialize a Linera wallet from the faucet
linera wallet init --faucet "$FAUCET_URL"

# Claim a chain that the relay will use as the "bridge chain"
linera wallet request-chain --faucet "$FAUCET_URL"
```

Note the **chain ID** and **owner** printed by `request-chain` — you'll need
them for both the setup script and the relay.

### 3. Deploy contracts

Pick a shared directory for coordination files between the setup script and
the relay:

```bash
export SHARED_DIR="/tmp/bridge-demo-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$SHARED_DIR"
```

Run the setup script to deploy EVM contracts and Linera apps:

```bash
cd examples/bridge-demo

./setup.sh \
  --evm-rpc-url https://base-sepolia.g.alchemy.com/v2/YOUR_KEY \
  --evm-private-key 0x... \
  --evm-chain-id 84532 \
  --bridge-chain-id <CHAIN_ID> \
  --relay-owner <OWNER> \
  --faucet-url "$FAUCET_URL" \
  --relay-url http://localhost:3001 \
  --shared-dir "$SHARED_DIR"
```

The setup script:
1. Fetches validator info and deploys the **LightClient** contract
2. Deploys a **MockERC20** token (or pass `--token-address` to use an existing one)
3. Publishes **wrapped-fungible** and **evm-bridge** apps on the bridge chain
4. Deploys the **FungibleBridge** contract (referencing LightClient + apps)
5. Funds the bridge with ERC-20 tokens
6. Writes contract/app addresses to `$SHARED_DIR` (relay picks them up) and
   `.env.local` (frontend reads them)

### 4. Start the relay

The relay uses the same keystore created in step 2, plus a `--data-dir` for
persistent RocksDB block storage. Pass the contract addresses and app IDs
from the setup script output.

```bash
linera-bridge serve \
  --rpc-url https://base-sepolia.g.alchemy.com/v2/YOUR_KEY \
  --faucet-url "$FAUCET_URL" \
  --data-dir ~/.config/linera \
  --keystore ~/.config/linera/keystore.json \
  --chain-id <CHAIN_ID> \
  --bridge-address <BRIDGE_ADDRESS> \
  --bridge-app-id <BRIDGE_APP_ID> \
  --fungible-app-id <FUNGIBLE_APP_ID> \
  --evm-private-key 0x...
```

On restart, run the same command — the relay loads persistent state from
`--data-dir` and syncs from validators to catch up on missed blocks.

### 5. Start the frontend

```bash
pnpm install && pnpm dev
```

Open <http://localhost:5173>, connect MetaMask to your EVM network, and use
the deposit/withdraw forms.

### Setup script flags

| Flag | Default (Docker) | Default (Direct) | Description |
|------|-------------------|-------------------|-------------|
| `--compose-file PATH` | -- | -- | Enables Docker mode |
| `--evm-rpc-url URL` | `http://anvil:8545` | `http://localhost:8545` | EVM JSON-RPC endpoint |
| `--evm-private-key KEY` | Anvil account 0 | **required** | Private key for EVM txs |
| `--evm-chain-id ID` | 31337 | 31337 | EVM chain ID |
| `--light-client-address ADDR` | read from `/shared/` | deployed if omitted | Skip LightClient deploy |
| `--bridge-chain-id ID` | polled from relay | **required** | Linera bridge chain (64 hex chars) |
| `--token-address ADDR` | deployed | deployed | Skip MockERC20 deploy |
| `--relay-owner OWNER` | read from `/shared/` | **required** | Relay's AccountOwner (minter) |
| `--faucet-url URL` | `http://localhost:8080` | `http://localhost:8080` | Linera faucet |
| `--relay-url URL` | `http://localhost:3001` | `http://localhost:3001` | Relay HTTP endpoint |
| `--ticker-symbol SYM` | wTT | wTT | Wrapped token ticker |
| `--fund-amount WEI` | 500e18 | 500e18 | Fund bridge with ERC-20; 0 to skip |
| `--shared-dir PATH` | auto (`/tmp/bridge-demo-*`) | auto | Shared state directory for relay coordination |
| `--wasm-dir PATH` | `/wasm` | `../../examples/target/wasm32-unknown-unknown/release` | Directory with `.wasm` binaries |
| `--contracts-dir PATH` | `/contracts` | `../../linera-bridge/src/solidity` | Solidity source root |
| `--output PATH` | `.env.local` | `.env.local` | Output env file |

## How a deposit works

1. User approves ERC-20 spend on `FungibleBridge`
2. User calls `FungibleBridge.deposit(chainId, appId, owner, amount)`
3. Frontend POSTs the tx hash to the relay's `/deposit` endpoint
4. Relay generates a receipt inclusion proof (MPT) and submits it to the
   `evm-bridge` Linera app
5. `evm-bridge` verifies the proof and tells `wrapped-fungible` to mint tokens
6. User's Linera balance updates

## How a withdrawal works

1. User calls `wrapped-fungible.transfer` targeting the relay's chain with an
   `Address20` owner (their EVM address)
2. Wrapped tokens are burned and a `Credit` message is sent to the relay chain
3. Relay forwards the Linera block containing the `Credit` to
   `FungibleBridge.addBlock()`
4. `FungibleBridge` verifies the block via `LightClient`, deserializes the
   `Credit`, and transfers ERC-20 tokens to the user's EVM address

## Frontend environment

The frontend reads these variables from `.env.local` (generated by `setup.sh`):

```
LINERA_FAUCET_URL
LINERA_APPLICATION_ID       # wrapped-fungible app ID
LINERA_BRIDGE_APP_ID        # evm-bridge app ID
LINERA_RELAY_URL
LINERA_BRIDGE_ADDRESS       # FungibleBridge contract
LINERA_TOKEN_ADDRESS        # ERC-20 token contract
LINERA_BRIDGE_CHAIN_ID
LINERA_EVM_CHAIN_ID
```

All are required -- the Vite dev server will refuse to start if any are missing.

## EVM finality verification

The `evm-bridge` Linera app verifies that deposit blocks are finalized by
querying an EVM JSON-RPC endpoint (the `rpc_endpoint` parameter). This endpoint
hostname must be in the Linera network's HTTP request allow list.

- **Docker mode**: The compose file passes `--http-request-allow-list` to
  `linera net up`, defaulting to `anvil`. Override with the
  `HTTP_REQUEST_ALLOW_LIST` env var (e.g.
  `HTTP_REQUEST_ALLOW_LIST=base-sepolia.g.alchemy.com`).
- **Testnet mode**: The Linera testnet validators must whitelist the RPC
  hostname (e.g. `base-sepolia.g.alchemy.com`). Contact the testnet operators
  to add your RPC provider's hostname to the allow list.

If the RPC hostname is not whitelisted, deposits will fail with
`UnauthorizedHttpRequest`. As a workaround, set `rpc_endpoint` to empty in the
evm-bridge parameters to skip finality verification (not recommended for
production).

## Troubleshooting

- **MetaMask shows 0 ETH** (local): make sure you switched to the Anvil
  network and imported the correct private key.
- **Token balance is 0**: double-check that you imported the token using the
  address from `.env.local`, and that you are on the correct account.
- **Deposit hangs**: check relay logs -- the relay needs to be running and
  healthy before submitting deposits.
