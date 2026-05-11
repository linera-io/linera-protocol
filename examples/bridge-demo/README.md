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

## Spawn just the relayer (against an external network)

When you already have contracts deployed on a real EVM (e.g. Base Sepolia)
and apps published on a real Linera network (e.g. testnet Conway), and you
just want to run the relayer locally pointing at those artifacts, use
`spawn-relayer.sh`. It does **not** deploy or register anything — operator
supplies a pre-built env file and a pre-populated data directory.

For full local development (anvil + local validator + frontend) use
`make demo` from `linera-bridge/` instead — that's what `setup.sh` is for.

```bash
cd examples/bridge-demo
./spawn-relayer.sh \
  --env-file /path/to/relayer.env \
  --env-secret-file /path/to/relayer.env.secret \
  --data-dir /path/to/wallet-host-dir
```

Internally this is `docker compose -f docker/docker-compose.bridge-testnet.yml up -d relayer`
with the env-file + data-dir paths injected via `RELAYER_ENV_FILE`,
`RELAYER_ENV_SECRET`, `RELAYER_DATA_DIR`.

The env file must contain the keys consumed by `bridge-entrypoint.sh`:

```
RPC_URL=https://sepolia.base.org
FAUCET_URL=https://faucet.testnet-conway.linera.net
EVM_BRIDGE_ADDRESS=0x...
LINERA_BRIDGE_APP=...
LINERA_FUNGIBLE_APP=...
LINERA_BRIDGE_CHAIN_ID=...
LINERA_BRIDGE_CHAIN_OWNER=...
LINERA_WALLET=/data/wallet.json
LINERA_KEYSTORE=/data/keystore.json
LINERA_STORAGE=rocksdb:/data/client.db
MONITOR_SCAN_INTERVAL=30
MONITOR_START_BLOCK=0
MAX_RETRIES=10
PORT=3001
```

Plus `EVM_PRIVATE_KEY=0x...` in the secret env file.

The data dir is bind-mounted at `/data` in the container and must already
contain `wallet.json`, `keystore.json`, and `client.db` for a wallet that
owns the bridge chain on the target Linera network.

For a production testnet deployment runbook see
[`docker/README.testnet.md`](../../docker/README.testnet.md).

### Setup script flags (Docker mode)

| Flag | Default | Description |
|------|---------|-------------|
| `--compose-file PATH` | **required** | Docker Compose file |
| `--faucet-url URL` | `http://localhost:8080` | Linera faucet |
| `--relay-url URL` | `http://localhost:3001` | Relay HTTP endpoint |
| `--ticker-symbol SYM` | wTT | Wrapped token ticker |
| `--fund-amount WEI` | 500e18 | Fund bridge with ERC-20; 0 to skip |
| `--shared-dir PATH` | auto (`/tmp/bridge-demo-*`) | Shared state directory for relay coordination |
| `--wasm-dir PATH` | `/wasm` | Directory with `.wasm` binaries |
| `--contracts-dir PATH` | `/contracts` | Solidity source root |
| `--output PATH` | `.env.local` | Output env file |

## How a deposit works

1. User approves ERC-20 spend on `FungibleBridge`
2. User calls `FungibleBridge.deposit(chainId, appId, owner, amount)`
3. Relay's EVM scanner detects the `DepositInitiated` event
4. Relay generates a receipt inclusion proof (MPT) and submits it to the
   `evm-bridge` Linera app
5. `evm-bridge` verifies the proof and tells `wrapped-fungible` to mint tokens
6. User's Linera balance updates

## How a withdrawal works

1. User calls `wrapped-fungible.transfer` targeting the relay's chain with an
   `Address20` owner (their EVM address)
2. On the relay chain, the `Credit` message triggers an auto-burn and emits a
   `BurnEvent` on the `"burns"` stream
3. Relay detects the `BurnEvent` and forwards the Linera block to
   `FungibleBridge.addBlock()`
4. `FungibleBridge` verifies the block via `LightClient`, deserializes the
   `BurnEvent`, and transfers ERC-20 tokens to the user's EVM address

## Active scanning and auto-retry

The relay actively scans both chains to detect missed or failed bridging
requests and automatically retries them:

- **EVM→Linera deposits**: the relay polls EVM for `DepositInitiated` events
  and checks the Linera `evm-bridge` app to see if each deposit has been
  processed. Unprocessed deposits are retried by regenerating the MPT proof
  and resubmitting.
- **Linera→EVM burns**: the relay scans Linera blocks for `BurnEvent` events
  on the `"burns"` stream and checks EVM for matching ERC-20 `Transfer` events.
  Unforwarded burns are retried by re-reading the block containing the
  auto-burn from chain storage and re-calling `addBlock`.

This means the relay self-heals after crashes or transient RPC failures
without operator intervention. On-chain replay protection (`processed_deposits`
on Linera, `verifiedBlocks` on EVM) makes retries safe.

Monitoring endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /monitor/status` | Summary counts of pending/completed deposits and burns |
| `GET /monitor/deposits?status=pending` | List pending deposits |
| `GET /monitor/burns?status=pending` | List unforwarded burns |

Relay flags for tuning:

| Flag | Default | Description |
|------|---------|-------------|
| `--monitor-scan-interval` | 30 | Seconds between chain scan iterations |
| `--monitor-start-block` | 0 | EVM block to start scanning from |
| `--max-retries` | 10 | Max retry attempts before marking an item as failed |

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
