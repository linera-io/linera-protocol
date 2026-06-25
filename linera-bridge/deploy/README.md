# Bridge deployment runbook

Provision a complete EVM↔Linera bridge against a **real** EVM network (Base,
Ethereum, …) and a **real** Linera network, by running a sequence of
copy-pasteable commands inside the project's pre-built Docker images. No host
toolchain beyond Docker (and a Rust toolchain *once*, to build the Wasm modules).

Every step runs in one of three images, through a thin shell wrapper you paste
once (`docker-foundry`, `docker-linera`, `docker-linera-bridge`). Values produced
by one step (addresses, app IDs, the bridge chain) are copied forward as
environment variables in a single shell session — what you see is what runs.

> Deploying a bridge is not the same as **running** it. This runbook provisions
> contracts, apps, and registrations, then emits a relayer env file. To operate
> the relayer afterwards, see [`docker/README.testnet.md`](../../docker/README.testnet.md).

## What gets deployed

A bridge spans both chains, and the pieces have a strict creation order because
later steps consume earlier outputs:

```
  Linera faucet ──init-light-client──▶ committee args (+ pause-guardian, proposer)
                                            │
  EVM:  LightClient ◀───────────────────────┘
        Token (reuse existing ERC-20, or deploy LineraToken)

  Linera: request bridge chain  ──▶ chain id + owner
          wrapped-fungible app  ──▶ WRAPPED_APP_ID   (needs token, chain ids)
          evm-bridge app        ──▶ BRIDGE_APP_ID    (needs WRAPPED_APP_ID)

  EVM:  FungibleBridge ◀── LightClient + chain id + token + app ids + governance
                                            │
  Linera: register on both sides ◀──────────┘
          (wrapped: authorize bridge;  evm-bridge: record FungibleBridge addr)
```

| Side | Artifact | Notes |
|------|----------|-------|
| EVM | `LightClient` | Verifies Linera certificates; genesis committee from the faucet |
| EVM | ERC-20 token | An existing token, or a freshly-deployed `LineraToken` |
| EVM | `FungibleBridge` | Holds ERC-20 liquidity; releases on verified `BurnEvent`s |
| Linera | bridge chain | A chain the relayer owns and drives |
| Linera | `wrapped-fungible` app | Mints/burns wrapped tokens |
| Linera | `evm-bridge` app | Verifies EVM deposit proofs; coordinates mint/burn |

## The three images

| Wrapper | Image | Built by | Provides |
|---------|-------|----------|----------|
| `docker-foundry` | `foundry-jq` | `Dockerfile.foundry` | `forge`, `cast`, `jq` |
| `docker-linera` | `linera-test` | `Dockerfile` (`--target runtime`) | the `linera` client (`/linera`) |
| `docker-linera-bridge` | `linera-bridge` | `Dockerfile.bridge` | the `linera-bridge` CLI |

## Prerequisites

On the host running the deploy:

- **Docker** (with the repo directory shared — the default for paths under your
  home directory; `$OUT` below must live inside the repo, not `/tmp`).
- The repo checked out **with submodules** so `forge` can compile the contracts:
  ```bash
  git submodule update --init --recursive
  ```
- The **three images** and the **Wasm modules**, built once from the repo root:
  ```bash
  make -C linera-bridge build-all
  ```
  This builds `foundry-jq`, `linera-test`, `linera-bridge`, and (via its
  `build-wasm` dependency) the four `.wasm` modules that `publish-and-create`
  needs. Building the Wasm requires a **Rust toolchain with the
  `wasm32-unknown-unknown` target** (`rustup target add wasm32-unknown-unknown`);
  it is the one host toolchain this runbook assumes. Verify the modules exist:
  ```bash
  ls examples/target/wasm32-unknown-unknown/release/wrapped_fungible_{contract,service}.wasm \
     linera-bridge/contracts/evm-bridge/target/wasm32-unknown-unknown/release/evm_bridge_{contract,service}.wasm
  ```
- A **funded EVM account** on the target network (pays for deploys and, later,
  the relayer's `addBlock` gas).
- **Governance addresses** you control, decided up front (EVM addresses):
  a `PAUSE_GUARDIAN`, a `PROPOSER`, and a `CANCELLER` (see
  [Governance roles](#governance-roles)).
- Network access to the target EVM RPC and the Linera faucet.
- **Coordination with the Linera network operators** to whitelist your EVM RPC
  hostname (see [HTTP allow-list](#http-allow-list) — deposits fail without it).

> Run every command below **from the repo root**, in a **single shell session**
> (the wrappers and the copied-forward `export`s live in that session).

## Setup: environment + wrappers

Fill in the per-deployment values, then paste the wrappers and helpers.

```bash
# ── Per-deployment configuration ──
export NETWORK=base-sepolia
export RPC_URL=https://sepolia.base.org              # target EVM JSON-RPC
export FAUCET_URL=https://faucet.<your-linera-net>   # faucet of the Linera network you bridge to
export EVM_PRIVATE_KEY=0x...                          # funded deployer key

# Governance roles (EVM addresses you control)
export PAUSE_GUARDIAN=0x...
export PROPOSER=0x...
export CANCELLER=0x...
export TIMELOCK_DELAY=172800                          # FungibleBridge timelock, seconds (e.g. 48h)

# Wrapped-token metadata on Linera
export TICKER_SYMBOL=wTT
export TOKEN_DECIMALS=18                               # MUST match the ERC-20's decimals

# RPC the Linera validators use for EVM finality (must be allow-listed; defaults to RPC_URL)
export EVM_RPC_ENDPOINT_FOR_LINERA="${EVM_RPC_ENDPOINT_FOR_LINERA:-$RPC_URL}"

# Output dir — holds the bridge-chain wallet. BACK THIS UP. Must be under the repo.
export OUT="$PWD/linera-bridge/deploy/out/$NETWORK"
mkdir -p "$OUT/wallet"
```

```bash
# ── Wrappers (paste once) ──

# forge / cast / jq. Its entrypoint is `sh -c`, so the command is one string;
# $RPC_URL and $EVM_PRIVATE_KEY are expanded INSIDE the container (forwarded via -e).
docker-foundry() {
  docker run --rm \
    -e RPC_URL -e EVM_PRIVATE_KEY \
    -e LIGHT_CLIENT_ARGS_JSON_FILE \
    -e TOKEN_NAME -e TOKEN_SYMBOL -e TOKEN_DECIMALS -e TOKEN_SUPPLY \
    -v "$PWD/linera-bridge/src/solidity:/contracts" \
    -v "$OUT:/shared" \
    -w /contracts \
    foundry-jq "$*"
}

# linera client. Wallet/keystore/storage persist in $OUT/wallet across calls.
# Wasm modules are reachable under /repo.
docker-linera() {
  docker run --rm \
    -e LINERA_WALLET=/data/wallet.json \
    -e LINERA_KEYSTORE=/data/keystore.json \
    -e LINERA_STORAGE=rocksdb:/data/client.db \
    -v "$OUT/wallet:/data" \
    -v "$PWD:/repo" \
    linera-test /linera "$@"
}

# linera-bridge CLI (its image entrypoint is the relayer; override it).
docker-linera-bridge() {
  docker run --rm \
    -v "$OUT:/shared" \
    --entrypoint linera-bridge \
    linera-bridge "$@"
}

# Extract a deployed address from a forge broadcast file by contract name:
#   deployed_addr <ScriptFile.s.sol> <ContractName>
deployed_addr() {
  docker-foundry "jq -r '.transactions[]|select(.contractName==\"$2\")|.contractAddress' broadcast/$1/$EVM_CHAIN_ID/run-latest.json"
}

# Convert a 0x EVM address to the JSON byte array the Linera apps expect.
addr_to_bytes() {
  local h=${1#0x} out="" i
  for (( i=0; i<${#h}; i+=2 )); do out="$out,$(( 16#${h:$i:2} ))"; done
  echo "[${out#,}]"
}
```

Detect the EVM chain id (used by later steps and the broadcast paths):

```bash
export EVM_CHAIN_ID=$(docker-foundry 'cast chain-id --rpc-url $RPC_URL')
echo "EVM chain id: $EVM_CHAIN_ID"
```

---

## 1. Committee args (Linera → JSON for LightClient)

Query the faucet and write the `LightClient` genesis constructor args (validator
addresses, weights, admin chain, epoch, and the pause-guardian/proposer roles)
to `$OUT/light-client-args.json`:

```bash
docker-linera-bridge init-light-client \
  --faucet-url "$FAUCET_URL" \
  --output /shared/light-client-args.json \
  --pause-guardian "$PAUSE_GUARDIAN" \
  --proposer "$PROPOSER"
```

## 2. Deploy LightClient (EVM)

```bash
export LIGHT_CLIENT_ARGS_JSON_FILE=/shared/light-client-args.json
docker-foundry 'forge script script/DeployLightClient.s.sol \
  --rpc-url $RPC_URL --private-key $EVM_PRIVATE_KEY --broadcast'

export LIGHT_CLIENT_ADDRESS=$(deployed_addr DeployLightClient.s.sol LightClient)
echo "LightClient: $LIGHT_CLIENT_ADDRESS"
```

To verify the contract on a block explorer, append
`--verify --etherscan-api-key $KEY --verifier-url $URL` to the `forge` command
(see [Per-network notes](#per-network-notes)).

## 3. Resolve the ERC-20 token (EVM)

**Reuse an existing ERC-20** (the common case for mainnet) — just set it:

```bash
export TOKEN_ADDRESS=0x...   # existing ERC-20; TOKEN_DECIMALS above must match it
```

**…or deploy a fresh `LineraToken`:**

```bash
export TOKEN_NAME=LineraToken TOKEN_SYMBOL=LIN TOKEN_DECIMALS=18 \
       TOKEN_SUPPLY=1000000000000000000000
docker-foundry 'forge script script/DeployLineraToken.s.sol \
  --rpc-url $RPC_URL --private-key $EVM_PRIVATE_KEY --broadcast'

export TOKEN_ADDRESS=$(deployed_addr DeployLineraToken.s.sol LineraToken)
echo "Token: $TOKEN_ADDRESS"
```

## 4. Claim the bridge chain (Linera)

```bash
docker-linera wallet init --faucet "$FAUCET_URL"
docker-linera wallet request-chain --faucet "$FAUCET_URL" --set-default
```

`--set-default` makes the new chain the wallet's default, so the
`publish-and-create` steps below (which have no `--chain-id` flag) target it.

From the `request-chain` output, copy the 64-hex **chain id** (first line) and
the `0x…` **owner** (second line) into:

```bash
export BRIDGE_CHAIN_ID=<64-hex chain id>
export BRIDGE_CHAIN_OWNER=0x<64-hex owner>
```

## 5. Publish the wrapped-fungible app (Linera)

```bash
docker-linera sync || true
docker-linera process-inbox || true

export WRAPPED_PARAMS="{\"ticker_symbol\":\"$TICKER_SYMBOL\",\"decimals\":$TOKEN_DECIMALS,\"mint_chain_id\":\"$BRIDGE_CHAIN_ID\",\"evm_token_address\":$(addr_to_bytes $TOKEN_ADDRESS),\"evm_source_chain_id\":$EVM_CHAIN_ID}"

docker-linera publish-and-create \
  /repo/examples/target/wasm32-unknown-unknown/release/wrapped_fungible_contract.wasm \
  /repo/examples/target/wasm32-unknown-unknown/release/wrapped_fungible_service.wasm \
  --json-parameters "$WRAPPED_PARAMS" \
  --json-argument '{"accounts":{}}'
```

Copy the printed 64-hex application id:

```bash
export WRAPPED_APP_ID=<64-hex app id>
```

## 6. Publish the evm-bridge app (Linera)

```bash
docker-linera sync || true
docker-linera process-inbox || true

export BRIDGE_PARAMS="{\"source_chain_id\":$EVM_CHAIN_ID,\"token_address\":$(addr_to_bytes $TOKEN_ADDRESS),\"bridge_chain_id\":\"$BRIDGE_CHAIN_ID\",\"fungible_app_id\":\"$WRAPPED_APP_ID\"}"
export BRIDGE_ARG="{\"rpc_endpoint\":\"$EVM_RPC_ENDPOINT_FOR_LINERA\"}"

docker-linera publish-and-create \
  /repo/linera-bridge/contracts/evm-bridge/target/wasm32-unknown-unknown/release/evm_bridge_contract.wasm \
  /repo/linera-bridge/contracts/evm-bridge/target/wasm32-unknown-unknown/release/evm_bridge_service.wasm \
  --json-parameters "$BRIDGE_PARAMS" \
  --json-argument "$BRIDGE_ARG" \
  --required-application-ids "$WRAPPED_APP_ID"
```

Copy the printed 64-hex application id:

```bash
export BRIDGE_APP_ID=<64-hex app id>
```

## 7. Deploy FungibleBridge (EVM)

This is the one step that doesn't use the `docker-foundry` wrapper: the
constructor inputs are passed as explicit `-e` env (and the app IDs / chain id
need a `0x` prefix that the Linera-side values don't carry):

```bash
docker run --rm \
  -v "$PWD/linera-bridge/src/solidity:/contracts" -w /contracts \
  -e RPC_URL -e EVM_PRIVATE_KEY \
  -e LIGHT_CLIENT="$LIGHT_CLIENT_ADDRESS" \
  -e TOKEN_ADDRESS="$TOKEN_ADDRESS" \
  -e BRIDGE_CHAIN_ID="0x$BRIDGE_CHAIN_ID" \
  -e FUNGIBLE_APP_ID="0x$WRAPPED_APP_ID" \
  -e BRIDGE_APP_ID="0x$BRIDGE_APP_ID" \
  -e PAUSE_GUARDIAN -e PROPOSER -e CANCELLER -e TIMELOCK_DELAY \
  foundry-jq 'forge script script/DeployFungibleBridge.s.sol \
    --rpc-url $RPC_URL --private-key $EVM_PRIVATE_KEY --broadcast'

export BRIDGE_ADDRESS=$(deployed_addr DeployFungibleBridge.s.sol FungibleBridge)
echo "FungibleBridge: $BRIDGE_ADDRESS"
```

## 8. Cross-register on both sides (Linera)

Authorize the bridge on the wrapped app, then record the FungibleBridge address
in the evm-bridge app. Both operations run on the bridge chain. (The operation
bytes are the BCS variant tag followed by the payload: `08` +
`evm-bridge app id` for `RegisterAuthorizedCaller`; `02` + 20-byte EVM address
for `RegisterFungibleBridge`.)

```bash
# wrapped-fungible: RegisterAuthorizedCaller(evm-bridge app id)
docker-linera execute-operation \
  --application-id "$WRAPPED_APP_ID" \
  --operation "08$BRIDGE_APP_ID" \
  --chain-id "$BRIDGE_CHAIN_ID"

# evm-bridge: RegisterFungibleBridge(FungibleBridge address)
export BRIDGE_ADDR_HEX=$(echo "${BRIDGE_ADDRESS#0x}" | tr 'A-Z' 'a-z')
docker-linera execute-operation \
  --application-id "$BRIDGE_APP_ID" \
  --operation "02$BRIDGE_ADDR_HEX" \
  --chain-id "$BRIDGE_CHAIN_ID"
```

These registrations are **set-once**; re-running them against an already-registered
bridge fails at the app level — expected.

## 9. (Optional) Fund the bridge with ERC-20 liquidity

The bridge releases ERC-20 from its own balance, so it must hold tokens. For a
fresh `LineraToken` you can seed it from the deployer; on mainnet you typically
fund out-of-band with real liquidity instead.

```bash
export FUND_AMOUNT=0   # ERC-20 base units to send; 0 = skip
docker run --rm \
  -e RPC_URL -e EVM_PRIVATE_KEY foundry-jq \
  "cast send $TOKEN_ADDRESS 'transfer(address,uint256)' $BRIDGE_ADDRESS $FUND_AMOUNT \
     --rpc-url \$RPC_URL --private-key \$EVM_PRIVATE_KEY"
```

## 10. Emit the relayer env file

```bash
# Deploy block seeds the relayer's start block (hex → decimal).
RAW_BLOCK=$(docker-foundry "jq -r '.receipts[0].blockNumber' broadcast/DeployFungibleBridge.s.sol/$EVM_CHAIN_ID/run-latest.json")
export MONITOR_START_BLOCK=$(( RAW_BLOCK ))

cat > "$OUT/relayer.env" <<EOF
# Relayer env for the '$NETWORK' bridge deployment.
# Wallet paths are CONTAINER paths: bind-mount the host $OUT/wallet at /data.
# EVM_PRIVATE_KEY is intentionally NOT written here — supply it separately.
RPC_URL=$RPC_URL
FAUCET_URL=$FAUCET_URL
EVM_BRIDGE_ADDRESS=$BRIDGE_ADDRESS
LINERA_BRIDGE_APP=$BRIDGE_APP_ID
LINERA_FUNGIBLE_APP=$WRAPPED_APP_ID
LINERA_BRIDGE_CHAIN_ID=$BRIDGE_CHAIN_ID
LINERA_BRIDGE_CHAIN_OWNER=$BRIDGE_CHAIN_OWNER
LINERA_WALLET=/data/wallet.json
LINERA_KEYSTORE=/data/keystore.json
LINERA_STORAGE=rocksdb:/data/client.db
MONITOR_SCAN_INTERVAL=30
MONITOR_START_BLOCK=$MONITOR_START_BLOCK
MAX_RETRIES=10
PORT=3001
EOF

echo "Wrote $OUT/relayer.env"
cat "$OUT/relayer.env"
```

## Deployment summary

| Output | Where |
|--------|-------|
| `LightClient` (EVM) | `$LIGHT_CLIENT_ADDRESS` |
| `FungibleBridge` (EVM) | `$BRIDGE_ADDRESS` |
| Token (EVM) | `$TOKEN_ADDRESS` |
| `evm-bridge` (Linera) | `$BRIDGE_APP_ID` |
| `wrapped-fungible` (Linera) | `$WRAPPED_APP_ID` |
| Bridge chain id / owner | `$BRIDGE_CHAIN_ID` / `$BRIDGE_CHAIN_OWNER` |
| Bridge-chain wallet | `$OUT/wallet/` — **back this up** |
| Relayer env | `$OUT/relayer.env` |

## Handing off to the relayer

`relayer.env` matches the format the relayer runbook consumes. The wallet paths
inside it are **container** paths (`/data/...`); bind-mount the host
`$OUT/wallet/` dir at `/data`. `EVM_PRIVATE_KEY` is deliberately omitted — supply
it via a separate secret file. See
[`docker/README.testnet.md`](../../docker/README.testnet.md) for running the relayer.

## Governance roles

- **`PAUSE_GUARDIAN`** — can emergency-pause block registration on the
  `LightClient`/bridge; a governance role that cannot move funds.
- **`PROPOSER`** — gates committee-epoch maintenance (`expireEpochsBelow`) on the
  `LightClient`, and proposes timelocked changes on the `FungibleBridge`.
- **`CANCELLER`** — can cancel a pending timelocked change on the `FungibleBridge`
  (must differ from the proposer).
- **`TIMELOCK_DELAY`** — seconds a queued `FungibleBridge` governance change must
  wait before execution.

Use addresses you control (ideally multisigs). They are baked into the contracts
at deploy time.

## Per-network notes

| Network | EVM RPC | Explorer verifier URL |
|---------|---------|-----------------------|
| Base Sepolia | `https://sepolia.base.org` | `https://api-sepolia.basescan.org/api` |
| Ethereum Sepolia | a Sepolia RPC | `https://api-sepolia.etherscan.io/api` |
| Base / Ethereum mainnet | a mainnet RPC | basescan/etherscan `…/api` |

For mainnet you almost always reuse an existing ERC-20 (step 3, first option)
and fund the bridge with real liquidity out-of-band (skip step 9). To verify
contracts, append `--verify --etherscan-api-key <KEY> --verifier-url <URL>` to
the `forge` commands.

### HTTP allow-list

The `evm-bridge` app verifies EVM deposit finality by calling the configured RPC
endpoint (`EVM_RPC_ENDPOINT_FOR_LINERA`) **from inside the Linera validators**.
That hostname must be on the validators' HTTP request allow-list, or deposits
fail with `UnauthorizedHttpRequest`. Coordinate with the network operators before
going live.

## Security notes

- **Deployer is trusted at genesis.** `LightClient`'s constructor takes the
  genesis committee on faith; thereafter committee changes require a certified
  `CreateCommittee`. Generate the args from a faucet you trust.
- **Registrations are set-once** (step 8) and access-controlled — they cannot be
  front-run without the chain owner (Linera) or deployer (EVM) key.
- **Protect `$OUT/`.** It holds the bridge-chain wallet. Losing the wallet means
  losing control of the bridge chain (no recovery — re-provisioning is a fresh
  bridge). It is gitignored; back it up securely.
- **Keys never land on disk via this runbook.** `EVM_PRIVATE_KEY` is forwarded to
  containers from your shell env and is never written to `relayer.env`. Keeping
  it out of shell history is your responsibility (e.g. `read -rs EVM_PRIVATE_KEY`).

## Troubleshooting

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| `/shared` is empty in a container | `$OUT` not under a Docker-shared path | keep `$OUT` inside the repo (not `/tmp`) |
| `cannot reach EVM RPC` | bad `RPC_URL` / network | `docker-foundry 'cast chain-id --rpc-url $RPC_URL'` |
| `forge` cannot read the args file | committee args not at `/shared` | re-run step 1; `LIGHT_CLIENT_ARGS_JSON_FILE=/shared/light-client-args.json` |
| `vm.readFile` permission denied | path outside `fs_permissions` | the args file must be under `/shared` (allow-listed in `foundry.toml`) |
| `publish-and-create` fails / no Wasm | modules not built | `make -C linera-bridge build-wasm`; check the `ls` in Prerequisites |
| `deployed_addr` prints empty | wrong `$EVM_CHAIN_ID` or contract name | confirm `$EVM_CHAIN_ID`; the broadcast dir is `broadcast/<script>/<chainid>/` |
| Deposits fail `UnauthorizedHttpRequest` | RPC host not allow-listed | see [HTTP allow-list](#http-allow-list) |
