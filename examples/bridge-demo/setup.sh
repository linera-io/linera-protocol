#!/usr/bin/env bash
# Setup script for the EVM↔Linera bridge demo.
#
# Two modes:
#   Docker mode:  ./setup.sh --compose-file ../../docker/docker-compose.bridge-test.yml
#   Direct mode:  ./setup.sh --evm-rpc-url URL --evm-private-key KEY \
#                   --light-client-address ADDR --linera-bridge-chain-id ID
#
# Docker mode runs forge/cast/linera inside Docker containers (local dev).
# Direct mode calls them directly on the host (real network deployments).
#
# Outputs: .env.local with all required environment variables for the frontend.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── Defaults ──
COMPOSE_FILE=""
PROJECT_NAME="linera-bridge-demo"
EVM_RPC_URL=""
EVM_PRIVATE_KEY=""
EVM_CHAIN_ID=31337
LIGHT_CLIENT_ADDR=""
BRIDGE_CHAIN_ID=""
TOKEN_ADDRESS=""
WASM_DIR=""
CONTRACTS_DIR=""
OUTPUT_FILE=""
FAUCET_URL=""
RELAY_URL=""
RELAY_OWNER=""
TICKER_SYMBOL="wTT"
FUND_AMOUNT="500000000000000000000"
SHARED_DIR=""
WALLET_DIR="/tmp/wallet"
EXTRA_WALLET_ID=1
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LINERA_BIN=""
LINERA_BRIDGE_BIN=""
LINERA_WALLET_PATH=""
LINERA_KEYSTORE_PATH=""
LINERA_STORAGE_PATH=""

die() { echo "ERROR: $*" >&2; exit 1; }

validate_eth_address() {
    local name="$1" addr="$2"
    echo "$addr" | grep -qiE '^0x[0-9a-fA-F]{40}$' \
        || die "$name is not a valid Ethereum address: $addr"
}

validate_hex64() {
    local name="$1" val="$2"
    echo "$val" | grep -qE '^[a-f0-9]{64}$' \
        || die "$name is not a valid 64-char hex ID: $val"
}

# Normalize a hex ID: strip optional 0x prefix and lowercase.
normalize_hex() {
    echo "$1" | sed 's/^0x//' | tr '[:upper:]' '[:lower:]'
}

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Docker mode (local dev):
  $(basename "$0") --compose-file ../../docker/docker-compose.bridge-test.yml

Direct mode (real networks):
  $(basename "$0") --evm-rpc-url URL --evm-private-key KEY \\
    --linera-bridge-chain-id ID --relay-owner OWNER --faucet-url URL

Options:
  --compose-file PATH       Docker Compose file (enables Docker mode)
  --evm-rpc-url URL         EVM JSON-RPC endpoint
                            Docker default: http://anvil:8545
                            Direct default: http://localhost:8545
  --evm-private-key KEY     Private key for EVM transactions
                            Docker default: Anvil account 0
  --evm-chain-id ID         EVM chain ID (default: 31337)
  --light-client-address ADDR  LightClient contract address (skip deploy)
                            Docker mode reads from /shared/
                            Direct mode deploys if not provided
  --linera-bridge-chain-id ID      Linera bridge chain ID (64 hex chars)
                            Docker mode polls /shared/
  --token-address ADDR      ERC20 token address (skip MockERC20 deploy)
  --wasm-dir PATH           Directory with .wasm binaries
                            Docker default: /wasm
                            Direct default: ../../examples/target/wasm32-unknown-unknown/release
  --contracts-dir PATH      Solidity source root for forge --root
                            Docker default: /contracts
                            Direct default: ../../linera-bridge/src/solidity
  --output PATH             Output env file (default: .env.local in script dir)
  --faucet-url URL          Linera faucet URL (default: http://localhost:8080)
  --relay-url URL           Relay service URL (default: http://localhost:3001)
  --relay-owner OWNER       Relay's AccountOwner (minter for wrapped-fungible)
                            Docker mode reads from /shared/
  --ticker-symbol SYM       Wrapped token ticker (default: wTT)
  --fund-amount WEI         Fund bridge with this many tokens; 0 to skip
                            (default: 500000000000000000000)
  --shared-dir PATH         Directory for shared state files (bridge-address,
                            app IDs). Default: /tmp/bridge-demo-<timestamp>
  --linera-wallet PATH      Path to existing Linera wallet.json
  --linera-keystore PATH    Path to existing Linera keystore.json
  --linera-storage CONFIG   Linera storage config (e.g. rocksdb:/path/to/client.db)
  --help                    Show this help
EOF
    exit 0
}

# ── Parse flags ──
while [[ $# -gt 0 ]]; do
    case "$1" in
        --compose-file)      COMPOSE_FILE="$2"; shift 2 ;;
        --evm-rpc-url)       EVM_RPC_URL="$2"; shift 2 ;;
        --evm-private-key)   EVM_PRIVATE_KEY="$2"; shift 2 ;;
        --evm-chain-id)      EVM_CHAIN_ID="$2"; shift 2 ;;
        --light-client-address) LIGHT_CLIENT_ADDR="$2"; shift 2 ;;
        --linera-bridge-chain-id)   BRIDGE_CHAIN_ID="$2"; shift 2 ;;
        --token-address)     TOKEN_ADDRESS="$2"; shift 2 ;;
        --wasm-dir)          WASM_DIR="$2"; shift 2 ;;
        --contracts-dir)     CONTRACTS_DIR="$2"; shift 2 ;;
        --output)            OUTPUT_FILE="$2"; shift 2 ;;
        --faucet-url)        FAUCET_URL="$2"; shift 2 ;;
        --relay-url)         RELAY_URL="$2"; shift 2 ;;
        --relay-owner)       RELAY_OWNER="$2"; shift 2 ;;
        --ticker-symbol)     TICKER_SYMBOL="$2"; shift 2 ;;
        --fund-amount)       FUND_AMOUNT="$2"; shift 2 ;;
        --shared-dir)        SHARED_DIR="$2"; shift 2 ;;
        --linera-wallet)     LINERA_WALLET_PATH="$2"; shift 2 ;;
        --linera-keystore)   LINERA_KEYSTORE_PATH="$2"; shift 2 ;;
        --linera-storage)    LINERA_STORAGE_PATH="$2"; shift 2 ;;
        --help)              usage ;;
        *) die "Unknown option: $1" ;;
    esac
done

# Also accept COMPOSE_FILE from env var.
COMPOSE_FILE="${COMPOSE_FILE:-${BRIDGE_COMPOSE_FILE:-}}"

# ── Docker helpers ──
dc() {
    docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" "$@"
}

dc_exec() {
    dc exec -T "$@"
}

# ── Command execution abstraction ──
evm_exec() {
    if [[ -n "$COMPOSE_FILE" ]]; then
        dc_exec foundry-tools "$@"
    else
        "$@"
    fi
}

linera_exec() {
    if [[ -n "$COMPOSE_FILE" ]]; then
        dc_exec linera-network env \
            LINERA_WALLET="$WALLET_DIR/wallet_${EXTRA_WALLET_ID}.json" \
            LINERA_KEYSTORE="$WALLET_DIR/keystore_${EXTRA_WALLET_ID}.json" \
            LINERA_STORAGE="rocksdb:$WALLET_DIR/client_${EXTRA_WALLET_ID}.db" \
            ./linera "$@"
    else
        LINERA_WALLET="$LINERA_WALLET_PATH" \
        LINERA_KEYSTORE="$LINERA_KEYSTORE_PATH" \
        LINERA_STORAGE="$LINERA_STORAGE_PATH" \
        "$LINERA_BIN" "$@"
    fi
}

# Parse "Deployed to: 0x..." from forge create output.
parse_address() {
    grep 'Deployed to:' | sed 's/.*Deployed to: //' | tr -d '[:space:]'
}

# Parse "Transaction hash: 0x..." from forge create output.
parse_tx_hash() {
    grep 'Transaction hash:' | sed 's/.*Transaction hash: //' | tr -d '[:space:]'
}

# Wait for a transaction to be mined. Needed on public testnets where forge
# create returns before the tx is confirmed, causing nonce races.
wait_for_tx() {
    local tx_hash="$1"
    if [[ -z "$tx_hash" || "$tx_hash" == "0x" ]]; then
        return 0
    fi
    echo "  Waiting for tx $tx_hash..."
    evm_exec cast receipt --confirmations 1 \
        --rpc-url "$EVM_RPC_URL" "$tx_hash" >/dev/null
}

# ── Mode detection & defaults ──
if [[ -n "$COMPOSE_FILE" ]]; then
    echo "Mode: Docker (compose file: $COMPOSE_FILE)"
    EVM_RPC_URL="${EVM_RPC_URL:-http://anvil:8545}"
    EVM_PRIVATE_KEY="${EVM_PRIVATE_KEY:-0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80}"
    WASM_DIR="${WASM_DIR:-/wasm}"
    CONTRACTS_DIR="${CONTRACTS_DIR:-/contracts}"
    OUTPUT_FILE="${OUTPUT_FILE:-$SCRIPT_DIR/.env.local}"
    FAUCET_URL="${FAUCET_URL:-http://localhost:8080}"
    RELAY_URL="${RELAY_URL:-http://localhost:3001}"
    # Docker container has its own solc; don't force a version download.
    FORGE_USE_SOLC=()
else
    echo "Mode: Direct"
    [[ -z "$EVM_PRIVATE_KEY" ]] && die "--evm-private-key is required in direct mode"
    [[ -z "$BRIDGE_CHAIN_ID" ]] && die "--linera-bridge-chain-id is required in direct mode"
    EVM_RPC_URL="${EVM_RPC_URL:-http://localhost:8545}"
    WASM_DIR="${WASM_DIR:-$SCRIPT_DIR/../../examples/target/wasm32-unknown-unknown/release}"
    CONTRACTS_DIR="${CONTRACTS_DIR:-$SCRIPT_DIR/../../linera-bridge/src/solidity}"
    OUTPUT_FILE="${OUTPUT_FILE:-$SCRIPT_DIR/.env.local}"
    FAUCET_URL="${FAUCET_URL:-http://localhost:8080}"
    RELAY_URL="${RELAY_URL:-http://localhost:3001}"
    FORGE_USE_SOLC=(--use 0.8.33)

    # Resolve binaries: prefer target/debug, then target/release, then PATH.
    if [[ -z "$LINERA_BIN" ]]; then
        if [[ -x "$REPO_ROOT/target/debug/linera" ]]; then
            LINERA_BIN="$REPO_ROOT/target/debug/linera"
        elif [[ -x "$REPO_ROOT/target/release/linera" ]]; then
            LINERA_BIN="$REPO_ROOT/target/release/linera"
        else
            LINERA_BIN="$(command -v linera 2>/dev/null || true)"
            [[ -z "$LINERA_BIN" ]] && die "linera binary not found — build with 'cargo build -p linera-service' or add to PATH"
        fi
    fi
    if [[ -z "$LINERA_BRIDGE_BIN" ]]; then
        if [[ -x "$REPO_ROOT/target/debug/linera-bridge" ]]; then
            LINERA_BRIDGE_BIN="$REPO_ROOT/target/debug/linera-bridge"
        elif [[ -x "$REPO_ROOT/target/release/linera-bridge" ]]; then
            LINERA_BRIDGE_BIN="$REPO_ROOT/target/release/linera-bridge"
        else
            LINERA_BRIDGE_BIN="$(command -v linera-bridge 2>/dev/null || true)"
            [[ -z "$LINERA_BRIDGE_BIN" ]] && die "linera-bridge binary not found — build with 'cargo build -p linera-bridge --features cli' or add to PATH"
        fi
    fi
    echo "  linera:        $LINERA_BIN"
    echo "  linera-bridge: $LINERA_BRIDGE_BIN"
fi

# ── Shared dir for relay coordination ──
if [[ -z "$SHARED_DIR" ]]; then
    SHARED_DIR="/tmp/bridge-demo-$(date +%Y%m%d-%H%M%S)"
fi
mkdir -p "$SHARED_DIR"
echo "  Shared dir: $SHARED_DIR"

echo "=== Bridge Demo Setup ==="

# ── 0. Resolve Linera wallet paths (direct mode only) ──
# Resolve env var fallbacks early so we can print them.
if [[ -z "$COMPOSE_FILE" ]]; then
    LINERA_WALLET_PATH="${LINERA_WALLET_PATH:-${LINERA_WALLET:-}}"
    LINERA_KEYSTORE_PATH="${LINERA_KEYSTORE_PATH:-${LINERA_KEYSTORE:-}}"
    LINERA_STORAGE_PATH="${LINERA_STORAGE_PATH:-${LINERA_STORAGE:-}}"
    if [[ -n "$LINERA_WALLET_PATH" ]]; then
        # Use caller-provided wallet paths.
        [[ -z "$LINERA_KEYSTORE_PATH" ]] && die "--linera-keystore is required when --linera-wallet is set"
        [[ -z "$LINERA_STORAGE_PATH" ]] && die "--linera-storage is required when --linera-wallet is set"
        echo "  Using wallet at $LINERA_WALLET_PATH"
    else
        # Create a temporary wallet.
        LINERA_TMP_DIR="$SHARED_DIR/linera-wallet"
        mkdir -p "$LINERA_TMP_DIR"
        LINERA_WALLET_PATH="$LINERA_TMP_DIR/wallet.json"
        LINERA_KEYSTORE_PATH="$LINERA_TMP_DIR/keystore.json"
        LINERA_STORAGE_PATH="rocksdb:$LINERA_TMP_DIR/client.db"
        if [[ ! -f "$LINERA_WALLET_PATH" ]]; then
            echo "Initializing Linera wallet from faucet..."
            linera_exec wallet init --faucet "$FAUCET_URL"
        else
            echo "  Using existing wallet at $LINERA_TMP_DIR"
        fi
    fi
fi

echo ""
echo "Configuration:"
echo "  EVM RPC URL:        $EVM_RPC_URL"
echo "  EVM chain ID:       $EVM_CHAIN_ID"
echo "  Bridge chain ID:    ${BRIDGE_CHAIN_ID:-(will be read from /shared/)}"
echo "  Relay owner:        ${RELAY_OWNER:-(will be read from /shared/)}"
echo "  Faucet URL:         $FAUCET_URL"
echo "  Shared dir:         $SHARED_DIR"
if [[ -z "$COMPOSE_FILE" ]]; then
echo "  Linera wallet:      $LINERA_WALLET_PATH"
echo "  Linera keystore:    $LINERA_KEYSTORE_PATH"
echo "  Linera storage:     $LINERA_STORAGE_PATH"
fi
echo ""

# ── 1. Deploy or read LightClient ──
if [[ -n "$COMPOSE_FILE" && -z "$LIGHT_CLIENT_ADDR" ]]; then
    echo "Reading LightClient address from /shared/..."
    LIGHT_CLIENT_ADDR=$(dc_exec foundry-tools cat /shared/light-client-address | tr -d '[:space:]')
elif [[ -z "$LIGHT_CLIENT_ADDR" ]]; then
    echo "Fetching LightClient constructor args from faucet..."
    LC_ARGS_JSON=$("$LINERA_BRIDGE_BIN" init-light-client --faucet-url "$FAUCET_URL" --output /dev/null)
    LC_VALIDATORS=$(echo "$LC_ARGS_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('[' + ','.join(d['validators']) + ']')
")
    LC_WEIGHTS=$(echo "$LC_ARGS_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('[' + ','.join(str(w) for w in d['weights']) + ']')
")
    LC_ADMIN_CHAIN=$(echo "$LC_ARGS_JSON" | python3 -c "
import sys, json; print(json.load(sys.stdin)['admin_chain_id'])
")
    LC_EPOCH=$(echo "$LC_ARGS_JSON" | python3 -c "
import sys, json; print(json.load(sys.stdin)['epoch'])
")
    echo "  Deploying LightClient (epoch=$LC_EPOCH)..."
    LC_OUTPUT=$(evm_exec \
        forge create "LightClient.sol:LightClient" \
        --root "$CONTRACTS_DIR" --via-ir --optimize --optimizer-runs 1 \
        "${FORGE_USE_SOLC[@]}" --evm-version shanghai \
        --out /tmp/forge-out --cache-path /tmp/forge-cache \
        --rpc-url "$EVM_RPC_URL" \
        --private-key "$EVM_PRIVATE_KEY" \
        --broadcast \
        --constructor-args \
        "$LC_VALIDATORS" "$LC_WEIGHTS" "$LC_ADMIN_CHAIN" "$LC_EPOCH")
    LIGHT_CLIENT_ADDR=$(echo "$LC_OUTPUT" | parse_address)
    wait_for_tx "$(echo "$LC_OUTPUT" | parse_tx_hash)"
fi
validate_eth_address "LightClient address" "$LIGHT_CLIENT_ADDR"
echo "  LightClient: $LIGHT_CLIENT_ADDR"

# ── 2. Read bridge chain ID (Docker mode only) ──
if [[ -n "$COMPOSE_FILE" && -z "$BRIDGE_CHAIN_ID" ]]; then
    echo "Waiting for bridge chain ID..."
    for i in $(seq 1 30); do
        BRIDGE_CHAIN_ID=$(dc_exec foundry-tools cat /shared/bridge-chain-id 2>/dev/null | tr -d '[:space:]')
        BRIDGE_CHAIN_ID=$(normalize_hex "$BRIDGE_CHAIN_ID")
        if echo "$BRIDGE_CHAIN_ID" | grep -qE '^[a-f0-9]{64}$'; then
            break
        fi
        BRIDGE_CHAIN_ID=""
        echo "  Waiting... ($i/30)"
        sleep 2
    done
    if [[ -z "$BRIDGE_CHAIN_ID" ]]; then
        die "Bridge chain ID not found within timeout"
    fi
else
    BRIDGE_CHAIN_ID=$(normalize_hex "$BRIDGE_CHAIN_ID")
fi
validate_hex64 "Bridge chain ID" "$BRIDGE_CHAIN_ID"
echo "  Bridge chain: $BRIDGE_CHAIN_ID"

# ── 2b. Create extra wallet copy (Docker mode only) ──
if [[ -n "$COMPOSE_FILE" ]]; then
    echo "Creating extra wallet copy..."
    dc_exec linera-network sh -c "\
        cp $WALLET_DIR/wallet_0.json $WALLET_DIR/wallet_${EXTRA_WALLET_ID}.json && \
        cp $WALLET_DIR/keystore_0.json $WALLET_DIR/keystore_${EXTRA_WALLET_ID}.json && \
        cp -r $WALLET_DIR/client_0.db $WALLET_DIR/client_${EXTRA_WALLET_ID}.db"
fi

# ── 3. Deploy MockERC20 (skip if --token-address was provided) ──
if [[ -z "$TOKEN_ADDRESS" ]]; then
    echo "Deploying MockERC20..."
    ERC20_OUTPUT=$(evm_exec \
        forge create "$CONTRACTS_DIR/MockERC20.sol:MockERC20" \
        --root "$CONTRACTS_DIR" --via-ir --optimize --optimizer-runs 1 \
        "${FORGE_USE_SOLC[@]}" --evm-version shanghai \
        --out /tmp/forge-out --cache-path /tmp/forge-cache \
        --rpc-url "$EVM_RPC_URL" \
        --private-key "$EVM_PRIVATE_KEY" \
        --broadcast \
        --constructor-args "TestToken" "TT" 1000000000000000000000)
    TOKEN_ADDRESS=$(echo "$ERC20_OUTPUT" | parse_address)
    wait_for_tx "$(echo "$ERC20_OUTPUT" | parse_tx_hash)"
    echo "  MockERC20: $TOKEN_ADDRESS"
else
    echo "  Using existing token: $TOKEN_ADDRESS"
fi
validate_eth_address "Token address" "$TOKEN_ADDRESS"
TOKEN_ADDR_HEX=$(echo "$TOKEN_ADDRESS" | sed 's/^0x//')

# ── 3b. Read relay owner (minter for wrapped-fungible) ──
echo "Reading relay owner..."
if [[ -n "$COMPOSE_FILE" ]]; then
    for i in $(seq 1 30); do
        RELAY_OWNER=$(dc_exec foundry-tools cat /shared/relay-owner 2>/dev/null | tr -d '[:space:]')
        if [[ -n "$RELAY_OWNER" ]]; then
            break
        fi
        echo "  Waiting for relay owner... ($i/30)"
        sleep 2
    done
    [[ -z "$RELAY_OWNER" ]] && die "Relay owner not found within timeout"
else
    [[ -z "$RELAY_OWNER" ]] && die "--relay-owner is required in direct mode"
fi
echo "  Relay owner (minter): $RELAY_OWNER"

# ── 4. Deploy FungibleBridge on EVM ──
echo "Deploying FungibleBridge..."
CHAIN_BYTES32="0x${BRIDGE_CHAIN_ID}"

BRIDGE_OUTPUT=$(evm_exec \
    forge create "$CONTRACTS_DIR/FungibleBridge.sol:FungibleBridge" \
    --root "$CONTRACTS_DIR" --via-ir --optimize --optimizer-runs 1 \
    "${FORGE_USE_SOLC[@]}" --ignored-error-codes 6321 \
    --evm-version shanghai \
    --out /tmp/forge-out --cache-path /tmp/forge-cache \
    --rpc-url "$EVM_RPC_URL" \
    --private-key "$EVM_PRIVATE_KEY" \
    --broadcast \
    --constructor-args \
    "$LIGHT_CLIENT_ADDR" \
    "$CHAIN_BYTES32" \
    "$TOKEN_ADDRESS")
BRIDGE_ADDRESS=$(echo "$BRIDGE_OUTPUT" | parse_address)
wait_for_tx "$(echo "$BRIDGE_OUTPUT" | parse_tx_hash)"
validate_eth_address "FungibleBridge address" "$BRIDGE_ADDRESS"
BRIDGE_ADDR_HEX=$(echo "$BRIDGE_ADDRESS" | sed 's/^0x//')
echo "  FungibleBridge: $BRIDGE_ADDRESS"

# Write bridge address to shared dir for relay.
if [[ -n "$COMPOSE_FILE" ]]; then
    dc_exec --user root foundry-tools sh -c "echo '$BRIDGE_ADDRESS' > /shared/bridge-address"
fi
echo "$BRIDGE_ADDRESS" > "$SHARED_DIR/bridge-address"

# ── 5. Publish and create evm-bridge app ──
echo "Syncing chain state..."
linera_exec sync 2>&1
linera_exec process-inbox 2>&1
echo "Publishing and creating evm-bridge app..."
BRIDGE_PARAMS=$(
    CHAIN_ID="$EVM_CHAIN_ID" \
    BRIDGE_HEX="$BRIDGE_ADDR_HEX" \
    TOKEN_HEX="$TOKEN_ADDR_HEX" \
    EVM_RPC_URL="$EVM_RPC_URL" \
    python3 -c "
import json, os
def hex_to_array(h):
    return [int(h[i:i+2], 16) for i in range(0, len(h), 2)]
params = {
    'source_chain_id': int(os.environ['CHAIN_ID']),
    'bridge_contract_address': hex_to_array(os.environ['BRIDGE_HEX']),
    'token_address': hex_to_array(os.environ['TOKEN_HEX']),
    'rpc_endpoint': os.environ.get('EVM_RPC_URL', ''),
}
print(json.dumps(params))
")

for attempt in 1 2 3; do
    BRIDGE_APP_OUTPUT=$(linera_exec publish-and-create \
        "$WASM_DIR/evm_bridge_contract.wasm" \
        "$WASM_DIR/evm_bridge_service.wasm" \
        --json-parameters "$BRIDGE_PARAMS" \
        --json-argument 'null' 2>&1) && break
    echo "  Attempt $attempt failed, retrying..." >&2
    sleep 2
done
[[ -z "$BRIDGE_APP_OUTPUT" ]] && { echo "ERROR: publish-and-create evm-bridge failed after retries" >&2; exit 1; }
BRIDGE_APP_ID=$(echo "$BRIDGE_APP_OUTPUT" | grep -oE '^[a-f0-9]{64}$' | tail -1)
validate_hex64 "Linera bridge app ID" "$BRIDGE_APP_ID"
echo "  Linera bridge app: $BRIDGE_APP_ID"

# Write bridge app ID to shared dir for relay.
if [[ -n "$COMPOSE_FILE" ]]; then
    dc_exec --user root foundry-tools sh -c "echo '$BRIDGE_APP_ID' > /shared/bridge-app-id"
fi
echo "$BRIDGE_APP_ID" > "$SHARED_DIR/bridge-app-id"

# ── 6. Publish and create wrapped-fungible app ──
echo "Syncing chain state..."
linera_exec sync 2>&1
linera_exec process-inbox 2>&1
echo "Publishing and creating wrapped-fungible app..."
WRAPPED_PARAMS=$(
    TICKER="$TICKER_SYMBOL" \
    MINTER="$RELAY_OWNER" \
    TOKEN_HEX="$TOKEN_ADDR_HEX" \
    CHAIN_ID="$EVM_CHAIN_ID" \
    BRIDGE_CHAIN="$BRIDGE_CHAIN_ID" \
    BRIDGE_APP="$BRIDGE_APP_ID" \
    python3 -c "
import json, os
def hex_to_array(h):
    return [int(h[i:i+2], 16) for i in range(0, len(h), 2)]
params = {
    'ticker_symbol': os.environ['TICKER'],
    'minter': os.environ['MINTER'],
    'mint_chain_id': os.environ['BRIDGE_CHAIN'],
    'evm_token_address': hex_to_array(os.environ['TOKEN_HEX']),
    'evm_source_chain_id': int(os.environ['CHAIN_ID']),
    'bridge_app_id': os.environ['BRIDGE_APP'],
}
print(json.dumps(params))
")
for attempt in 1 2 3; do
    WRAPPED_APP_OUTPUT=$(linera_exec publish-and-create \
        "$WASM_DIR/wrapped_fungible_contract.wasm" \
        "$WASM_DIR/wrapped_fungible_service.wasm" \
        --json-parameters "$WRAPPED_PARAMS" \
        --json-argument '{"accounts":{}}' 2>&1) && break
    echo "  Attempt $attempt failed, retrying..." >&2
    sleep 2
done
[[ -z "$WRAPPED_APP_OUTPUT" ]] && { echo "ERROR: publish-and-create wrapped-fungible failed after retries" >&2; exit 1; }
WRAPPED_APP_ID=$(echo "$WRAPPED_APP_OUTPUT" | grep -oE '^[a-f0-9]{64}$' | tail -1)
validate_hex64 "Wrapped-fungible app ID" "$WRAPPED_APP_ID"
echo "  Wrapped-fungible app: $WRAPPED_APP_ID"

# Write wrapped-fungible app ID to shared dir for relay.
if [[ -n "$COMPOSE_FILE" ]]; then
    dc_exec --user root foundry-tools sh -c "echo '$WRAPPED_APP_ID' > /shared/wrapped-app-id"
fi
echo "$WRAPPED_APP_ID" > "$SHARED_DIR/wrapped-app-id"

# ── 7. Register app IDs on both sides ──
echo "Registering fungible app in evm-bridge..."
# BCS encoding: variant index 0 (RegisterFungibleApp) + 32-byte app ID hash
# Must use the relay wallet since it owns the bridge chain.
REGISTER_HEX="00${WRAPPED_APP_ID}"
if [[ -n "$COMPOSE_FILE" ]]; then
    dc_exec linera-network env \
        LINERA_WALLET=/shared/relay-wallet/wallet.json \
        LINERA_KEYSTORE=/shared/relay-wallet/keystore.json \
        LINERA_STORAGE=rocksdb:/shared/relay-wallet/client.db \
        ./linera execute-operation \
        --application-id "$BRIDGE_APP_ID" \
        --operation "$REGISTER_HEX" \
        --chain-id "$BRIDGE_CHAIN_ID" 2>&1
else
    linera_exec execute-operation --application-id "$BRIDGE_APP_ID" --operation "$REGISTER_HEX" --chain-id "$BRIDGE_CHAIN_ID" 2>&1
fi

APP_ID_BYTES32="0x${WRAPPED_APP_ID:0:64}"
echo "Registering fungibleApplicationId in FungibleBridge..."
REGISTER_OUTPUT=$(evm_exec \
    cast send --rpc-url "$EVM_RPC_URL" \
    --private-key "$EVM_PRIVATE_KEY" \
    "$BRIDGE_ADDRESS" \
    'registerFungibleApplicationId(bytes32)' \
    "$APP_ID_BYTES32")
wait_for_tx "$(echo "$REGISTER_OUTPUT" | parse_tx_hash)"
echo "  App IDs registered on both sides"

# ── 8. Fund FungibleBridge with ERC20 tokens ──
if [[ "$FUND_AMOUNT" != "0" ]]; then
    echo "Funding FungibleBridge with tokens..."
    evm_exec \
        cast send --rpc-url "$EVM_RPC_URL" \
        --private-key "$EVM_PRIVATE_KEY" \
        "$TOKEN_ADDRESS" \
        'transfer(address,uint256)' \
        "$BRIDGE_ADDRESS" \
        "$FUND_AMOUNT"
else
    echo "Skipping bridge funding (--fund-amount 0)"
fi

# ── 8. Write output env file ──
echo "Writing $OUTPUT_FILE..."
cat > "$OUTPUT_FILE" << EOF
LINERA_FAUCET_URL=$FAUCET_URL
LINERA_APPLICATION_ID=$WRAPPED_APP_ID
LINERA_BRIDGE_APP_ID=$BRIDGE_APP_ID
LINERA_RELAY_URL=$RELAY_URL
LINERA_BRIDGE_ADDRESS=$BRIDGE_ADDRESS
LINERA_TOKEN_ADDRESS=$TOKEN_ADDRESS
LINERA_BRIDGE_CHAIN_ID=$BRIDGE_CHAIN_ID
LINERA_EVM_CHAIN_ID=$EVM_CHAIN_ID
EOF

# Write setup-complete marker so the relay knows it's safe to start.
if [[ -n "$COMPOSE_FILE" ]]; then
    dc_exec --user root foundry-tools sh -c "echo 'done' > /shared/setup-complete"
fi

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Addresses & IDs:"
echo "  LightClient (EVM):          $LIGHT_CLIENT_ADDR"
echo "  FungibleBridge (EVM):       $BRIDGE_ADDRESS"
echo "  MockERC20 (EVM):            $TOKEN_ADDRESS"
echo "  evm-bridge (Linera):        $BRIDGE_APP_ID"
echo "  wrapped-fungible (Linera):  $WRAPPED_APP_ID"
echo "  Bridge chain ID:            $BRIDGE_CHAIN_ID"
echo "  Relay owner:                $RELAY_OWNER"
echo "  EVM chain ID:               $EVM_CHAIN_ID"
echo ""
echo "Environment written to:       $OUTPUT_FILE"
echo "Shared state dir:             $SHARED_DIR"
echo ""
echo "Next steps:"
if [[ -n "$COMPOSE_FILE" ]]; then
echo "  Start the frontend:"
echo "     cd examples/bridge-demo"
echo "     pnpm install && pnpm dev"
else
echo "  1. Start the relay:"
echo "     linera-bridge serve \\"
echo "       --rpc-url $EVM_RPC_URL \\"
echo "       --faucet-url $FAUCET_URL \\"
echo "       --wallet $LINERA_WALLET_PATH \\"
echo "       --keystore $LINERA_KEYSTORE_PATH \\"
echo "       --storage $LINERA_STORAGE_PATH \\"
echo "       --linera-bridge-chain-id $BRIDGE_CHAIN_ID \\"
echo "       --linera-bridge-chain-owner $RELAY_OWNER \\"
echo "       --evm-bridge-address $BRIDGE_ADDRESS \\"
echo "       --linera-bridge-address $BRIDGE_APP_ID \\"
echo "       --linera-fungible-address $WRAPPED_APP_ID \\"
echo "       --evm-private-key 0x... \\"
echo "       --monitor-scan-interval 30 \\"
echo "       --max-retries 10"
echo "  2. Start the frontend:"
echo "     cd examples/bridge-demo"
echo "     pnpm install && pnpm dev"
fi
