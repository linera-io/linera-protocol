#!/usr/bin/env bash
# Setup script for the EVM↔Linera bridge demo.
#
# Two modes:
#   Docker mode:  ./setup.sh --compose-file ../../docker/docker-compose.bridge-test.yml
#   Direct mode:  ./setup.sh --evm-rpc-url URL --evm-private-key KEY \
#                   --light-client-address ADDR --bridge-chain-id ID
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
WALLET_DIR="/tmp/wallet"
EXTRA_WALLET_ID=1

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
    --light-client-address ADDR --bridge-chain-id ID

Options:
  --compose-file PATH       Docker Compose file (enables Docker mode)
  --evm-rpc-url URL         EVM JSON-RPC endpoint
                            Docker default: http://anvil:8545
                            Direct default: http://localhost:8545
  --evm-private-key KEY     Private key for EVM transactions
                            Docker default: Anvil account 0
  --evm-chain-id ID         EVM chain ID (default: 31337)
  --light-client-address ADDR  LightClient contract address
                            Docker mode reads from /shared/
  --bridge-chain-id ID      Linera bridge chain ID (64 hex chars)
                            Docker mode polls /shared/
  --token-address ADDR      ERC20 token address (skip MockERC20 deploy)
  --wasm-dir PATH           Directory with .wasm binaries
                            Docker default: /wasm
                            Direct default: ../../target/wasm32-unknown-unknown/release
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
        --bridge-chain-id)   BRIDGE_CHAIN_ID="$2"; shift 2 ;;
        --token-address)     TOKEN_ADDRESS="$2"; shift 2 ;;
        --wasm-dir)          WASM_DIR="$2"; shift 2 ;;
        --contracts-dir)     CONTRACTS_DIR="$2"; shift 2 ;;
        --output)            OUTPUT_FILE="$2"; shift 2 ;;
        --faucet-url)        FAUCET_URL="$2"; shift 2 ;;
        --relay-url)         RELAY_URL="$2"; shift 2 ;;
        --relay-owner)       RELAY_OWNER="$2"; shift 2 ;;
        --ticker-symbol)     TICKER_SYMBOL="$2"; shift 2 ;;
        --fund-amount)       FUND_AMOUNT="$2"; shift 2 ;;
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
        linera "$@"
    fi
}

# Parse "Deployed to: 0x..." from forge create output.
parse_address() {
    grep 'Deployed to:' | sed 's/.*Deployed to: //' | tr -d '[:space:]'
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
else
    echo "Mode: Direct"
    [[ -z "$EVM_PRIVATE_KEY" ]] && die "--evm-private-key is required in direct mode"
    [[ -z "$LIGHT_CLIENT_ADDR" ]] && die "--light-client-address is required in direct mode"
    [[ -z "$BRIDGE_CHAIN_ID" ]] && die "--bridge-chain-id is required in direct mode"
    EVM_RPC_URL="${EVM_RPC_URL:-http://localhost:8545}"
    WASM_DIR="${WASM_DIR:-$SCRIPT_DIR/../../target/wasm32-unknown-unknown/release}"
    CONTRACTS_DIR="${CONTRACTS_DIR:-$SCRIPT_DIR/../../linera-bridge/src/solidity}"
    OUTPUT_FILE="${OUTPUT_FILE:-$SCRIPT_DIR/.env.local}"
    FAUCET_URL="${FAUCET_URL:-http://localhost:8080}"
    RELAY_URL="${RELAY_URL:-http://localhost:3001}"
fi

echo "=== Bridge Demo Setup ==="

# ── 1. Read LightClient address (Docker mode only) ──
if [[ -n "$COMPOSE_FILE" && -z "$LIGHT_CLIENT_ADDR" ]]; then
    echo "Reading LightClient address from /shared/..."
    LIGHT_CLIENT_ADDR=$(dc_exec foundry-tools cat /shared/light-client-address | tr -d '[:space:]')
fi
validate_eth_address "LightClient address" "$LIGHT_CLIENT_ADDR"
echo "  LightClient: $LIGHT_CLIENT_ADDR"

# ── 2. Read bridge chain ID (Docker mode only) ──
if [[ -n "$COMPOSE_FILE" && -z "$BRIDGE_CHAIN_ID" ]]; then
    echo "Waiting for relay to claim bridge chain..."
    for i in $(seq 1 30); do
        BRIDGE_CHAIN_ID=$(dc_exec linera-relay cat /shared/bridge-chain-id 2>/dev/null | tr -d '[:space:]' || true)
        BRIDGE_CHAIN_ID=$(normalize_hex "$BRIDGE_CHAIN_ID")
        if echo "$BRIDGE_CHAIN_ID" | grep -qE '^[a-f0-9]{64}$'; then
            break
        fi
        BRIDGE_CHAIN_ID=""
        echo "  Waiting... ($i/30)"
        sleep 2
    done
    if [[ -z "$BRIDGE_CHAIN_ID" ]]; then
        die "Relay did not write bridge chain ID within timeout"
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
        --root "$CONTRACTS_DIR" --via-ir --optimize \
        --evm-version shanghai \
        --out /tmp/forge-out --cache-path /tmp/forge-cache \
        --rpc-url "$EVM_RPC_URL" \
        --private-key "$EVM_PRIVATE_KEY" \
        --broadcast \
        --constructor-args "TestToken" "TT" 1000000000000000000000)
    TOKEN_ADDRESS=$(echo "$ERC20_OUTPUT" | parse_address)
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
        RELAY_OWNER=$(dc_exec linera-relay cat /shared/relay-owner 2>/dev/null | tr -d '[:space:]' || true)
        if [[ -n "$RELAY_OWNER" ]]; then
            break
        fi
        echo "  Waiting for relay owner... ($i/30)"
        sleep 2
    done
    [[ -z "$RELAY_OWNER" ]] && die "Relay did not write owner within timeout"
else
    [[ -z "$RELAY_OWNER" ]] && die "--relay-owner is required in direct mode"
fi
echo "  Relay owner (minter): $RELAY_OWNER"

# ── 4. Publish and create wrapped-fungible app ──
echo "Publishing and creating wrapped-fungible app..."
WRAPPED_PARAMS=$(
    TICKER="$TICKER_SYMBOL" \
    MINTER="$RELAY_OWNER" \
    TOKEN_HEX="$TOKEN_ADDR_HEX" \
    CHAIN_ID="$EVM_CHAIN_ID" \
    BRIDGE_CHAIN="$BRIDGE_CHAIN_ID" \
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
}
print(json.dumps(params))
")
WRAPPED_APP_OUTPUT=$(linera_exec publish-and-create \
    "$WASM_DIR/wrapped_fungible_contract.wasm" \
    "$WASM_DIR/wrapped_fungible_service.wasm" \
    --json-parameters "$WRAPPED_PARAMS" \
    --json-argument '{"accounts":{}}' 2>&1) || {
    echo "ERROR: publish-and-create wrapped-fungible failed:" >&2
    echo "$WRAPPED_APP_OUTPUT" >&2
    exit 1
}
# The application ID is the last 64-hex-char token on its own line.
WRAPPED_APP_ID=$(echo "$WRAPPED_APP_OUTPUT" | grep -oE '^[a-f0-9]{64}$' | tail -1)
validate_hex64 "Wrapped-fungible app ID" "$WRAPPED_APP_ID"
echo "  Wrapped-fungible app: $WRAPPED_APP_ID"

# ── 5. Deploy FungibleBridge ──
echo "Deploying FungibleBridge..."
APP_ID_BYTES32="0x${WRAPPED_APP_ID:0:64}"
CHAIN_BYTES32="0x${BRIDGE_CHAIN_ID}"

BRIDGE_OUTPUT=$(evm_exec \
    forge create "$CONTRACTS_DIR/FungibleBridge.sol:FungibleBridge" \
    --root "$CONTRACTS_DIR" --via-ir --optimize \
    --ignored-error-codes 6321 \
    --evm-version shanghai \
    --out /tmp/forge-out --cache-path /tmp/forge-cache \
    --rpc-url "$EVM_RPC_URL" \
    --private-key "$EVM_PRIVATE_KEY" \
    --broadcast \
    --constructor-args \
    "$LIGHT_CLIENT_ADDR" \
    "$CHAIN_BYTES32" \
    0 \
    "$APP_ID_BYTES32" \
    "$TOKEN_ADDRESS")
BRIDGE_ADDRESS=$(echo "$BRIDGE_OUTPUT" | parse_address)
validate_eth_address "FungibleBridge address" "$BRIDGE_ADDRESS"
BRIDGE_ADDR_HEX=$(echo "$BRIDGE_ADDRESS" | sed 's/^0x//')
echo "  FungibleBridge: $BRIDGE_ADDRESS"

# Write bridge address to shared volume (Docker mode only).
if [[ -n "$COMPOSE_FILE" ]]; then
    dc_exec --user root foundry-tools sh -c "echo '$BRIDGE_ADDRESS' > /shared/bridge-address"
fi

# ── 6. Publish and create evm-bridge app ──
echo "Publishing and creating evm-bridge app..."
BRIDGE_PARAMS=$(
    CHAIN_ID="$EVM_CHAIN_ID" \
    BRIDGE_HEX="$BRIDGE_ADDR_HEX" \
    APP_ID="$WRAPPED_APP_ID" \
    TOKEN_HEX="$TOKEN_ADDR_HEX" \
    python3 -c "
import json, os
def hex_to_array(h):
    return [int(h[i:i+2], 16) for i in range(0, len(h), 2)]
params = {
    'source_chain_id': int(os.environ['CHAIN_ID']),
    'bridge_contract_address': hex_to_array(os.environ['BRIDGE_HEX']),
    'fungible_app_id': os.environ['APP_ID'],
    'token_address': hex_to_array(os.environ['TOKEN_HEX']),
}
print(json.dumps(params))
")

BRIDGE_APP_OUTPUT=$(linera_exec publish-and-create \
    "$WASM_DIR/evm_bridge_contract.wasm" \
    "$WASM_DIR/evm_bridge_service.wasm" \
    --json-parameters "$BRIDGE_PARAMS" \
    --json-argument 'null' \
    --required-application-ids "$WRAPPED_APP_ID" 2>&1) || {
    echo "ERROR: publish-and-create evm-bridge failed:" >&2
    echo "$BRIDGE_APP_OUTPUT" >&2
    exit 1
}
BRIDGE_APP_ID=$(echo "$BRIDGE_APP_OUTPUT" | grep -oE '^[a-f0-9]{64}$' | tail -1)
validate_hex64 "EVM-bridge app ID" "$BRIDGE_APP_ID"
echo "  EVM-bridge app: $BRIDGE_APP_ID"

# Write bridge app ID to shared volume (Docker mode only).
if [[ -n "$COMPOSE_FILE" ]]; then
    dc_exec --user root foundry-tools sh -c "echo '$BRIDGE_APP_ID' > /shared/bridge-app-id"
fi

# ── 7. Fund FungibleBridge with ERC20 tokens ──
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
EOF

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Environment written to: $OUTPUT_FILE"
echo ""
echo "Start the frontend:"
echo "  cd examples/bridge-demo"
echo "  pnpm install && pnpm dev"
