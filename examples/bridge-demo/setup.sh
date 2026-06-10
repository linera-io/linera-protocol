#!/usr/bin/env bash
# Setup script for the EVM↔Linera bridge demo (local development).
#
# Brings up the local compose stack, deploys EVM contracts, publishes Linera
# apps, registers IDs on both sides, optionally funds the bridge, and writes
# .env.local for the demo frontend.
#
# Usage:
#   ./setup.sh --compose-file ../../docker/docker-compose.bridge-test.yml
#
# To spawn just the relayer container against an already-deployed local stack
# (no deploy / no register), use the companion script `spawn-relayer.sh`.

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
TOKEN_DECIMALS=18
FUND_AMOUNT="500000000000000000000"
SHARED_DIR=""
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

  $(basename "$0") --compose-file ../../docker/docker-compose.bridge-test.yml

Options:
  --compose-file PATH       Docker Compose file (required)
  --wasm-dir PATH           Directory with .wasm binaries (default: /wasm)
  --contracts-dir PATH      Solidity source root for forge --root
                            (default: /contracts)
  --output PATH             Output env file (default: .env.local in script dir)
  --faucet-url URL          Linera faucet URL (default: http://localhost:8080)
  --relay-url URL           Relay service URL (default: http://localhost:3001)
  --ticker-symbol SYM       Wrapped token ticker (default: wTT)
  --token-decimals N        Decimal places for the deployed ERC-20 and the
                            matching wrapped-fungible app (default: 18). Both
                            sides MUST agree or the relayer will refuse to start.
  --fund-amount WEI         Fund bridge with this many tokens; 0 to skip
                            (default: 500000000000000000000)
  --shared-dir PATH         Directory for shared state files (bridge-address,
                            app IDs). Default: /tmp/bridge-demo-<timestamp>
  --help                    Show this help
EOF
    exit 0
}

# ── Parse flags ──
while [[ $# -gt 0 ]]; do
    case "$1" in
        --compose-file)      COMPOSE_FILE="$2"; shift 2 ;;
        --wasm-dir)          WASM_DIR="$2"; shift 2 ;;
        --contracts-dir)     CONTRACTS_DIR="$2"; shift 2 ;;
        --output)            OUTPUT_FILE="$2"; shift 2 ;;
        --faucet-url)        FAUCET_URL="$2"; shift 2 ;;
        --relay-url)         RELAY_URL="$2"; shift 2 ;;
        --ticker-symbol)     TICKER_SYMBOL="$2"; shift 2 ;;
        --token-decimals)    TOKEN_DECIMALS="$2"; shift 2 ;;
        --fund-amount)       FUND_AMOUNT="$2"; shift 2 ;;
        --shared-dir)        SHARED_DIR="$2"; shift 2 ;;
        --help)              usage ;;
        *) die "Unknown option: $1" ;;
    esac
done

# Also accept COMPOSE_FILE from env var.
COMPOSE_FILE="${COMPOSE_FILE:-${BRIDGE_COMPOSE_FILE:-}}"
[[ -n "$COMPOSE_FILE" ]] || die "--compose-file is required; see --help"

# ── Docker helpers ──
dc() {
    docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" "$@"
}

dc_exec() {
    dc exec -T "$@"
}

# ── Command execution helpers ──
evm_exec() {
    dc_exec foundry-tools "$@"
}

linera_exec() {
    dc_exec linera-network env \
        LINERA_WALLET="$WALLET_DIR/wallet_${EXTRA_WALLET_ID}.json" \
        LINERA_KEYSTORE="$WALLET_DIR/keystore_${EXTRA_WALLET_ID}.json" \
        LINERA_STORAGE="rocksdb:$WALLET_DIR/client_${EXTRA_WALLET_ID}.db" \
        ./linera "$@"
}

# ── Defaults ──
echo "Compose file: $COMPOSE_FILE"
EVM_RPC_URL="${EVM_RPC_URL:-http://anvil:8545}"
EVM_PRIVATE_KEY="${EVM_PRIVATE_KEY:-0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80}"
WASM_DIR="${WASM_DIR:-/wasm/examples}"
EVM_BRIDGE_WASM_DIR="${EVM_BRIDGE_WASM_DIR:-/wasm/root}"
CONTRACTS_DIR="${CONTRACTS_DIR:-/contracts}"
OUTPUT_FILE="${OUTPUT_FILE:-$SCRIPT_DIR/.env.local}"
FAUCET_URL="${FAUCET_URL:-http://localhost:8080}"
RELAY_URL="${RELAY_URL:-http://localhost:3001}"
# Docker container has its own solc; don't force a version download.
FORGE_USE_SOLC=()

# ── Shared dir for relay coordination ──
if [[ -z "$SHARED_DIR" ]]; then
    SHARED_DIR="/tmp/bridge-demo-$(date +%Y%m%d-%H%M%S)"
fi
mkdir -p "$SHARED_DIR"
echo "  Shared dir: $SHARED_DIR"

echo "=== Bridge Demo Setup ==="
echo ""
echo "Configuration:"
echo "  EVM RPC URL:        $EVM_RPC_URL"
echo "  EVM chain ID:       $EVM_CHAIN_ID"
echo "  Bridge chain ID:    (will be read from /shared/)"
echo "  Relay owner:        (will be read from /shared/)"
echo "  Faucet URL:         $FAUCET_URL"
echo "  Shared dir:         $SHARED_DIR"
echo ""

# ── 1. Read LightClient address (deployed by bridge-init in compose) ──
echo "Reading LightClient address from /shared/..."
LIGHT_CLIENT_ADDR=$(dc_exec foundry-tools cat /shared/light-client-address | tr -d '[:space:]')
validate_eth_address "LightClient address" "$LIGHT_CLIENT_ADDR"
echo "  LightClient: $LIGHT_CLIENT_ADDR"

# ── 2. Read bridge chain ID ──
echo "Waiting for bridge chain ID..."
BRIDGE_CHAIN_ID=""
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
[[ -z "$BRIDGE_CHAIN_ID" ]] && die "Bridge chain ID not found within timeout"
validate_hex64 "Bridge chain ID" "$BRIDGE_CHAIN_ID"
echo "  Bridge chain: $BRIDGE_CHAIN_ID"

# ── 2b. Create extra wallet copy ──
echo "Creating extra wallet copy..."
dc_exec linera-network sh -c "\
    cp $WALLET_DIR/wallet_0.json $WALLET_DIR/wallet_${EXTRA_WALLET_ID}.json && \
    cp $WALLET_DIR/keystore_0.json $WALLET_DIR/keystore_${EXTRA_WALLET_ID}.json && \
    cp -r $WALLET_DIR/client_0.db $WALLET_DIR/client_${EXTRA_WALLET_ID}.db"

# ── 3. Deploy LineraToken ──
echo "Deploying LineraToken via forge script..."
EVM_CHAIN_ID_DECIMAL=$(dc_exec foundry-tools cast chain-id --rpc-url "$EVM_RPC_URL")
dc_exec foundry-tools env \
    TOKEN_NAME="TestToken" \
    TOKEN_SYMBOL="TT" \
    TOKEN_DECIMALS="$TOKEN_DECIMALS" \
    TOKEN_SUPPLY="1000000000000000000000" \
    forge script /contracts/script/DeployLineraToken.s.sol \
    --root /contracts \
    --rpc-url "$EVM_RPC_URL" \
    --private-key "$EVM_PRIVATE_KEY" \
    --broadcast >/tmp/linera-token-deploy.log 2>&1 || {
    cat /tmp/linera-token-deploy.log >&2
    die "LineraToken deploy failed"
}
TOKEN_ADDRESS=$(dc_exec foundry-tools \
    jq -r '.transactions[0].contractAddress' \
    "/contracts/broadcast/DeployLineraToken.s.sol/${EVM_CHAIN_ID_DECIMAL}/run-latest.json" \
    | tr -d '[:space:]')
echo "  LineraToken: $TOKEN_ADDRESS"
validate_eth_address "Token address" "$TOKEN_ADDRESS"
TOKEN_ADDR_HEX=$(echo "$TOKEN_ADDRESS" | sed 's/^0x//')

# ── 3b. Read relay owner (owns the bridge chain; performs the registrations) ──
echo "Reading relay owner..."
for i in $(seq 1 30); do
    RELAY_OWNER=$(dc_exec foundry-tools cat /shared/relay-owner 2>/dev/null | tr -d '[:space:]')
    if [[ -n "$RELAY_OWNER" ]]; then
        break
    fi
    echo "  Waiting for relay owner... ($i/30)"
    sleep 2
done
[[ -z "$RELAY_OWNER" ]] && die "Relay owner not found within timeout"
echo "  Relay owner: $RELAY_OWNER"

# FungibleBridge is deployed below (step 7), after the wrapped-fungible app
# is created so the canonical applicationId can be baked into its constructor.
CHAIN_BYTES32="0x${BRIDGE_CHAIN_ID}"

# ── 5. Publish and create wrapped-fungible app (first; the evm-bridge app takes
#       its app ID as a creation parameter) ──
echo "Syncing chain state..."
linera_exec sync 2>&1
linera_exec process-inbox 2>&1
echo "Publishing and creating wrapped-fungible app..."
WRAPPED_PARAMS=$(
    TICKER="$TICKER_SYMBOL" \
    DECIMALS="$TOKEN_DECIMALS" \
    TOKEN_HEX="$TOKEN_ADDR_HEX" \
    CHAIN_ID="$EVM_CHAIN_ID" \
    BRIDGE_CHAIN="$BRIDGE_CHAIN_ID" \
    python3 -c "
import json, os
def hex_to_array(h):
    return [int(h[i:i+2], 16) for i in range(0, len(h), 2)]
params = {
    'ticker_symbol': os.environ['TICKER'],
    'decimals': int(os.environ['DECIMALS']),
    'mint_chain_id': os.environ['BRIDGE_CHAIN'],
    'evm_token_address': hex_to_array(os.environ['TOKEN_HEX']),
    'evm_source_chain_id': int(os.environ['CHAIN_ID']),
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
    echo "$WRAPPED_APP_OUTPUT" >&2
    sleep 2
done
[[ -z "$WRAPPED_APP_OUTPUT" ]] && { echo "ERROR: publish-and-create wrapped-fungible failed after retries" >&2; exit 1; }
WRAPPED_APP_ID=$(echo "$WRAPPED_APP_OUTPUT" | grep -oE '^[a-f0-9]{64}$' | tail -1)
validate_hex64 "Wrapped-fungible app ID" "$WRAPPED_APP_ID"
echo "  Wrapped-fungible app: $WRAPPED_APP_ID"

# Write wrapped-fungible app ID to shared dir for relay.
dc_exec --user root foundry-tools sh -c "echo '$WRAPPED_APP_ID' > /shared/wrapped-app-id"
echo "$WRAPPED_APP_ID" > "$SHARED_DIR/wrapped-app-id"

APP_ID_BYTES32="0x${WRAPPED_APP_ID}"

# ── 6. Publish and create evm-bridge app (points at the wrapped app) ──
echo "Syncing chain state..."
linera_exec sync 2>&1
linera_exec process-inbox 2>&1
echo "Publishing and creating evm-bridge app..."
BRIDGE_PARAMS=$(
    CHAIN_ID="$EVM_CHAIN_ID" \
    TOKEN_HEX="$TOKEN_ADDR_HEX" \
    BRIDGE_CHAIN="$BRIDGE_CHAIN_ID" \
    FUNGIBLE_APP="$WRAPPED_APP_ID" \
    python3 -c "
import json, os
def hex_to_array(h):
    return [int(h[i:i+2], 16) for i in range(0, len(h), 2)]
params = {
    'source_chain_id': int(os.environ['CHAIN_ID']),
    'token_address': hex_to_array(os.environ['TOKEN_HEX']),
    'bridge_chain_id': os.environ['BRIDGE_CHAIN'],
    'fungible_app_id': os.environ['FUNGIBLE_APP'],
}
print(json.dumps(params))
")

BRIDGE_ARGUMENT=$(EVM_RPC_URL="$EVM_RPC_URL" python3 -c "
import json, os
print(json.dumps({'rpc_endpoint': os.environ.get('EVM_RPC_URL', '')}))
")

for attempt in 1 2 3; do
    # The bridge calls the wrapped-fungible app (escrow transfer + burn), so it is
    # declared as a required application dependency. This makes the wrapped module
    # load eagerly with the bridge — required on the web client, which cannot load
    # modules dynamically mid-execution (#2927). Removed in a follow-up commit that
    # adds generalized module preloading.
    BRIDGE_APP_OUTPUT=$(linera_exec publish-and-create \
        "$EVM_BRIDGE_WASM_DIR/evm_bridge_contract.wasm" \
        "$EVM_BRIDGE_WASM_DIR/evm_bridge_service.wasm" \
        --json-parameters "$BRIDGE_PARAMS" \
        --json-argument "$BRIDGE_ARGUMENT" \
        --required-application-ids "$WRAPPED_APP_ID" 2>&1) && break
    echo "  Attempt $attempt failed:" >&2
    echo "$BRIDGE_APP_OUTPUT" >&2
    sleep 2
done
[[ -z "$BRIDGE_APP_OUTPUT" ]] && { echo "ERROR: publish-and-create evm-bridge failed after retries" >&2; exit 1; }
BRIDGE_APP_ID=$(echo "$BRIDGE_APP_OUTPUT" | grep -oE '^[a-f0-9]{64}$' | tail -1)
validate_hex64 "Linera bridge app ID" "$BRIDGE_APP_ID"
echo "  Linera bridge app: $BRIDGE_APP_ID"

# Write bridge app ID to shared dir for relay.
dc_exec --user root foundry-tools sh -c "echo '$BRIDGE_APP_ID' > /shared/bridge-app-id"
echo "$BRIDGE_APP_ID" > "$SHARED_DIR/bridge-app-id"

BRIDGE_APP_ID_BYTES32="0x${BRIDGE_APP_ID}"

# ── 7. Deploy FungibleBridge with the wrapped-fungible applicationId baked in ──
echo "Deploying FungibleBridge via forge script..."
dc_exec foundry-tools env \
    LIGHT_CLIENT="$LIGHT_CLIENT_ADDR" \
    BRIDGE_CHAIN_ID="$CHAIN_BYTES32" \
    TOKEN_ADDRESS="$TOKEN_ADDRESS" \
    FUNGIBLE_APP_ID="$APP_ID_BYTES32" \
    BRIDGE_APP_ID="$BRIDGE_APP_ID_BYTES32" \
    forge script /contracts/script/DeployFungibleBridge.s.sol \
    --root /contracts \
    --rpc-url "$EVM_RPC_URL" \
    --private-key "$EVM_PRIVATE_KEY" \
    --broadcast >/tmp/bridge-deploy.log 2>&1 || {
    cat /tmp/bridge-deploy.log >&2
    die "FungibleBridge deploy failed"
}
BRIDGE_ADDRESS=$(dc_exec foundry-tools \
    jq -r '.transactions[0].contractAddress' \
    "/contracts/broadcast/DeployFungibleBridge.s.sol/${EVM_CHAIN_ID_DECIMAL}/run-latest.json" \
    | tr -d '[:space:]')
validate_eth_address "FungibleBridge address" "$BRIDGE_ADDRESS"
BRIDGE_ADDR_HEX=$(echo "$BRIDGE_ADDRESS" | sed 's/^0x//')
echo "  FungibleBridge: $BRIDGE_ADDRESS"

# Write bridge address to shared dir for relay.
dc_exec --user root foundry-tools sh -c "echo '$BRIDGE_ADDRESS' > /shared/bridge-address"
echo "$BRIDGE_ADDRESS" > "$SHARED_DIR/bridge-address"

# ── 8. Register the bridge ↔ wrapped relationship on both sides ──
echo "Registering the bridge as the wrapped-fungible authorized caller..."
# BCS: WrappedFungibleOperation::RegisterAuthorizedCaller (variant 8) + 32-byte
# bridge app ID. Submitted on the WRAPPED app, on the mint (bridge) chain — the
# relay wallet owns that chain.
REGISTER_CALLER_HEX="08${BRIDGE_APP_ID}"
dc_exec linera-network env \
    LINERA_WALLET=/shared/relay-wallet/wallet.json \
    LINERA_KEYSTORE=/shared/relay-wallet/keystore.json \
    LINERA_STORAGE=rocksdb:/shared/relay-wallet/client.db \
    ./linera execute-operation \
    --application-id "$WRAPPED_APP_ID" \
    --operation "$REGISTER_CALLER_HEX" \
    --chain-id "$BRIDGE_CHAIN_ID" 2>&1

echo "Registering FungibleBridge address in evm-bridge..."
# BCS: BridgeOperation::RegisterFungibleBridge (variant 2) + 20-byte EVM address.
REGISTER_BRIDGE_HEX="02${BRIDGE_ADDR_HEX}"
dc_exec linera-network env \
    LINERA_WALLET=/shared/relay-wallet/wallet.json \
    LINERA_KEYSTORE=/shared/relay-wallet/keystore.json \
    LINERA_STORAGE=rocksdb:/shared/relay-wallet/client.db \
    ./linera execute-operation \
    --application-id "$BRIDGE_APP_ID" \
    --operation "$REGISTER_BRIDGE_HEX" \
    --chain-id "$BRIDGE_CHAIN_ID" 2>&1
echo "  Registered: bridge as wrapped's authorized caller; FungibleBridge address in evm-bridge"

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
dc_exec --user root foundry-tools sh -c "echo 'done' > /shared/setup-complete"

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Addresses & IDs:"
echo "  LightClient (EVM):          $LIGHT_CLIENT_ADDR"
echo "  FungibleBridge (EVM):       $BRIDGE_ADDRESS"
echo "  LineraToken (EVM):          $TOKEN_ADDRESS"
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
echo "  Start the frontend:"
echo "     cd examples/bridge-demo"
echo "     pnpm install && pnpm dev"
