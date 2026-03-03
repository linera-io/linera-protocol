#!/usr/bin/env bash
# Setup script for the EVM↔Linera bridge demo.
#
# Prerequisites:
#   - Docker Compose stack running (docker-compose.bridge-test.yml)
#   - Wasm binaries built: cd examples && cargo build --release --target wasm32-unknown-unknown
#   - Images built: make -C linera-bridge build-all
#
# Usage:
#   cd examples/bridge-demo
#   ./setup.sh
#
# Outputs: .env.local with all required environment variables for the frontend.

set -euo pipefail

COMPOSE_FILE="../../docker/docker-compose.bridge-test.yml"
PROJECT_NAME="linera-bridge-demo"
ANVIL_PRIVATE_KEY="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ANVIL_RPC_URL="http://anvil:8545"
FAUCET_URL="http://linera-network:8080"
WASM_DIR="/wasm"  # Mounted from examples/target/wasm32-unknown-unknown/release
WALLET_DIR="/tmp/wallet"
EXTRA_WALLET_ID=1

dc() {
    docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" "$@"
}

dc_exec() {
    dc exec -T "$@"
}

# Parse "Deployed to: 0x..." from forge create output.
parse_address() {
    grep 'Deployed to:' | sed 's/.*Deployed to: //' | tr -d '[:space:]'
}

# Run a linera CLI command using the extra wallet copy (avoids RocksDB lock
# conflicts with the running faucet process that holds wallet_0).
linera_cli() {
    dc_exec linera-network env \
        LINERA_WALLET="$WALLET_DIR/wallet_${EXTRA_WALLET_ID}.json" \
        LINERA_KEYSTORE="$WALLET_DIR/keystore_${EXTRA_WALLET_ID}.json" \
        LINERA_STORAGE="rocksdb:$WALLET_DIR/client_${EXTRA_WALLET_ID}.db" \
        ./linera "$@"
}

echo "=== Bridge Demo Setup ==="

# ── 1. Read LightClient address (deployed by bridge-init service) ──
echo "Reading LightClient address..."
LIGHT_CLIENT_ADDR=$(dc_exec foundry-tools cat /shared/light-client-address | tr -d '[:space:]')
echo "  LightClient: $LIGHT_CLIENT_ADDR"

# ── 2. Read bridge chain ID (written by relay on startup) ──
echo "Waiting for relay to claim bridge chain..."
for i in $(seq 1 30); do
    BRIDGE_CHAIN_ID=$(dc_exec linera-relay cat /shared/bridge-chain-id 2>/dev/null | tr -d '[:space:]' || true)
    # Validate it looks like a hex chain ID (64 hex chars), not an error message.
    if echo "$BRIDGE_CHAIN_ID" | grep -qE '^[a-f0-9]{64}$'; then
        break
    fi
    BRIDGE_CHAIN_ID=""
    echo "  Waiting... ($i/30)"
    sleep 2
done
if [ -z "$BRIDGE_CHAIN_ID" ]; then
    echo "ERROR: Relay did not write bridge chain ID within timeout" >&2
    exit 1
fi
echo "  Bridge chain: $BRIDGE_CHAIN_ID"

# ── 2b. Create extra wallet copy (avoids RocksDB lock with running faucet) ──
echo "Creating extra wallet copy..."
dc_exec linera-network sh -c "\
    cp $WALLET_DIR/wallet_0.json $WALLET_DIR/wallet_${EXTRA_WALLET_ID}.json && \
    cp $WALLET_DIR/keystore_0.json $WALLET_DIR/keystore_${EXTRA_WALLET_ID}.json && \
    cp -r $WALLET_DIR/client_0.db $WALLET_DIR/client_${EXTRA_WALLET_ID}.db"

# ── 3. Deploy MockERC20 ──
echo "Deploying MockERC20..."
ERC20_OUTPUT=$(dc_exec foundry-tools \
    forge create /contracts/MockERC20.sol:MockERC20 \
    --root /contracts --via-ir --optimize \
    --evm-version shanghai \
    --out /tmp/forge-out --cache-path /tmp/forge-cache \
    --rpc-url "$ANVIL_RPC_URL" \
    --private-key "$ANVIL_PRIVATE_KEY" \
    --broadcast \
    --constructor-args "TestToken" "TT" 1000000000000000000000)
TOKEN_ADDRESS=$(echo "$ERC20_OUTPUT" | parse_address)
TOKEN_ADDR_HEX=$(echo "$TOKEN_ADDRESS" | sed 's/^0x//')
echo "  MockERC20: $TOKEN_ADDRESS"

# ── 3b. Extract chain owner (needed as minter for wrapped-fungible) ──
echo "Extracting chain owner..."
WALLET_JSON=$(dc_exec linera-network cat "$WALLET_DIR/wallet_${EXTRA_WALLET_ID}.json")
CHAIN_OWNER=$(echo "$WALLET_JSON" | python3 -c "
import json, sys
w = json.load(sys.stdin)
default_chain = w['default']
print(w['chains'][default_chain]['owner'])
")
echo "  Chain owner: $CHAIN_OWNER"

# ── 4. Publish and create wrapped-fungible app ──
echo "Publishing and creating wrapped-fungible app..."
# WrappedParameters requires: ticker_symbol, minter (AccountOwner), evm_token_address ([u8;20]),
# evm_source_chain_id (u64). See examples/wrapped-fungible/src/lib.rs.
WRAPPED_PARAMS=$(python3 -c "
import json
def hex_to_array(h):
    return [int(h[i:i+2], 16) for i in range(0, len(h), 2)]
params = {
    'ticker_symbol': 'wTT',
    'minter': '$CHAIN_OWNER',
    'evm_token_address': hex_to_array('$TOKEN_ADDR_HEX'),
    'evm_source_chain_id': 31337,
}
print(json.dumps(params))
")
WRAPPED_APP_OUTPUT=$(linera_cli publish-and-create \
    "$WASM_DIR/wrapped_fungible_contract.wasm" \
    "$WASM_DIR/wrapped_fungible_service.wasm" \
    --json-parameters "$WRAPPED_PARAMS" \
    --json-argument '{"accounts":{}}' 2>&1) || {
    echo "ERROR: publish-and-create wrapped-fungible failed:" >&2
    echo "$WRAPPED_APP_OUTPUT" >&2
    exit 1
}
WRAPPED_APP_ID=$(echo "$WRAPPED_APP_OUTPUT" | grep -oE '[a-f0-9]{64}[a-f0-9]+' | tail -1)
echo "  Wrapped-fungible app: $WRAPPED_APP_ID"

# ── 5. Deploy FungibleBridge ──
# Deployed before evm-bridge so we have the real bridge_contract_address.
# FungibleBridge.applicationId = wrapped-fungible app's description hash, used in
# deposit() validation and _onBlock() Credit message matching.
echo "Deploying FungibleBridge..."
APP_ID_BYTES32="0x${WRAPPED_APP_ID:0:64}"
CHAIN_BYTES32="0x${BRIDGE_CHAIN_ID}"

BRIDGE_OUTPUT=$(dc_exec foundry-tools \
    forge create /contracts/FungibleBridge.sol:FungibleBridge \
    --root /contracts --via-ir --optimize \
    --ignored-error-codes 6321 \
    --evm-version shanghai \
    --out /tmp/forge-out --cache-path /tmp/forge-cache \
    --rpc-url "$ANVIL_RPC_URL" \
    --private-key "$ANVIL_PRIVATE_KEY" \
    --broadcast \
    --constructor-args \
    "$LIGHT_CLIENT_ADDR" \
    "$CHAIN_BYTES32" \
    0 \
    "$APP_ID_BYTES32" \
    "$TOKEN_ADDRESS")
BRIDGE_ADDRESS=$(echo "$BRIDGE_OUTPUT" | parse_address)
BRIDGE_ADDR_HEX=$(echo "$BRIDGE_ADDRESS" | sed 's/^0x//')
echo "  FungibleBridge: $BRIDGE_ADDRESS"

# Write bridge address to shared volume so the relay can pick it up.
dc_exec foundry-tools sh -c "echo '$BRIDGE_ADDRESS' > /shared/bridge-address"

# ── 6. Publish and create evm-bridge app ──
echo "Publishing and creating evm-bridge app..."
# BridgeParameters: source_chain_id, bridge_contract_address ([u8;20]),
# fungible_app_id (ApplicationId), token_address ([u8;20]).
BRIDGE_PARAMS=$(python3 -c "
import json
def hex_to_array(h):
    return [int(h[i:i+2], 16) for i in range(0, len(h), 2)]
params = {
    'source_chain_id': 31337,
    'bridge_contract_address': hex_to_array('$BRIDGE_ADDR_HEX'),
    'fungible_app_id': '$WRAPPED_APP_ID',
    'token_address': hex_to_array('$TOKEN_ADDR_HEX'),
}
print(json.dumps(params))
")

BRIDGE_APP_OUTPUT=$(linera_cli publish-and-create \
    "$WASM_DIR/evm_bridge_contract.wasm" \
    "$WASM_DIR/evm_bridge_service.wasm" \
    --json-parameters "$BRIDGE_PARAMS" \
    --json-argument 'null' 2>&1) || {
    echo "ERROR: publish-and-create evm-bridge failed:" >&2
    echo "$BRIDGE_APP_OUTPUT" >&2
    exit 1
}
BRIDGE_APP_ID=$(echo "$BRIDGE_APP_OUTPUT" | grep -oE '[a-f0-9]{64}[a-f0-9]+' | tail -1)
echo "  EVM-bridge app: $BRIDGE_APP_ID"

# ── 7. Fund FungibleBridge with ERC20 tokens (for withdrawals) ──
echo "Funding FungibleBridge with 500 tokens..."
dc_exec foundry-tools \
    cast send --rpc-url "$ANVIL_RPC_URL" \
    --private-key "$ANVIL_PRIVATE_KEY" \
    "$TOKEN_ADDRESS" \
    'transfer(address,uint256)(bool)' \
    "$BRIDGE_ADDRESS" \
    500000000000000000000

# ── 8. Write .env.local ──
echo "Writing .env.local..."
cat > .env.local << EOF
LINERA_FAUCET_URL=http://localhost:8080
LINERA_APPLICATION_ID=$WRAPPED_APP_ID
LINERA_BRIDGE_APP_ID=$BRIDGE_APP_ID
LINERA_RELAY_URL=http://localhost:3001
LINERA_BRIDGE_ADDRESS=$BRIDGE_ADDRESS
LINERA_TOKEN_ADDRESS=$TOKEN_ADDRESS
LINERA_BRIDGE_CHAIN_ID=$BRIDGE_CHAIN_ID
EOF

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Start the frontend:"
echo "  cd examples/bridge-demo"
echo "  pnpm install && pnpm dev"
echo ""
echo "Open http://localhost:5173 and connect MetaMask to Anvil (chain ID 31337)"
echo "Import Anvil account 0: $ANVIL_PRIVATE_KEY"
