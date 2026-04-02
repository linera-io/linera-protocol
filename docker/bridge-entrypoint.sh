#!/bin/sh
set -e

# If the first argument is "serve" with no other args, build CLI from env vars.
# Otherwise, pass everything through as-is (supports "sh -c ...", direct CLI usage, etc).
if [ "$1" != "serve" ]; then
    exec "$@"
fi

# Build the CLI invocation from environment variables.
shift  # consume "serve"

set -- linera-bridge serve \
    --rpc-url="${RPC_URL:?RPC_URL is required}" \
    --evm-bridge-address="${EVM_BRIDGE_ADDRESS:?EVM_BRIDGE_ADDRESS is required}" \
    --linera-bridge-address="${LINERA_BRIDGE_APP:?LINERA_BRIDGE_APP is required}" \
    --linera-fungible-address="${LINERA_FUNGIBLE_APP:?LINERA_FUNGIBLE_APP is required}" \
    --evm-private-key="${EVM_PRIVATE_KEY:?EVM_PRIVATE_KEY is required}" \
    --port="${PORT:-5001}" \
    --monitor-scan-interval="${MONITOR_SCAN_INTERVAL:-30}" \
    --monitor-start-block="${MONITOR_START_BLOCK:-0}" \
    --max-retries="${MAX_RETRIES:-10}" \
    --blob-cache-size="${BLOB_CACHE_SIZE:-1000}" \
    --confirmed-block-cache-size="${CONFIRMED_BLOCK_CACHE_SIZE:-1000}" \
    --lite-certificate-cache-size="${LITE_CERTIFICATE_CACHE_SIZE:-1000}" \
    --certificate-raw-cache-size="${CERTIFICATE_RAW_CACHE_SIZE:-1000}" \
    --event-cache-size="${EVENT_CACHE_SIZE:-1000}"

# Optional: faucet URL (required when wallet doesn't exist or chain ID not provided)
if [ -n "$FAUCET_URL" ]; then
    set -- "$@" --faucet-url="$FAUCET_URL"
fi

# Optional: bridge chain ID + owner (owner is required when chain ID is provided)
if [ -n "$LINERA_BRIDGE_CHAIN_ID" ]; then
    set -- "$@" --linera-bridge-chain-id="$LINERA_BRIDGE_CHAIN_ID"
    set -- "$@" --linera-bridge-chain-owner="${LINERA_BRIDGE_CHAIN_OWNER:?LINERA_BRIDGE_CHAIN_OWNER is required when LINERA_BRIDGE_CHAIN_ID is set}"
fi

# LINERA_WALLET, LINERA_KEYSTORE, LINERA_STORAGE are read directly by clap
# via `env = "..."`, so they don't need explicit --flags here.

exec "$@"
