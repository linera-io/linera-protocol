#!/usr/bin/env bash
# Spawn a single linera-bridge relayer container against an external network
# where contracts and Linera apps are already deployed (e.g. Base +
# Linera mainnet). No deploys, no app registrations.
#
# For full local development (anvil + local validator + frontend) use
# `make demo` from linera-bridge/ instead — that path uses setup.sh.
#
# Usage:
#   ./spawn-relayer.sh --env-file PATH --data-dir PATH [--env-secret-file PATH]
#
# --env-file:        env file consumed by the linera-bridge container (must
#                    contain RPC_URL, FAUCET_URL, EVM_BRIDGE_ADDRESS,
#                    LINERA_BRIDGE_APP, LINERA_FUNGIBLE_APP,
#                    LINERA_BRIDGE_CHAIN_ID, LINERA_BRIDGE_CHAIN_OWNER,
#                    LINERA_WALLET=/data/..., LINERA_KEYSTORE=/data/...,
#                    LINERA_STORAGE=rocksdb:/data/..., MONITOR_*,
#                    MAX_RETRIES, PORT).
# --env-secret-file: optional second env file (e.g. EVM_PRIVATE_KEY=0x...).
#                    If omitted, an empty temp file is used.
# --data-dir:        host directory bind-mounted at /data in the container
#                    (wallet.json, keystore.json, client.db).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

ENV_FILE=""
ENV_SECRET_FILE=""
DATA_DIR=""

die() { echo "ERROR: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --env-file)        ENV_FILE="$2"; shift 2 ;;
        --env-secret-file) ENV_SECRET_FILE="$2"; shift 2 ;;
        --data-dir)        DATA_DIR="$2"; shift 2 ;;
        --help) echo "Usage: $(basename "$0") --env-file PATH --data-dir PATH [--env-secret-file PATH]"; exit 0 ;;
        *) die "Unknown option: $1" ;;
    esac
done

[[ -n "$ENV_FILE" ]] || die "--env-file is required"
[[ -f "$ENV_FILE" ]] || die "--env-file path does not exist: $ENV_FILE"
[[ -n "$DATA_DIR" ]] || die "--data-dir is required"
[[ -d "$DATA_DIR" ]] || die "--data-dir path does not exist or is not a directory: $DATA_DIR"

if [[ -z "$ENV_SECRET_FILE" ]]; then
    ENV_SECRET_FILE="$(mktemp)"
    trap 'rm -f "$ENV_SECRET_FILE"' EXIT
fi
[[ -f "$ENV_SECRET_FILE" ]] || die "--env-secret-file path does not exist: $ENV_SECRET_FILE"

RELAYER_COMPOSE="$(cd "$SCRIPT_DIR/../.." && pwd)/docker/docker-compose.bridge-mainnet.yml"
[[ -f "$RELAYER_COMPOSE" ]] || die "expected compose file at $RELAYER_COMPOSE"

echo "Spawning relayer:"
echo "  Env file:        $ENV_FILE"
echo "  Env secret file: $ENV_SECRET_FILE"
echo "  Data dir:        $DATA_DIR"

RELAYER_ENV_FILE="$ENV_FILE" \
RELAYER_ENV_SECRET="$ENV_SECRET_FILE" \
RELAYER_DATA_DIR="$DATA_DIR" \
    docker compose -f "$RELAYER_COMPOSE" up -d relayer
