#!/usr/bin/env bash
# Build linera-storage-udf for ScyllaDB Wasm UDF registration.
#
# Produces a `.wat` file that can be embedded directly into a CQL `CREATE FUNCTION` statement.
# Run from the repository root.
set -euo pipefail

BIN_NAME=linera_storage_udf
TARGET=wasm32-wasip1
PROFILE=wasm-udf
OUT_DIR=target/${TARGET}/${PROFILE}

for tool in cargo wasm2wat; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "error: missing $tool" >&2
        exit 1
    fi
done

if ! rustup target list --installed 2>/dev/null | grep -q "^${TARGET}$"; then
    rustup target add "$TARGET"
fi

# scylla-rust-udf README: WASI default stack of 1MB causes Scylla warnings; keep 128KB.
RUSTFLAGS="-C link-args=-zstack-size=131072" \
    cargo build -p linera-storage-udf --target "$TARGET" --profile "$PROFILE"

if command -v wasm-strip >/dev/null 2>&1; then
    wasm-strip "${OUT_DIR}/${BIN_NAME}.wasm"
fi

wasm2wat "${OUT_DIR}/${BIN_NAME}.wasm" > "${OUT_DIR}/${BIN_NAME}.wat"

echo
echo "Built: ${OUT_DIR}/${BIN_NAME}.wasm ($(stat -c%s "${OUT_DIR}/${BIN_NAME}.wasm") bytes)"
echo "WAT:   ${OUT_DIR}/${BIN_NAME}.wat ($(wc -l < "${OUT_DIR}/${BIN_NAME}.wat") lines)"
