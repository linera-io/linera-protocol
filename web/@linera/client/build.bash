#!/usr/bin/env bash

set -eu

shopt -s extglob

cd $(dirname -- "${BASH_SOURCE[0]}")

wasm_bindgen_cli_version=$(wasm-bindgen --version)
wasm_bindgen_cli_version=${wasm_bindgen_cli_version##* }

wasm_bindgen_cargo_version=$(cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "wasm-bindgen").version')
target_dir=$(cargo metadata --format-version 1 | jq -r .target_directory)

if [[ "$wasm_bindgen_cargo_version" != "$wasm_bindgen_cli_version" ]]
then
    cargo update --package wasm-bindgen --precise "$wasm_bindgen_cli_version"
fi

if [ "${1-}" = "--release" ]
then
    profile_flag=--release
    profile_dir=release
else
    profile_flag=
    profile_dir=debug
fi

cargo build --lib --target wasm32-unknown-unknown $profile_flag

wasm-bindgen \
    "$target_dir"/wasm32-unknown-unknown/$profile_dir/linera_web.wasm \
    --out-dir src/wasm \
    --out-name index \
    --typescript \
    --target web \
    --keep-debug \
    --split-linked-modules

if command -v wasm-split >/dev/null; then
    wasm-split \
        src/wasm/index_bg.wasm \
        --strip \
        --debug-out src/wasm/index_bg.debug.wasm
else
    echo "wasm-split not found, skipping (debug wasm and stripping disabled)" >&2
fi

# Start from a clean dist so stale artifacts from a previous build can never be
# published (dist/ is gitignored and rebuilt fresh on `prepare`/publish).
rm -rf dist
mkdir -p dist
cp -r src/wasm dist/

pnpm exec tsc
pnpm exec tsc-alias
