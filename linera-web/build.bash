#!/usr/bin/env bash

set -eu

cd $(dirname -- "${BASH_SOURCE[0]}")

wasm_bindgen_cli_version=$(wasm-bindgen --version)
wasm_bindgen_cli_version=${wasm_bindgen_cli_version##* }

wasm_bindgen_cargo_version=
if type -P tomlq > /dev/null
then
    wasm_bindgen_cargo_version=$(tomlq -r < Cargo.lock '.package[]|select(.name == "wasm-bindgen")|.version')
fi

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
    target/wasm32-unknown-unknown/$profile_dir/linera_web.wasm \
    --out-dir dist \
    --typescript \
    --target web \
    --split-linked-modules

