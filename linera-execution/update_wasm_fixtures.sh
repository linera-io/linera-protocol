#!/bin/bash -e

cd `dirname $0`/..

(cd examples && cargo build --release)

cp examples/target/wasm32-unknown-unknown/release/counter_{contract,service}.wasm linera-execution/tests/fixtures
