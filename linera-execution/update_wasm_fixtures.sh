#!/bin/bash -e

cd `dirname $0`/..

(cd linera-examples && cargo build --release)

cp linera-examples/target/wasm32-unknown-unknown/release/counter_{contract,service}.wasm linera-execution/tests/fixtures
