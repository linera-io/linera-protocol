[![Build Status](https://github.com/zefchain/zefchain-protocol/actions/workflows/rust.yml/badge.svg)](https://github.com/zef/zef-protocol/actions/workflows/rust.yml)
[![License](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE.md)

# Zefchain Protocol

This repository is dedicated to developing the Zefchain protocol.

## Quickstart with the Zef service CLI

The current code was imported from https://github.com/novifinancial/fastpay/pull/24 then
cleaned up (e.g. removing coins and assets for now). Atomic swaps are still WIP (notably
the client and CLI code is missing).

The following script can be run with `cargo test`.

```bash
# For debug builds:
cargo build && cd target/debug
# For release builds:
# cargo build --release && cd target/release

# Clean up data files
rm -rf *.json *.txt *.db

# Make sure to clean up child processes on exit.
trap 'kill $(jobs -p)' EXIT

# Create configuration files for 4 validators with 4 shards each.
# * Private server states are stored in `server*.json`.
# * `committee.json` is the public description of the FastPay committee.
./server generate-all --validators \
   server_1.json:udp:127.0.0.1:9100:4 \
   server_2.json:udp:127.0.0.1:9200:4 \
   server_3.json:udp:127.0.0.1:9300:4 \
   server_4.json:udp:127.0.0.1:9400:4 \
--committee committee.json

# Create configuration files for 1000 user chains.
# * Private chain states are stored in one local wallet `wallet.json`.
# * `genesis.json` will contain the initial balances of chains as well as the initial committee.
./client --wallet wallet.json --genesis genesis.json create_genesis_config 1000 --initial-funding 100 --committee committee.json

# Start servers and create initial chains in DB
for I in 1 2 3 4
do
    for J in $(seq 0 3)
    do
        ./server run --storage server_"$I"_"$J".db --server server_"$I".json --shard "$J" --genesis genesis.json &
    done
 done

LAST_PID="$!"

# Command line prefix for client calls
CLIENT=(./client --storage client.db --wallet wallet.json --genesis genesis.json)

# Query balance for first and last user chain
CHAIN1="[0]"
CHAIN2="[999]"
${CLIENT[@]} query_balance "$CHAIN1"
${CLIENT[@]} query_balance "$CHAIN2"

# Transfer 10 units then 5 back
${CLIENT[@]} transfer 10 --from "$CHAIN1" --to "$CHAIN2"
${CLIENT[@]} transfer 5 --from "$CHAIN2" --to "$CHAIN1"

# Restart last server
kill "$LAST_PID"
./server run --storage server_"$I"_"$J".db --server server_"$I".json --shard "$J" --genesis genesis.json &

# Query balances again
${CLIENT[@]} query_balance "$CHAIN1"
${CLIENT[@]} query_balance "$CHAIN2"

# Launch local benchmark using all user chains
${CLIENT[@]} benchmark --max-in-flight 50

# Create derived chain
CHAIN3="`${CLIENT[@]} open_chain --from "$CHAIN1"`"

# Inspect state of derived chain
fgrep '"chain_id"':"$CHAIN3" wallet.json

# Query the balance of the first chain
${CLIENT[@]} query_balance "$CHAIN1"

cd ../..
```

## Contributing

### Formatting and linting

Make sure to fix the lint errors reported by
```
cargo clippy --all-targets
```
and run `cargo fmt` like this:
```
cargo fmt -- --config unstable_features=true --config imports_granularity=Crate
```
or (optimistically)
```
cargo fmt +nightly
```
(see also [rust-lang/rustfmt#4991](https://github.com/rust-lang/rustfmt/issues/4991))

### Dealing with test failures `test_format` after code changes

Getting an error with the test in [`zef-core/tests/format.rs`](zef-core/tests/format.rs) ?
Probably the file [`zef-core/tests/staged/formats.yaml`](zef-core/tests/staged/formats.yaml) (recording message formats) is
outdated. In the most case (but not always sadly), this can be fixed by running
[`zef-core/generate-format.sh`](zef-core/generate-format.sh).

See https://github.com/novifinancial/serde-reflection for more context.
