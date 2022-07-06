[![Build Status](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml)
[![License](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE.md)

# Linera

This repository is dedicated to developing the Linera protocol.

## Quickstart with the Linera service CLI

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
./server generate --validators \
   server_1.json:127.0.0.1:9100:udp:127.0.0.1:9101:127.0.0.1:9102:127.0.0.1:9103:127.0.0.1:9104 \
   server_2.json:127.0.0.1:9200:udp:127.0.0.1:9201:127.0.0.1:9202:127.0.0.1:9203:127.0.0.1:9204 \
   server_3.json:127.0.0.1:9300:udp:127.0.0.1:9301:127.0.0.1:9302:127.0.0.1:9303:127.0.0.1:9304 \
   server_4.json:127.0.0.1:9400:udp:127.0.0.1:9401:127.0.0.1:9402:127.0.0.1:9403:127.0.0.1:9404 \
--committee committee.json

# Create configuration files for 1000 user chains.
# * Private chain states are stored in one local wallet `wallet.json`.
# * `genesis.json` will contain the initial balances of chains as well as the initial committee.
./client --wallet wallet.json --genesis genesis.json create_genesis_config 1000 --initial-funding 100 --committee committee.json

# Start servers and create initial chains in DB
for I in 1 2 3 4
do
    ./proxy server_"$I".json &

    for J in $(seq 0 3)
    do
        ./server run --storage server_"$I"_"$J".db --server server_"$I".json --shard "$J" --genesis genesis.json &
    done
 done

LAST_PID="$!"

# Command line prefix for client calls
CLIENT=(./client --storage client.db --wallet wallet.json --genesis genesis.json)

# Query balance for first and last user chain
CHAIN1="7817752ff06b8266d77df8febf5c4b524cec096bd83dc54f989074fb94f833737ae984f32be2cee1dfab766fe2d0c726503c4d97117eb59023e9cc65a8ecd1f7"
CHAIN2="8ec607f670dd82d6986e95815b6ba64e4aad748e6f023f8dde42439222959c0c4aa9d3af00a983deec3f1ab0e64ef27ff80a1a8e1c1990be677ef5cfb316de85"
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
fgrep '"chain_id":"'$CHAIN3'"' wallet.json

# Query the balance of the first chain
${CLIENT[@]} query_balance "$CHAIN1"

cd ../..
```

## Contributing

### Copyright

The new code should be copyrighted by "Zefchain Labs, Inc". (This is currently still the legal entity behind the Linera project.)

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

Getting an error with the test in [`linera-base/tests/format.rs`](linera-base/tests/format.rs) ?
Probably the file [`linera-base/tests/staged/formats.yaml`](linera-base/tests/staged/formats.yaml) (recording message formats) is
outdated. In the most case (but not always sadly), this can be fixed by running
[`linera-base/generate-format.sh`](linera-base/generate-format.sh).

See https://github.com/zefchain/serde-reflection for more context.
