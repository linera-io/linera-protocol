[![Build Status](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml)
[![License](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE.md)

# Linera

This repository is dedicated to developing the Linera protocol.

## Quickstart with the Linera service CLI

The current code was imported from https://github.com/novifinancial/fastpay/pull/24 then
cleaned up (e.g. removing coins and assets for now).

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

# Create configuration files for 10 user chains.
# * Private chain states are stored in one local wallet `wallet.json`.
# * `genesis.json` will contain the initial balances of chains as well as the initial committee.
./client --wallet wallet.json --genesis genesis.json create_genesis_config 10 --initial-funding 10 --committee committee.json

# Start servers and create initial chains in DB
for I in 1 2 3 4
do
    ./proxy server_"$I".json &

    for J in $(seq 0 3)
    do
        ./server run --storage rocksdb:server_"$I"_"$J".db --server server_"$I".json --shard "$J" --genesis genesis.json &
    done
 done

LAST_PID="$!"

# Command line prefix for client calls
CLIENT=(./client --storage rocksdb:client.db --wallet wallet.json --genesis genesis.json --max-pending-messages 10000)

${CLIENT[@]} query_validators

# Give some time for server startup
sleep 1

# Query balance for first and last user chain
CHAIN1="7817752ff06b8266d77df8febf5c4b524cec096bd83dc54f989074fb94f833737ae984f32be2cee1dfab766fe2d0c726503c4d97117eb59023e9cc65a8ecd1f7"
CHAIN2="16377ac9ccb009cf58898bf3ffd3bf293aad12f31c0dfa798b819deece9e57a5730146a399c812d3fc551f4290b15dc08f3f1527b06252284b4b89327caaffad"
${CLIENT[@]} query_balance "$CHAIN1"
${CLIENT[@]} query_balance "$CHAIN2"

# Transfer 10 units then 5 back
${CLIENT[@]} transfer 10 --from "$CHAIN1" --to "$CHAIN2"
${CLIENT[@]} transfer 5 --from "$CHAIN2" --to "$CHAIN1"

# Restart last server
kill "$LAST_PID"
./server run --storage rocksdb:server_"$I"_"$J".db --server server_"$I".json --shard "$J" --genesis genesis.json &

sleep 1

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

# Create two more validators
NAME5=$(./server generate --validators \
   server_5.json:127.0.0.1:9500:udp:127.0.0.1:9501:127.0.0.1:9502:127.0.0.1:9503:127.0.0.1:9504)

NAME6=$(./server generate --validators \
   server_6.json:127.0.0.1:9600:udp:127.0.0.1:9601:127.0.0.1:9602:127.0.0.1:9603:127.0.0.1:9604)

# Start the corresponding services
for I in 6 5
do
    ./proxy server_"$I".json &

    # hack!
    PID5="$!"

    for J in $(seq 0 3)
    do
        ./server run --storage rocksdb:server_"$I"_"$J".db --server server_"$I".json --shard "$J" --genesis genesis.json &
    done
done

sleep 1

${CLIENT[@]} set_validator --name "$NAME5" --address 127.0.0.1:9500 --votes 100

${CLIENT[@]} query_balance "$CHAIN1"
${CLIENT[@]} query_validators
${CLIENT[@]} query_validators "$CHAIN1"

${CLIENT[@]} set_validator --name "$NAME6" --address 127.0.0.1:9600 --votes 1
sleep 2

${CLIENT[@]} remove_validator --name "$NAME5"
kill "$PID5"

${CLIENT[@]} query_balance "$CHAIN1"
${CLIENT[@]} query_validators
${CLIENT[@]} query_validators "$CHAIN1"

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

Getting an error with the test in [`linera-rpc/tests/format.rs`](linera-rpc/tests/format.rs) ?
Probably the file [`linera-rpc/tests/staged/formats.yaml`](linera-rpc/tests/staged/formats.yaml) (recording message formats) is
outdated. In the most case (but not always sadly), this can be fixed by running
[`linera-rpc/generate-format.sh`](linera-rpc/generate-format.sh).

See https://github.com/zefchain/serde-reflection for more context.
