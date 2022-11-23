[![Build Status](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml)
[![License](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE.md)

# Linera

This repository is dedicated to developing the Linera protocol. For an overview of how
the Linera protocol functions refer to the [whitepaper](https://github.com/linera-io/linera-internal/tree/main/whitepaper).
<!-- Refer to public whitepaper once this repository is made public -->

## Repository Structure

The Linera protocol repository is broken down into the following crates: (from low-level to high-level in the dependency graph)

1. [`linera-base`](./linera-base) Basic type definitions, including cryptography. This will slowly be transitioned out into the rest of the crate ecosystem.
2. [`linera-views`](./linera-views) A module enabling the mapping of complex data structures onto a KV store.
3. [`linera-execution`](./linera-execution) Persistent data and the corresponding logics for runtime and execution of smart contracts / applications.
4. [`linera-chain`](./linera-chain) Persistent data and the corresponding logics for chains of blocks, certificates, and cross-chain messaging.
5. [`linera-storage`](./linera-storage) Defines the storage abstraction and corresponding concrete implementations (DynamoDB, RocksDB, etc.) on top of `linera-chain`.
6. [`linera-core`](./linera-core) The Linera core protocol. Contains client / server logic, node synchronization etc.
7. [`linera-rpc`](./linera-rpc)  Defines the data-type for RPC messages (currently all client<->proxy<->chain<->chain interactions), and track the corresponding data schemas.
8. [`linera-service`](./linera-service) Executable for clients (aka CLI wallets), proxy (aka validator frontend) and servers.


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
   server_1.json:grpc:127.0.0.1:9100:grpc:127.0.0.1:9101:127.0.0.1:9102:127.0.0.1:9103:127.0.0.1:9104 \
   server_2.json:grpc:127.0.0.1:9200:grpc:127.0.0.1:9201:127.0.0.1:9202:127.0.0.1:9203:127.0.0.1:9204 \
   server_3.json:grpc:127.0.0.1:9300:grpc:127.0.0.1:9301:127.0.0.1:9302:127.0.0.1:9303:127.0.0.1:9304 \
   server_4.json:grpc:127.0.0.1:9400:grpc:127.0.0.1:9401:127.0.0.1:9402:127.0.0.1:9403:127.0.0.1:9404 \
--committee committee.json

# Create configuration files for 10 user chains.
# * Private chain states are stored in one local wallet `wallet.json`.
# * `genesis.json` will contain the initial balances of chains as well as the initial committee.
./client --wallet wallet.json --genesis genesis.json create_genesis_config 10 --initial-funding 10 --committee committee.json

# Start servers and create initial chains in DB
for I in 1 2 3 4
do
    ./grpc-proxy server_"$I".json &

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
# TODO uncomment
#${CLIENT[@]} benchmark --max-in-flight 50

# Create derived chain
CHAIN3="`${CLIENT[@]} open_chain --from "$CHAIN1"`"

# Inspect state of derived chain
fgrep '"chain_id":"'$CHAIN3'"' wallet.json

# Query the balance of the first chain
${CLIENT[@]} query_balance "$CHAIN1"

# Create two more validators
NAME5=$(./server generate --validators \
   server_5.json:grpc:127.0.0.1:9500:grpc:127.0.0.1:9501:127.0.0.1:9502:127.0.0.1:9503:127.0.0.1:9504)

NAME6=$(./server generate --validators \
   server_6.json:grpc:127.0.0.1:9600:grpc:127.0.0.1:9601:127.0.0.1:9602:127.0.0.1:9603:127.0.0.1:9604)

# Start the corresponding services
for I in 6 5
do
    ./proxy server_"$I".json || exit 1 &

    # hack!
    PID5="$!"

    for J in $(seq 0 3)
    do
        ./server run --storage rocksdb:server_"$I"_"$J".db --server server_"$I".json --shard "$J" --genesis genesis.json &
    done
done

sleep 1

${CLIENT[@]} set_validator --name "$NAME5" --address http://127.0.0.1:9500 --votes 100

${CLIENT[@]} query_balance "$CHAIN1"
${CLIENT[@]} query_validators
${CLIENT[@]} query_validators "$CHAIN1"

${CLIENT[@]} set_validator --name "$NAME6" --address http://127.0.0.1:9600 --votes 1
sleep 2

${CLIENT[@]} remove_validator --name "$NAME5"
kill "$PID5"

${CLIENT[@]} query_balance "$CHAIN1"
${CLIENT[@]} query_validators
${CLIENT[@]} query_validators "$CHAIN1"

cd ../..
```
