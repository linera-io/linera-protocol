[![License](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE)
[![Build Status for Rust](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml)
[![Build Status for Documentation](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml)
[![Build Status for DynamoDB](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml)
<!-- [![Build Status for Kubernetes](https://github.com/linera-io/linera-protocol/actions/workflows/kubernetes.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/kubernetes.yml) -->

# Linera

This repository is dedicated to developing the Linera protocol. For an overview of how
the Linera protocol functions refer to the [whitepaper](https://github.com/linera-io/linera-internal/tree/main/whitepaper).
<!-- Refer to public whitepaper once this repository is made public -->

## Repository Structure

The Linera protocol repository is broken down into the following crates and subdirectories: (from low-level to high-level in the dependency graph)

1. [`linera-base`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_base/index.html) Basic type definitions, including cryptography.

2. [`linera-views`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_views/index.html) A module enabling the mapping of complex data structures onto a KV store.

3. [`linera-execution`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_execution/index.html) Persistent data and the corresponding logics for runtime and execution of smart contracts / applications.

4. [`linera-chain`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_chain/index.html) Persistent data and the corresponding logics for chains of blocks, certificates, and cross-chain messaging.

5. [`linera-storage`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_storage/index.html) Defines the storage abstraction and corresponding concrete implementations (DynamoDB, RocksDB, etc.) on top of `linera-chain`.

6. [`linera-core`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_core/index.html) The Linera core protocol. Contains client / server logic, node synchronization etc.

7. [`linera-rpc`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_rpc/index.html)  Defines the data-type for RPC messages (currently all client<->proxy<->chain<->chain interactions), and track the corresponding data schemas.

8. [`linera-service`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_service/index.html) Executable for clients (aka CLI wallets), proxy (aka validator frontend) and servers.

9. [`linera-sdk`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_sdk/index.html) The library to develop Linera applications written in Rust for the WASM virtual machine.

10. [`linera-examples`](./linera-examples) Examples of Linera applications written in Rust.

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
./server generate \
    --validators ../../configuration/validator_1.toml ../../configuration/validator_2.toml ../../configuration/validator_3.toml ../../configuration/validator_4.toml \
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

# Command line prefix for client calls
CLIENT=(./client --storage rocksdb:client.db --wallet wallet.json --genesis genesis.json --max-pending-messages 10000)

${CLIENT[@]} query_validators

# Give some time for server startup
sleep 1

# Query balance for first and last user chain
CHAIN1="91c7b394ef500cd000e365807b770d5b76a6e8c9c2f2af8e58c205e521b5f646"
CHAIN2="170883d704512b1682064639bdda0aab27756727af8e0dc5732bae70b2e15997"
${CLIENT[@]} query_balance "$CHAIN1"
${CLIENT[@]} query_balance "$CHAIN2"

# Transfer 10 units then 5 back
${CLIENT[@]} transfer 10 --from "$CHAIN1" --to "$CHAIN2"
${CLIENT[@]} transfer 5 --from "$CHAIN2" --to "$CHAIN1"

# Query balances again
${CLIENT[@]} query_balance "$CHAIN1"
${CLIENT[@]} query_balance "$CHAIN2"

cd ../..
```
