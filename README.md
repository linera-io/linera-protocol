[![License](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE)
[![Build Status for Rust](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml)
[![Build Status for Documentation](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml)
[![Build Status for DynamoDB](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml)
<!-- [![Build Status for Kubernetes](https://github.com/linera-io/linera-protocol/actions/workflows/kubernetes.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/kubernetes.yml) -->

# Linera

This repository is dedicated to developing the Linera protocol. For an overview of how
the Linera protocol functions refer to the [whitepaper](https://linera.io/whitepaper).

## Repository Structure

The Linera protocol repository is broken down into the following crates and subdirectories: (from low-level to high-level in the dependency graph)

1. [`linera-base`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_base/index.html) Base definitions, including cryptography.

2. [`linera-views`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_views/index.html) A library mapping complex data structures onto a key-value store. The corresponding procedural macros are implemented in `linera-view-derive`.

3. [`linera-execution`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_execution/index.html) Persistent data and the corresponding logics for runtime and execution of smart contracts / applications.

4. [`linera-chain`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_chain/index.html) Persistent data and the corresponding logics for chains of blocks, certificates, and cross-chain messaging.

5. [`linera-storage`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_storage/index.html) Defines the storage abstractions for the protocol on top of `linera-chain`.

6. [`linera-core`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_core/index.html) The core Linera protocol, including client and server logic, node synchronization, etc.

7. [`linera-rpc`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_rpc/index.html) Defines the data-type for RPC messages (currently all client<->proxy<->chain<->chain interactions), and track the corresponding data schemas.

8. [`linera-service`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_service/index.html) Executable for clients (aka CLI wallets), proxy (aka validator frontend) and servers.

9. [`linera-sdk`](https://linera-io.github.io/linera-protocol/364a04086bc8f2bf91ec3406a2aac5f7e4e675b9/linera_sdk/index.html) The library to develop Linera applications written in Rust for the WASM virtual machine.

10. [`examples`](./examples) Examples of Linera applications written in Rust.

## Quickstart with the Linera service CLI

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
# * `committee.json` is the public description of the Linera committee.
./server generate --validators ../../configuration/validator_{1,2,3,4}.toml --committee committee.json

# Create configuration files for 10 user chains.
# * Private chain states are stored in one local wallet `wallet.json`.
# * `genesis.json` will contain the initial balances of chains as well as the initial committee.
./linera --wallet wallet.json create-genesis-config 10 --genesis genesis.json --initial-funding 10 --committee committee.json

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
CLIENT=(./linera --storage rocksdb:linera.db --wallet wallet.json --max-pending-messages 10000)

${CLIENT[@]} query-validators

# Give some time for server startup
sleep 1

# Query balance for first and last user chain, root chains 0 and 9
CHAIN1="e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
CHAIN2="256e1dbc00482ddd619c293cc0df94d366afe7980022bb22d99e33036fd465dd"
${CLIENT[@]} query-balance "$CHAIN1"
${CLIENT[@]} query-balance "$CHAIN2"

# Transfer 10 units then 5 back
${CLIENT[@]} transfer 10 --from "$CHAIN1" --to "$CHAIN2"
${CLIENT[@]} transfer 5 --from "$CHAIN2" --to "$CHAIN1"

# Query balances again
${CLIENT[@]} query-balance "$CHAIN1"
${CLIENT[@]} query-balance "$CHAIN2"

cd ../..
```
