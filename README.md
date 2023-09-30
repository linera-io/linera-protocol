[![License](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE)
[![Build Status for Rust](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml)
[![Build Status for Documentation](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml)
[![Build Status for DynamoDB](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml)
<!-- [![Build Status for Kubernetes](https://github.com/linera-io/linera-protocol/actions/workflows/kubernetes.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/kubernetes.yml) -->

# Linera

This repository is dedicated to developing the Linera protocol. For an overview of how
the Linera protocol functions refer to the [whitepaper](https://linera.io/whitepaper).

## Repository Structure

The Linera protocol repository is broken down into the following main crates and subdirectories: (from low-level to high-level in the dependency graph)

1. [`linera-base`](https://linera-io.github.io/linera-protocol/linera_base/index.html) Base definitions, including cryptography.

2. [`linera-views`](https://linera-io.github.io/linera-protocol/linera_views/index.html) A library mapping complex data structures onto a key-value store. The corresponding procedural macros are implemented in `linera-view-derive`.

3. [`linera-execution`](https://linera-io.github.io/linera-protocol/linera_execution/index.html) Persistent data and the corresponding logics for runtime and execution of smart contracts / applications.

4. [`linera-chain`](https://linera-io.github.io/linera-protocol/linera_chain/index.html) Persistent data and the corresponding logics for chains of blocks, certificates, and cross-chain messaging.

5. [`linera-storage`](https://linera-io.github.io/linera-protocol/linera_storage/index.html) Defines the storage abstractions for the protocol on top of `linera-chain`.

6. [`linera-core`](https://linera-io.github.io/linera-protocol/linera_core/index.html) The core Linera protocol, including client and server logic, node synchronization, etc.

7. [`linera-rpc`](https://linera-io.github.io/linera-protocol/linera_rpc/index.html) Defines the data-type for RPC messages (currently all client<->proxy<->chain<->chain interactions), and track the corresponding data schemas.

8. [`linera-service`](https://linera-io.github.io/linera-protocol/linera_service/index.html) Executable for clients (aka CLI wallets), proxy (aka validator frontend) and servers.

9. [`linera-sdk`](https://linera-io.github.io/linera-protocol/linera_sdk/index.html) The library to develop Linera applications written in Rust for the Wasm virtual machine.

10. [`examples`](./examples) Examples of Linera applications written in Rust.

Additionally,
- `linera-service-graphql-client` is a Rust GraphQL client for the system components of the `linera-service`
- `linera-explorer` contains an experimental block explorer.
- `linera-indexer` is an experimental block indexer.
- `linera-witty` is an experimental alternative to `wit-bindgen` for rust hosts.

## Quickstart

See our [developer manual](https://linera.dev) for a full introduction to the Linera SDK.

The following script creates a local test network and runs a simple transfers between two user chains.

It can be run with `cargo test readme`.

```bash
if [ $# -eq 1 ]
then
    STORAGE="$1"
else
    STORAGE="ROCKSDB"
fi

# Build the repository and clean up existing databases if needed.
if [ "$STORAGE" = "ROCKSDB" ]
then
    cargo build -p linera-service
elif [ "$STORAGE" = "DYNAMODB" ]
then
    cargo build -p linera-service --features aws
    target/debug/linera-db delete_all --storage dynamodb:table:localstack
elif [ "$STORAGE" = "SCYLLADB" ]
then
    cargo build -p linera-service --features scylladb
    target/debug/linera-db delete_all --storage scylladb:
fi

# Change working directory
cd target/debug

# Clean up files
rm -rf *.json *.txt *.db

# Make sure to clean up child processes on exit.
trap 'kill $(jobs -p)' EXIT

# Create configuration files for 4 validators with 1 shard each.
# * Private server states are stored in `server*.json`.
# * `committee.json` is the public description of the Linera committee.
./linera-server generate --validators ../../configuration/local/validator_{1,2,3,4}.toml --committee committee.json

# Command line prefix for client calls
CLIENT=(./linera --storage rocksdb:linera.db --wallet wallet.json)

# Create configuration files for 10 user chains.
# * Private chain states are stored in one local wallet `wallet.json`.
# * `genesis.json` will contain the initial balances of chains as well as the initial committee.
${CLIENT[@]} create-genesis-config 10 --genesis genesis.json --initial-funding 10 --committee committee.json

# Start servers and create initial chains in DB
for I in 1 2 3 4
do
    # Start validator proxy
    ./linera-proxy server_"$I".json &

    if [ "$STORAGE" = "ROCKSDB" ]
    then
        STORE=rocksdb:server_"$I".db
    elif [ "$STORAGE" = "DYNAMODB" ]
    then
        STORE=dynamodb:server-"$I":localstack
    elif [ "$STORAGE" = "SCYLLADB" ]
    then
        STORE=scylladb:table_server_"$I"
    fi

    # Initialize validator storage.
    ./linera-server initialize --storage "$STORE" --genesis genesis.json

    # Start the server of the unique shard.
    ./linera-server run --storage "$STORE" --server server_"$I".json --genesis genesis.json &
done

${CLIENT[@]} query-validators

# Give some time for server startup
sleep 5

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
