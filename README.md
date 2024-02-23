[![License](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE)
[![Build Status for Rust](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml)
[![Build Status for Documentation](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml)
[![Build Status for DynamoDB](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml)
<!-- [![Build Status for Kubernetes](https://github.com/linera-io/linera-protocol/actions/workflows/kubernetes.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/kubernetes.yml) -->

# Linera

This repository is used to develop the main protocol behind Linera. Linera is a
decentralized blockchain infrastructure optimized for low-latency user interactions at scale.

To learn more about the Linera protocol, you may read

* our [developer manual](https://linera.dev), and

* our [whitepaper](https://linera.io/whitepaper).


## Repository Structure

This repository is broken down into the following main crates and subdirectories: (from low-level to high-level in the dependency graph)

* [`linera-base`](https://linera-io.github.io/linera-protocol/linera_base/index.html) Base definitions, including cryptography.

* [`linera-version`](https://linera-io.github.io/linera-protocol/linera_version/index.html) A library to manage version infos in binaries and services.

* [`linera-views`](https://linera-io.github.io/linera-protocol/linera_views/index.html) A library mapping complex data structures onto a key-value store. The corresponding procedural macros are implemented in `linera-view-derive`.

* [`linera-execution`](https://linera-io.github.io/linera-protocol/linera_execution/index.html) Persistent data and the corresponding logics for runtime and execution of smart contracts / applications.

* [`linera-chain`](https://linera-io.github.io/linera-protocol/linera_chain/index.html) Persistent data and the corresponding logics for chains of blocks, certificates, and cross-chain messaging.

* [`linera-storage`](https://linera-io.github.io/linera-protocol/linera_storage/index.html) Defines the storage abstractions for the protocol on top of `linera-chain`.

* [`linera-core`](https://linera-io.github.io/linera-protocol/linera_core/index.html) The core Linera protocol, including client and server logic, node synchronization, etc.

* [`linera-rpc`](https://linera-io.github.io/linera-protocol/linera_rpc/index.html) Defines the data-type for RPC messages (currently all client<->proxy<->chain<->chain interactions), and track the corresponding data schemas.

* [`linera-service`](https://linera-io.github.io/linera-protocol/linera_service/index.html) Executable for clients (aka CLI wallets), proxy (aka validator frontend) and servers.

* [`linera-sdk`](https://linera-io.github.io/linera-protocol/linera_sdk/index.html) The library to develop Linera applications written in Rust for the Wasm virtual machine.

* [`examples`](./examples) Examples of Linera applications written in Rust.

## Quickstart with the Linera service CLI

The following commands set up a local test network and run a few transfers between
microchains owned by a single wallet.

```bash
# Make sure to compile the Linera binaries and add them in the $PATH.
# cargo build -p linera-service --bins
export PATH="$PWD/target/debug:$PATH"

# Import the optional helper function `linera_spawn_and_read_wallet_variables`.
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"

# Run a local test network with the default parameters and a number of microchains
# owned by the default wallet.
# The helper function will set the 2 environment variables LINERA_{WALLET,STORAGE}.
linera_spawn_and_read_wallet_variables \
linera net up

# Print the set of validators.
linera query-validators

# Query the default balance (aka the "chain balance") of some of the chains.
CHAIN1="e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
CHAIN2="256e1dbc00482ddd619c293cc0df94d366afe7980022bb22d99e33036fd465dd"
linera query-balance "$CHAIN1"
linera query-balance "$CHAIN2"

# Transfer 10 units then 5 back
linera transfer 10 --from "$CHAIN1" --to "$CHAIN2"
linera transfer 5 --from "$CHAIN2" --to "$CHAIN1"

# Query balances again
linera query-balance "$CHAIN1"
linera query-balance "$CHAIN2"
```

More complex examples may be found in our [developer manual](https://linera.dev) as well
as the [example applications](./examples) in this repository.
