# <img src="https://github.com/linera-io/linera-protocol/assets/1105398/fe08c941-93af-4114-bb83-bcc0eaec95f9" width="250" height="90" />

[![License](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE)
[![Build Status for Rust](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml)
[![Build Status for Documentation](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml)
[![Build Status for DynamoDB](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml)
<!-- [![Build Status for Kubernetes](https://github.com/linera-io/linera-protocol/actions/workflows/kubernetes.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/kubernetes.yml) -->

[Linera](https://linera.io) is a decentralized blockchain infrastructure designed for highly scalable,
low-latency Web3 applications.

Visit our [developer page](https://linera.dev) and read our
[whitepaper](https://linera.io/whitepaper) to learn more about the Linera protocol.
Visit our [Guild](https://guild.xyz/linera), 
[X](https://x.com/linera_io) and 
[Discord](https://discord.gg/KUkghUdk2G)

## Repository Structure

The main crates and directories of this repository can be summarized as follows: (listed
from low to high levels in the dependency graph)

* [`linera-base`](https://linera-io.github.io/linera-protocol/linera_base/index.html) Base
  definitions, including cryptography.

* [`linera-version`](https://linera-io.github.io/linera-protocol/linera_version/index.html)
  A library to manage version info in binaries and services.

* [`linera-views`](https://linera-io.github.io/linera-protocol/linera_views/index.html) A
  library mapping complex data structures onto a key-value store. The corresponding
  procedural macros are implemented in `linera-views-derive`.

* [`linera-execution`](https://linera-io.github.io/linera-protocol/linera_execution/index.html)
  Persistent data and the corresponding logic for runtime and execution of Linera
  applications.

* [`linera-chain`](https://linera-io.github.io/linera-protocol/linera_chain/index.html)
  Persistent data and the corresponding logic for chains of blocks, certificates, and
  cross-chain messaging.

* [`linera-storage`](https://linera-io.github.io/linera-protocol/linera_storage/index.html)
  Defines the storage abstractions for the protocol on top of `linera-chain`.

* [`linera-core`](https://linera-io.github.io/linera-protocol/linera_core/index.html) The
  core Linera protocol, including client and server logic, node synchronization, etc.

* [`linera-rpc`](https://linera-io.github.io/linera-protocol/linera_rpc/index.html)
  Defines the data-type for RPC messages (currently all client &#x2194; proxy &#x2194;
  chain &#x2194; chain interactions), and track the corresponding data schemas.

* [`linera-client`](https://linera-io.github.io/linera-protocol/linera_client/index.html)
  Library for writing Linera clients.  Used for the command-line
  client and the node service in `linera-service`, as well as the Web
  client in [`linera-web`](https://github.com/linera-io/linera-web/).

* [`linera-service`](https://linera-io.github.io/linera-protocol/linera_service/index.html)
  Executable for clients (aka CLI wallets), proxy (aka validator frontend) and servers.

* [`linera-sdk`](https://linera-io.github.io/linera-protocol/linera_sdk/index.html) The
  library to develop Linera applications written in Rust for the Wasm virtual machine. The
  corresponding procedural macros are implemented in `linera-sdk-derive`.

* [`examples`](./examples) Examples of Linera applications written in Rust.


## Quickstart with the Linera CLI tool

The following commands set up a local test network and run some transfers between the
microchains owned by a single wallet.

```bash
# Make sure to compile the Linera binaries and add them in the $PATH.
# cargo build -p linera-storage-service -p linera-service --bins
export PATH="$PWD/target/debug:$PATH"

# Import the optional helper function `linera_spawn`.
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"

# Run a local test network with the default parameters and a number of microchains
# owned by the default wallet. This also defines `LINERA_TMP_DIR`.
linera_spawn \
linera net up --with-faucet --faucet-port 8080

# Remember the URL of the faucet.
FAUCET_URL=http://localhost:8080

# If you're using a testnet, start here and run this instead:
#   LINERA_TMP_DIR=$(mktemp -d)
#   FAUCET_URL=https://faucet.testnet-XXX.linera.net  # for some value XXX

# Set the path of the future wallet.
export LINERA_WALLET="$LINERA_TMP_DIR/wallet.json"
export LINERA_STORAGE="rocksdb:$LINERA_TMP_DIR/client.db"

# Initialize a new user wallet.
linera wallet init --faucet $FAUCET_URL

# Request chains.
INFO1=($(linera wallet request-chain --faucet $FAUCET_URL))
INFO2=($(linera wallet request-chain --faucet $FAUCET_URL))
CHAIN1="${INFO1[0]}"
ACCOUNT1="User:${INFO1[3]}"
CHAIN2="${INFO2[0]}"
ACCOUNT2="User:${INFO2[3]}"

# Show the different chains tracked by the wallet.
linera wallet show

# Query the chain balance of some of the chains.
linera query-balance "$CHAIN1"
linera query-balance "$CHAIN2"

# Transfer 10 units then 5 back.
linera transfer 10 --from "$CHAIN1" --to "$CHAIN2"
linera transfer 5 --from "$CHAIN2" --to "$CHAIN1"

# Query balances again.
linera query-balance "$CHAIN1"
linera query-balance "$CHAIN2"

# Now let's fund the user balances.
linera transfer 5 --from "$CHAIN1" --to "$CHAIN1:$ACCOUNT1"
linera transfer 2 --from "$CHAIN1:$ACCOUNT1" --to "$CHAIN2:$ACCOUNT2"

# Query user balances again.
linera query-balance "$CHAIN1:$ACCOUNT1"
linera query-balance "$CHAIN2:$ACCOUNT2"

# TODO(#1713): The syntax `User:$OWNER` for user accounts will change in the future.
```

More complex examples may be found in our [developer manual](https://linera.dev) as well
as the [example applications](./examples) in this repository.
