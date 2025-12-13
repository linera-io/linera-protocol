# Running devnets with Docker Compose

In this section, we use Docker Compose to run a simple devnet with a single
validator.

Docker Compose is a tool for defining and managing multi-container Docker
applications. It allows you to describe the services, networks, and volumes of
your application in a single YAML file (docker-compose.yml). With Docker
Compose, you can easily start, stop, and manage all the containers in your
application as a single unit using simple commands like docker-compose up and
docker-compose down.

For a more complete setup, consider using Kind as described
[in the next section](kind.md).

## Installation

This section covers everything you need to install to run a Linera network with
Docker Compose.

Note: This section has been tested only on Linux.

### Docker Compose Requirements

To install Docker Compose see the
[installing Docker Compose](https://docs.docker.com/compose/install/) section in
the Docker docs.

### Installing the Linera Toolchain

To install the Linera Toolchain refer to the
[installation section](../../developers/getting_started/installation.md#installing-from-github).

You want to install the toolchain from GitHub, as you'll be using the repository
to run the Docker Compose validator service.

## Running with Docker Compose

To run a local devnet with Docker Compose, navigate to the root of the
`linera-protocol` repository and run:

```bash
cd docker && ./compose.sh
```

This will take some time as Docker images are built from the Linera source. When
the service is ready, a temporary wallet and database are available under the
`docker` subdirectory.

Referencing these variables with the `linera` binary will enable you to interact
with the devnet:

```bash
$ linera --wallet wallet.json --storage rocksdb:linera.db sync
2024-06-07T14:19:32.751359Z  INFO linera: Synchronizing chain information
2024-06-07T14:19:32.771842Z  INFO linera::client_context: Saved user chain states
2024-06-07T14:19:32.771850Z  INFO linera: Synchronized chain information in 20 ms
$ linera --wallet wallet.json --storage rocksdb:linera.db query-balance
2024-06-07T14:19:36.958149Z  INFO linera: Evaluating the local balance of e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65 by staging execution of known incoming messages
2024-06-07T14:19:36.959481Z  INFO linera: Balance obtained after 1 ms
10.
```

The network is transient, so killing the script will perform a cleanup operation
destroying wallets, storage and volumes associated with the network.
