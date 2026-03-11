# Hello, Linera

In this section, you will learn how to initialize a developer wallet, interact
with the current Testnet, run a local development network, then compile and
deploy your first application from scratch.

By the end of this section, you will have a
[microchain](../core_concepts/microchains.md) on the Testnet and/or on your
local network, and a working application that can be queried using GraphQL.

## Creating a wallet on the latest Testnet

To interact with the latest Testnet, you will need a developer wallet, a new
microchain, and some tokens. These can be all obtained at once by querying the
Testnet's **faucet** service as follows:

```bash
linera wallet init --faucet https://faucet.{{#include ../../RELEASE_DOMAIN}}.linera.net
linera wallet request-chain --faucet https://faucet.{{#include ../../RELEASE_DOMAIN}}.linera.net
```

If you obtain an error message instead, make sure to use a Linera toolchain
[compatible with the current Testnet](installation.md#installing-from-cratesio).

```admonish info
A Linera Testnet is a deployment of the Linera protocol used for testing. A deployment
consists of a number of [validators](../advanced_topics/validators.md), each of which runs
a frontend service (aka. `linera-proxy`), a number of workers (aka. `linera-server`), and
a shared database (by default `linera-storage-service`).
```

## Using a local test network

Another option is to start your own local development network. To do so, run the
following command:

```bash
linera net up --with-faucet --faucet-port 8080
```

This will start a validator with the default number of shards and start a
faucet.

Now, we're ready to create a developer wallet by running the following command
in a separate shell:

```bash
linera wallet init --faucet http://localhost:8080
linera wallet request-chain --faucet http://localhost:8080
```

```admonish warn
A wallet is valid for the lifetime of its network. Every time a local
network is restarted, the wallet needs to be removed and created again.
```

## Working with several developer wallets and several networks

By default, the `linera` command looks for wallet files located in a
configuration path determined by your operating system. If you prefer to choose
the location of your wallet files, you may optionally set the variables
`LINERA_WALLET`, `LINERA_KEYSTORE` and `LINERA_STORAGE` as follows:

```bash
DIR=$HOME/my_directory
mkdir -p $DIR
export LINERA_WALLET="$DIR/wallet.json"
export LINERA_KEYSTORE="$DIR/keystore.json"
export LINERA_STORAGE="rocksdb:$DIR/wallet.db"
```

Choosing such a directory can be useful to work with several networks because a
wallet is always specific to the network where it was created.

```admonish warn
We refer to the wallets created by the `linera` CLI as "developer wallets" because
they are operated from a developer tool and merely meant for testing and development.

Production-grade user wallets are generally operated by a browser
extension, a mobile application, or a hardware device.
```

## Interacting with the Linera network

To check that the network is working, you can synchronize your chain with the
rest of the network and display the chain balance as follows:

```bash
linera sync
linera query-balance
```

You should see an output number, e.g. `10`.

## Building an example application

Applications running on Linera are [Wasm](https://webassembly.org/) bytecode.
Each validator and client has a built-in Wasm virtual machine (VM) which can
execute bytecode.

Let's build the `counter` application from the `examples/` subdirectory of the
[Linera testnet
branch](https://github.com/linera-io/linera-protocol/tree/{{#include
../../RELEASE_BRANCH}}):

```bash
cd examples/counter && cargo build --release --target wasm32-unknown-unknown
```

## Publishing your application

You can publish the bytecode and create an application using it on your local
network using the `linera` client's `publish-and-create` command and provide:

1. The location of the contract bytecode
2. The location of the service bytecode
3. The JSON encoded initialization arguments

```bash
linera publish-and-create \
  target/wasm32-unknown-unknown/release/counter_{contract,service}.wasm \
  --json-argument "42"
```

Congratulations! You've published your first application on Linera!

## Querying your application

Now let's query your application to get the current counter value. To do that,
we need to use the client running in
[_service_ mode](../core_concepts/node_service.md). This will expose a bunch of
APIs locally which we can use to interact with applications on the network.

```bash
linera service --port 8080
```

<!-- TODO: add graphiql image here -->

Navigate to `http://localhost:8080` in your browser to access GraphiQL, the
[GraphQL](https://graphql.org) IDE. We'll look at this in more detail in a
[later section](../core_concepts/node_service.md#graphiql-ide); for now, list
the applications deployed on your default chain by running:

```gql
query {
  applications(chainId: "...") {
    id
    description
    link
  }
}
```

where `...` are replaced by the chain ID shown by `linera wallet show`.

Since we've only deployed one application, the results returned have a single
entry.

At the bottom of the returned JSON there is a field `link`. To interact with
your application copy and paste the link into a new browser tab.

Finally, to query the counter value, run:

```gql
query {
  value
}
```

This will return a value of `42`, which is the initialization argument we
specified when deploying our application.
