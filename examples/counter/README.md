# Counter Example Application

This example application implements a simple counter contract, it is initialized with an
unsigned integer that can be increased by the `increment` operation.

## How It Works

It is a simple Linera application, which is initialized by a `u64` which can be incremented
by a `u64`.

For example, if the contract was initialized with 1, querying the contract would give us 1. Now if we want to
`increment` it by 3, we will have to perform an operation with the parameter being 3. Now querying the
application would give us 4 (1+3 = 4).

## Setup and Deployment

Before getting started, make sure that the binary tools `linera*` corresponding to
your version of `linera-sdk` are in your PATH. For scripting purposes, we also assume
that the BASH function `linera_spawn` is defined.

From the root of Linera repository, this can be achieved as follows:

```bash
export PATH="$PWD/target/debug:$PATH"
eval "$(linera net helper 2>/dev/null)"
```

Next, start the local Linera network and run a faucet:

```bash
LINERA_FAUCET_PORT=8079
LINERA_FAUCET_URL=http://localhost:$LINERA_FAUCET_PORT
linera_spawn linera net up --with-faucet --faucet-port $LINERA_FAUCET_PORT

# If you're using a testnet, run this instead:
#   LINERA_TMP_DIR=$(mktemp -d)
#   export LINERA_FAUCET_URL=https://faucet.testnet-XXX.linera.net  # for some value XXX
```

Enable logs for user applications:

```bash
export LINERA_APPLICATION_LOGS=true
```

Create the user wallet and add chains to it:

```bash
export LINERA_WALLET="$LINERA_TMP_DIR/wallet.json"
export LINERA_KEYSTORE="$LINERA_TMP_DIR/keystore.json"
export LINERA_STORAGE="rocksdb:$LINERA_TMP_DIR/client.db"

linera wallet init --faucet $LINERA_FAUCET_URL

INFO=($(linera wallet request-chain --faucet $LINERA_FAUCET_URL))
CHAIN="${INFO[0]}"
OWNER="${INFO[1]}"
```

Now, compile the `counter` application WebAssembly binaries, publish and create an application instance.

```bash
cd examples/counter
cargo build --release --target wasm32-unknown-unknown

LINERA_APPLICATION_ID=$(linera publish-and-create \
  ../target/wasm32-unknown-unknown/release/counter_{contract,service}.wasm \
  --json-argument "1")
```

We have saved the `LINERA_APPLICATION_ID` as it will be useful later.

## Connecting with the Web Frontend

If you install the dependencies for the Web frontend and export the
necessary variables you can run a local development server using Vite:

```bash,ignore
export LINERA_APPLICATION_ID LINERA_FAUCET_URL
pnpm install
pnpm dev
```

This will start a server and print its address; access that URL to use
the application frontend.

Alternatively, there is a frontend in `metamask` that signs
transactions using the user's MetaMask wallet; it can be accessed the
same way.

## Connecting with the GraphQL Client

Alternatively, you can connect to the application using the GraphQL
client.  This is useful for inspecting and debugging the backend APIs.

First, a node service for the current wallet has to be started:

```bash
PORT=8080
linera service --port $PORT &
echo "http://localhost:8080/chains/$CHAIN/applications/$LINERA_APPLICATION_ID"
```

Clicking the printed URL will take you to a
[GraphiQL](https://www.gatsbyjs.com/docs/how-to/querying-data/running-queries-with-graphiql/)
environment connected to your app.

To get the current value of `counter`, you can run the query:

```gql,uri=http://localhost:8080/chains/$CHAIN/applications/$LINERA_APPLICATION_ID
query {
  value
}
```

To increase the value of the counter, you can run the `increment`
mutation. Note that the result of a mutation is the hash of the
resulting block, if any.

```gql,uri=http://localhost:8080/chains/$CHAIN/applications/$LINERA_APPLICATION_ID
mutation Increment {
  increment(value: 3)
}
```

If you run the query again, you'll now see a value of 4.
