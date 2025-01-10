# Counter Example Application

This example application implements a simple counter contract, it is initialized with an
unsigned integer that can be increased by the `increment` operation.

## How It Works

It is a very basic Linera application, which is initialized by a `u64` which can be incremented
by a `u64`.

For example if the contract was initialized with 1, querying the contract would give us 1. Now if we want to
`increment` it by 3, we will have to perform an operation with the parameter being 3. Now querying the
application would give us 4 (1+3 = 4).

## Usage

### Setting Up

Before getting started, make sure that the binary tools `linera*` corresponding to
your version of `linera-sdk` are in your PATH. For scripting purposes, we also assume
that the BASH function `linera_spawn_and_read_wallet_variables` is defined.

From the root of Linera repository, this can be achieved as follows:

```bash
export PATH="$PWD/target/debug:$PATH"
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"
```

To start the local Linera network:

```bash
linera_spawn_and_read_wallet_variables linera net up --testing-prng-seed 37
```

We use the test-only CLI option `--testing-prng-seed` to make keys deterministic and simplify our
explanation.

```bash
CHAIN_1=e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
OWNER_1=7136460f0c87ae46f966f898d494c4b40c4ae8c527f4d1c0b1fa0f7cff91d20f
```

Now, compile the `counter` application WebAssembly binaries, publish and create an application instance.

```bash
(cd examples/counter && cargo build --release --target wasm32-unknown-unknown)

APPLICATION_ID=$(linera publish-and-create \
  examples/target/wasm32-unknown-unknown/release/counter_{contract,service}.wasm \
  --json-argument "1")
```

We have saved the `APPLICATION_ID` as it will be useful later.

### Using the Counter Application

First, a node service for the current wallet has to be started:

```bash
PORT=8080
linera service --port $PORT &
```

#### Using GraphiQL

Type each of these in the GraphiQL interface and substitute the env variables with their actual values that we've defined above.

- Navigate to the URL you get by running `echo "http://localhost:8080/chains/$CHAIN_1/applications/$APPLICATION_ID"`.
- To get the current value of `counter`, run the query:
```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APPLICATION_ID
query {
  value
}
```
- To increase the value of the counter by 3, perform the `increment` operation.
```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APPLICATION_ID
mutation Increment {
  increment(value: 3)
}
```
- Running the query again would yield `4`.


#### Using web frontend

Installing and starting the web server:

```bash
cd examples/counter/web-frontend
npm install --no-save

# Start the server but do not open the web page right away.
BROWSER=none npm start &
```

Web UIs for specific accounts can be opened by navigating URLs of the form
`http://localhost:3000/$CHAIN_1?app=$APPLICATION_ID&owner=$OWNER_1&port=$PORT` where
- the path is the ID of the chain where the account is located.
- the `app` argument is the token application ID obtained when creating the token.
- `owner` is the address of the chosen user account (owner must have permissions to create blocks in the given chain).
- `port` is the port of the wallet service (the wallet must know the secret key of `owner`).

The following command will print the URL of the web UI:

```bash
echo "http://localhost:3000/$CHAIN_1?app=$APPLICATION_ID&owner=$OWNER_1&port=$PORT"
```
