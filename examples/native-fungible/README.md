# Native Fungible Token Example Application

This app is very similar to the [Fungible Token Example Application](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#fungible-token-example-application). The difference is that this is a native token that will use system API calls for operations.
The general aspects of how it works can be referred to the linked README. Bash commands will always be included here for testing purposes.

## How It Works

Refer to [Fungible Token Example Application - How It Works](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#how-it-works).

## Usage

### Setting Up

Before getting started, make sure that the binary tools `linera*` corresponding to
your version of `linera-sdk` are in your PATH. For scripting purposes, we also assume
that the BASH function `linera_spawn` is defined.

From the root of Linera repository, this can be achieved as follows:

```bash
export PATH="$PWD/target/debug:$PATH"
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"
```

Next, start the local Linera network and run a faucet:

```bash
FAUCET_PORT=8079
FAUCET_URL=http://localhost:$FAUCET_PORT
linera_spawn linera net up --with-faucet --faucet-port $FAUCET_PORT

# If you're using a testnet, run this instead:
#   LINERA_TMP_DIR=$(mktemp -d)
#   FAUCET_URL=https://faucet.testnet-XXX.linera.net  # for some value XXX
```

Create the user wallet and add chains to it:

```bash
export LINERA_WALLET="$LINERA_TMP_DIR/wallet.json"
export LINERA_STORAGE="rocksdb:$LINERA_TMP_DIR/client.db"

linera wallet init --faucet $FAUCET_URL

INFO_1=($(linera wallet request-chain --faucet $FAUCET_URL))
INFO_2=($(linera wallet request-chain --faucet $FAUCET_URL))
CHAIN_1="${INFO_1[0]}"
CHAIN_2="${INFO_2[0]}"
OWNER_1="${INFO_1[3]}"
OWNER_2="${INFO_2[3]}"
```

Compile the `native-fungible` application WebAssembly binaries, and publish them as an application
bytecode:

```bash
(cd examples/native-fungible && cargo build --release --target wasm32-unknown-unknown)

BYTECODE_ID="$(linera publish-bytecode \
    examples/target/wasm32-unknown-unknown/release/native_fungible_{contract,service}.wasm)"
```

Here, we stored the new bytecode ID in a variable `BYTECODE_ID` to be reused it later.

### Creating a Token

Most of this can also be referred to the [fungible app README](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#creating-a-token), except for at the end when creating the application, you always need to pass `NAT` as the `ticker_symbol` because the Native Fungible App has it hardcoded to that.

The app can't mint new native tokens, so the initial balance is taken from the chain balance.

```bash
APP_ID=$(linera create-application $BYTECODE_ID \
    --json-argument "{ \"accounts\": {
        \"User:$OWNER_1\": \"100.\"
    } }" \
    --json-parameters "{ \"ticker_symbol\": \"NAT\" }" \
)
```

### Using the Token Application

Refer to [Fungible Token Example Application - Using the Token Application](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#using-the-token-application).

```bash
PORT=8080
linera service --port $PORT &
```

#### Using GraphiQL

Type each of these in the GraphiQL interface and substitute the env variables with their actual values that we've defined above.

- Navigate to the URL you get by running `echo "http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID"`.
- To get the current balance of user $OWNER_1, run the query:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
query {
  accounts {
    entry(
      key: "User:$OWNER_1"
    ) {
      value
    }
  }
}
```

- To get the current balance of user $OWNER_2, run the query:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
query {
  accounts {
    entry(
      key: "User:$OWNER_2"
    ) {
      value
    }
  }
}
```

- To transfer 50 tokens from $OWNER_1 to $OWNER_2

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
mutation {
  transfer(
    owner: "User:$OWNER_1",
    amount: "50.",
    targetAccount: {
      chainId: "$CHAIN_1",
      owner: "User:$OWNER_2"
    }
  )
}
```

- To get the new balance of user $OWNER_1, run the query:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
query {
  accounts {
    entry(
      key: "User:$OWNER_1"
    ) {
      value
    }
  }
}
```

- To get the new balance of user $OWNER_2, run the query:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
query {
  accounts {
    entry(
      key: "User:$OWNER_2"
    ) {
      value
    }
  }
}
```

#### Using web frontend

Refer to [Fungible Token Example Application - Using web frontend](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#using-web-frontend).

```bash
cd examples/fungible/web-frontend
npm install --no-save

# Start the server but not open the web page right away.
BROWSER=none npm start &
```

```bash
echo "http://localhost:3000/$CHAIN_1?app=$APP_ID&owner=$OWNER_1&port=$PORT"
echo "http://localhost:3000/$CHAIN_1?app=$APP_ID&owner=$OWNER_2&port=$PORT"
```
