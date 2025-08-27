# Native Fungible Token Example Application

This app is very similar to the [Fungible Token Example Application](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#fungible-token-example-application). The difference is that this is a native token that will use system API calls for operations.
The general aspects of how it works can be referred to the linked README. Bash commands will always be included here for testing purposes.

## How It Works

Refer to [Fungible Token Example Application - How It Works](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#how-it-works).

## Setup and Deployment

Before getting started, make sure that the binary tools `linera*`
corresponding to your version of `linera-sdk` are in your PATH. For
scripting purposes, we also assume that the bash function
`linera_spawn` is defined.  From the root of Linera repository, this
can be achieved as follows:

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
#   LINERA_FAUCET_URL=https://faucet.testnet-XXX.linera.net  # for some value XXX
```

Create the user wallet and add chains to it:

```bash
export LINERA_WALLET="$LINERA_TMP_DIR/wallet.json"
export LINERA_KEYSTORE="$LINERA_TMP_DIR/keystore.json"
export LINERA_STORAGE="rocksdb:$LINERA_TMP_DIR/client.db"

linera wallet init --faucet $LINERA_FAUCET_URL

INFO_1=($(linera wallet request-chain --faucet $LINERA_FAUCET_URL))
INFO_2=($(linera wallet request-chain --faucet $LINERA_FAUCET_URL))
CHAIN_1="${INFO_1[0]}"
CHAIN_2="${INFO_2[0]}"
OWNER_1="${INFO_1[1]}"
OWNER_2="${INFO_2[1]}"
```

Compile the `native-fungible` application WebAssembly binaries, and publish them as an application
module:

```bash
cd examples/native-fungible

cargo build --release --target wasm32-unknown-unknown

MODULE_ID="$(linera publish-module \
    ../target/wasm32-unknown-unknown/release/native_fungible_{contract,service}.wasm)"
```

Here, we stored the new module ID in a variable `MODULE_ID` to be reused it later.

### Creating a Token

Most of this can also done according to the [fungible app
README](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#creating-a-token),
except for that at the end when creating the application, you always
need to pass `NAT` as the `ticker_symbol` because the Native Fungible
App has it hardcoded to that.

The app can't mint new native tokens, so the initial balance is taken
from the chain balance.

```bash
LINERA_APPLICATION_ID=$(linera create-application $MODULE_ID \
    --json-argument "{ \"accounts\": {
        \"$OWNER_1\": \"100.\"
    } }" \
    --json-parameters "{ \"ticker_symbol\": \"NAT\" }" \
)
```


## Connecting with the Web Frontend

You can run a local development server by exporting the necessary
variables and calling Vite:

```bash,ignore
export LINERA_APPLICATION_ID LINERA_FAUCET_URL
pnpm install
pnpm dev
```

This will start a server and print its address; access that URL to use
the application frontend.

## Connecting with the GraphQL Client

Alternatively, you can connect to the application using the GraphQL
client.  This is useful for inspecting and debugging the backend APIs.

Refer to [Fungible Token Example Application - Using the Token
Application](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#using-the-token-application)
for more detailed setup.

First, a node service for the current wallet has to be started:

```bash
PORT=8080
linera service --port $PORT &
echo "http://localhost:8080/chains/$CHAIN_1/applications/$LINERA_APPLICATION_ID"
```

Clicking the printed URL will take you to a
[GraphiQL](https://www.gatsbyjs.com/docs/how-to/querying-data/running-queries-with-graphiql/)
environment connected to your app.

### Using GraphiQL

Each of the following queries can be typed into the GraphiQL
interface, substituting the `$VARIABLE` references with the values of
the bash variables we've defined above.

To get the current balance of user `$OWNER_1`:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$LINERA_APPLICATION_ID
query {
  accounts {
    entry(
      key: "$OWNER_1"
    ) {
      value
    }
  }
}
```

To get the current balance of user `$OWNER_2`:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$LINERA_APPLICATION_ID
query {
  accounts {
    entry(
      key: "$OWNER_2"
    ) {
      value
    }
  }
}
```

To transfer 50 tokens from `$OWNER_1` to `$OWNER_2`:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$LINERA_APPLICATION_ID
mutation {
  transfer(
    owner: "$OWNER_1",
    amount: "50.",
    targetAccount: {
      chainId: "$CHAIN_1",
      owner: "$OWNER_2"
    }
  )
}
```

To get the new balance of user `$OWNER_1`:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$LINERA_APPLICATION_ID
query {
  accounts {
    entry(
      key: "$OWNER_1"
    ) {
      value
    }
  }
}
```

To get the new balance of user `$OWNER_2`:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$LINERA_APPLICATION_ID
query {
  accounts {
    entry(
      key: "$OWNER_2"
    ) {
      value
    }
  }
}
```
