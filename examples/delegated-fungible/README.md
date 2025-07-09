# Fungible Token Example Application

This example application implements fungible tokens. This demonstrates in particular
cross-chain messages and how applications are instantiated and auto-deployed.

Once this application is built and its module published on a Linera chain, the
published module can be used to create multiple application instances, where each
instance represents a different fungible token.

## How It Works

Individual chains have a set of accounts, where each account has an owner and a balance. The
same owner can have accounts on multiple chains, with a different balance on each chain. This
means that an account's balance is sharded across one or more chains.

There are two operations: `Transfer` and `Claim`. `Transfer` sends tokens from an account on the
chain where the operation is executed, while `Claim` sends a message from the current chain to
another chain in order to transfer tokens from that remote chain.

Tokens can be transferred from an account to different destinations, such as:

- other accounts on the same chain,
- the same account on another chain,
- other accounts on other chains.

## Usage

### Setting Up

The WebAssembly binaries for the module can be built and published using [steps from the
book](https://linera.dev/developers/getting_started.html),
summarized below.

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
export LINERA_KEYSTORE="$LINERA_TMP_DIR/keystore.json"
export LINERA_STORAGE="rocksdb:$LINERA_TMP_DIR/client.db"

linera wallet init --faucet $FAUCET_URL

INFO_1=($(linera wallet request-chain --faucet $FAUCET_URL))
CHAIN_1="${INFO_1[0]}"
OWNER_1="${INFO_1[1]}"
INFO_2=($(linera wallet request-chain --faucet $FAUCET_URL))
CHAIN_2="${INFO_2[0]}"
OWNER_2="${INFO_2[1]}"
```

Now, compile the `fungible` application WebAssembly binaries, and publish them as an application
bytecode:

```bash
(cd examples/fungible && cargo build --release --target wasm32-unknown-unknown)

MODULE_ID=$(linera publish-module \
    examples/target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm)
```

Here, we stored the new module ID in a variable `MODULE_ID` to be reused it later.

### Creating a Token

In order to use the published module to create a token application, the initial state must be
specified. This initial state is where the tokens are minted. After the token is created, no
additional tokens can be minted and added to the application. The initial state is a JSON string
that specifies the accounts that start with tokens.

In order to select the accounts to have initial tokens, the command below can be used to list
the chains created for the test in the default wallet:

```bash
linera wallet show
```

A table will be shown with the chains registered in the wallet and their meta-data. The default
chain should be highlighted in green. Each chain has an `Owner` field, and that is what is used
for the account.

The example below creates a token application on the default chain CHAIN_1 and gives the owner 100 tokens:

```bash
APP_ID=$(linera create-application $MODULE_ID \
    --json-argument "{ \"accounts\": {
        \"$OWNER_1\": \"100.\"
    } }" \
    --json-parameters "{ \"ticker_symbol\": \"FUN\" }" \
)
```

This will store the application ID in a new variable `APP_ID`.

### Using the Token Application

Before using the token, a source and target address should be selected. The source address
should ideally be on the default chain (used to create the token) and one of the accounts chosen
for the initial state, because it will already have some initial tokens to send.

First, a node service for the current wallet has to be started:

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
      key: "$OWNER_1"
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
      key: "$OWNER_2"
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
    owner: "$OWNER_1",
    amount: "50.",
    targetAccount: {
      chainId: "$CHAIN_1",
      owner: "$OWNER_2"
    }
  )
}
```

- To get the new balance of user $OWNER_1, run the query:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
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

- To get the new balance of user $OWNER_2, run the query:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
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

#### Using web frontend

Installing and starting the web server:

```bash
cd examples/fungible/web-frontend
npm install --no-save

# Start the server but not open the web page right away.
BROWSER=none npm start &
```

Web UIs for specific accounts can be opened by navigating URLs of the form
`http://localhost:3000/$CHAIN?app=$APP_ID&owner=$OWNER&port=$PORT` where

- the path is the ID of the chain where the account is located.
- the argument `app` is the token application ID obtained when creating the token.
- `owner` is the address of the chosen user account (owner must have permissions to create blocks in the given chain).
- `port` is the port of the wallet service (the wallet must know the secret key of `owner`).

In this example, two web pages for OWNER_1 and OWNER_2 can be opened by navigating these URLs:

```bash
echo "http://localhost:3000/$CHAIN_1?app=$APP_ID&owner=$OWNER_1&port=$PORT"
echo "http://localhost:3000/$CHAIN_1?app=$APP_ID&owner=$OWNER_2&port=$PORT"
```

OWNER_2 doesn't have the applications loaded initially. Using the first page to
transfer tokens from OWNER_1 to OWNER_2 at CHAIN_2 will instantly update the UI of the
second page.
