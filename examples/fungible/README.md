# Fungible Token Example Application

This example application implements fungible tokens. This demonstrates in particular
cross-chain messages and how applications are instantiated and auto-deployed.

Once this application is built and its bytecode published on a Linera chain, the
published bytecode can be used to create multiple application instances, where each
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

The WebAssembly binaries for the bytecode can be built and published using [steps from the
book](https://linera-io.github.io/linera-documentation/getting_started/first_app.html),
summarized below.

Before getting started, make sure that the binary tools `linera*` corresponding to
your version of `linera-sdk` are in your PATH. For scripting purposes, we also assume
that the BASH function `linera_spawn_and_read_wallet_variables` is defined.

From the root of Linera repository, this can be achieved as follows:

```bash
export PATH="$PWD/target/debug:$PATH"
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"
```

You may also use `cargo install linera-service` and append the output of
`linera net helper` to your `~/.bash_profile`.

Now, we are ready to set up a local network with an initial wallet owning several initial
chains. In a new BASH shell, enter:

```bash
linera_spawn_and_read_wallet_variables linera net up --testing-prng-seed 37
```

A new test network is now running and the environment variables `LINERA_WALLET` and
`LINERA_STORAGE` are now defined for the duration of the shell session. We used the
test-only CLI option `--testing-prng-seed` to make keys deterministic and simplify our
presentation.

Now, compile the `fungible` application WebAssembly binaries, and publish them as an application
bytecode:

```bash
(cd examples/fungible && cargo build --release --target wasm32-unknown-unknown)

BYTECODE_ID=$(linera publish-bytecode \
    examples/target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm)
```

Here, we stored the new bytecode ID in a variable `BYTECODE_ID` to be reused it later.

### Creating a Token

In order to use the published bytecode to create a token application, the initial state must be
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

Let's define some variables corresponding to these values. (Note that owner addresses
would not be predictable without `--testing-prng-seed` above.)

```bash
CHAIN_1=e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65  # default chain for the wallet
OWNER_1=7136460f0c87ae46f966f898d494c4b40c4ae8c527f4d1c0b1fa0f7cff91d20f  # owner of chain 1
CHAIN_2=256e1dbc00482ddd619c293cc0df94d366afe7980022bb22d99e33036fd465dd  # another chain in the wallet
OWNER_2=598d18f67709fe76ed6a36b75a7c9889012d30b896800dfd027ee10e1afd49a3  # owner of chain 2
```

The example below creates a token application on the default chain CHAIN_1 and gives the owner 100 tokens:

```bash
APP_ID=$(linera create-application $BYTECODE_ID \
    --json-argument "{ \"accounts\": {
        \"User:$OWNER_1\": \"100.\"
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
