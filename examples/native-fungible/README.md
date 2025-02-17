# Native Fungible Token Example Application

This app is very similar to the [Fungible Token Example Application](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#fungible-token-example-application). The difference is that this is a native token that will use system API calls for operations.
The general aspects of how it works can be referred to the linked README. Bash commands will always be included here for testing purposes.

## How It Works

Refer to [Fungible Token Example Application - How It Works](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#how-it-works).

## Usage

### Setting Up

Most of this can also be referred to the [fungible app README](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#setting-up), except for at the end when compiling and publishing the bytecode, what you'll need to do will be slightly different.

```bash
export PATH="$PWD/target/debug:$PATH"
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"

linera_spawn_and_read_wallet_variables linera net up --testing-prng-seed 37
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

```bash
linera wallet show
CHAIN_1=aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8  # default chain for the wallet
OWNER_1=de166237331a2966d8cf6778e81a8c007b4084be80dc1e0409d51f216c1deaa1  # owner of chain 1
CHAIN_2=63620ea465af9e9e0e8e4dd8d21593cc3a719feac5f096df8440f90738f4dbd8  # another chain in the wallet
OWNER_2=598d18f67709fe76ed6a36b75a7c9889012d30b896800dfd027ee10e1afd49a3  # owner of chain 2
```

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
