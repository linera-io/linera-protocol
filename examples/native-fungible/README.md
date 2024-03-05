<!-- cargo-rdme start -->

# Native Fungible Token Example Application

This app is very similar to the [Fungible Token Example Application](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#fungible-token-example-application). The difference is that this is a native token that will use system API calls for operations.
The general aspects of how it works can be referred to the linked README.

# How It Works

Refer to [Fungible Token Example Application - How It Works](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#how-it-works).

# Usage

## Setting Up

Most of this can also be referred to the [fungible app README](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#setting-up), except for at the end when compiling and publishing the bytecode, what you'll need to do will be slightly different:

Compile the `native-fungible` application WebAssembly binaries, and publish them as an application
bytecode:

```bash
(cd examples/native-fungible && cargo build --release)

BYTECODE_ID="$(linera publish-bytecode \
    examples/target/wasm32-unknown-unknown/release/native_fungible_{contract,service}.wasm)"
```

Here, we stored the new bytecode ID in a variable `BYTECODE_ID` to be reused it later.

## Creating a Token

Most of this can also be referred to the [fungible app README](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#creating-a-token), except for at the end when creating the application, you always need to pass `NAT` as the `ticker_symbol` because the Native Fungible App has it hardcoded to that.

The app can't mint new native tokens, so the initial balance is taken from the chain balance.

```bash
APP_ID=$(linera create-application $BYTECODE_ID \
    --json-argument '{ "accounts": {
        "User:$OWNER_1": "100."
    } }' \
    --json-parameters '{ "ticker_symbol": "NAT" }' \
)
```

## Using the Token Application

Refer to [Fungible Token Example Application - Using the Token Application](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#using-the-token-application).

### Using GraphiQL

Refer to [Fungible Token Example Application - Using GraphiQL](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#using-graphiql).

### Using web frontend

Refer to [Fungible Token Example Application - Using web frontend](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#using-web-frontend).

<!-- cargo-rdme end -->
