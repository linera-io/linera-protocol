<!-- cargo-rdme start -->

# Fungible Token Example Application

This example application implements fungible tokens. This demonstrates in particular
cross-chain messages and how applications are instantiated and auto-deployed.

Once this application is built and its bytecode published on a Linera chain, the
published bytecode can be used to create multiple application instances, where each
instance represents a different fungible token.

# How It Works

Individual chains have a set of accounts, where each account has an owner and a balance. The
same owner can have accounts on multiple chains, with a different balance on each chain. This
means that an account's balance is sharded across one or more chains.

There are two operations: `Transfer` and `Claim`. `Transfer` sends tokens from an account on the
chain where the operation is executed, while `Claim` sends a message from the current chain to
another chain in order to transfer tokens from that remote chain.

Tokens can be transferred from an account to different destinations, such as:

- other accounts on the same chain,
- the same account on another chain,
- other accounts on other chains,
- sessions so that other applications can use some tokens.

# Usage

## Setting Up

The WebAssembly binaries for the bytecode can be built and published using [steps from the
book](https://linera-io.github.io/linera-documentation/getting_started/first_app.html),
summarized below.

First setup a local network with two wallets, and keep it running in a separate terminal:

```bash
./scripts/run_local.sh
```

Compile the `fungible` application WebAssembly binaries, and publish them as an application
bytecode:

```bash
alias linera="$PWD/target/debug/linera"
export LINERA_WALLET="$(realpath target/debug/wallet.json)"
export LINERA_STORAGE="rocksdb:$(dirname "$LINERA_WALLET")/linera.db"

cd examples/fungible && cargo build --release && cd ../..
linera publish-bytecode \
examples/target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm
```

This will output the new bytecode ID, e.g.:

```rust
e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000
```

## Creating a Token

In order to use the published bytecode to create a token application, the initial state must be
specified. This initial state is where the tokens are minted. After the token is created, no
additional tokens can be minted and added to the application. The initial state is a JSON string
that specifies the accounts that start with tokens.

In order to select the accounts to have initial tokens, the command below can be used to list
the chains created for the test:

```bash
linera wallet show
```

A table will be shown with the chains registered in the wallet and their meta-data. The default
chain should be highlighted in green. Each chain has an `Owner` field, and that is what is used
for the account.

The example below creates a token application where two accounts start with the minted tokens,
one with 100 of them and another with 200 of them:

```bash
linera create-application e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000 \
    --json-argument '{ "accounts": {
        "User:445991f46ae490fe207e60c95d0ed95bf4e7ed9c270d4fd4fa555587c2604fe1": "100.",
        "User:c2f98d76c332bf809d7f91671eb76e5839c02d5896209881368da5838d85c83f": "200."
    } }'
```

This will output the application ID for the newly created token, e.g.:

```rust
e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
```

## Using the Token Application

Before using the token, a source and target address should be selected. The source address
should ideally be on the default chain (used to create the token) and one of the accounts chosen
for the initial state, because it will already have some initial tokens to send.

First, a node service has to be started:

```bash
linera service --port 8080 &
```

Then the web frontend:

```bash
cd examples/fungible/web-frontend
npm install
npm start
```

The web UI can then be opened by navigating to
`http://localhost:3000/$CHAIN_ID?app=$APPLICATION_ID&owner=$SOURCE_ACCOUNT&port=$PORT`, where:

- `$CHAIN_ID` is the ID of the chain where we registered the application.
- `$APPLICATION_ID` is the token application ID obtained when creating the token.
- `$SOURCE_ACCOUNT` is the owner of the chosen sender account.
- `$PORT` is the port the sender wallet service is listening to.

Two browser instances can be opened, one for the sender account and one for the receiver
account. In the sender account browser, the target chain ID and account can be specified, as
well as the amount to send. Once sent, the balance on the receiver account browser should
automatically update.

<!-- cargo-rdme end -->
