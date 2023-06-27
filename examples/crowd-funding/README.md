<!-- cargo-rdme start -->

# Crowd-funding Example Application

This example application implements crowd-funding campaigns using fungible tokens in
the `fungible` application. This demonstrates how to compose applications together and
how to instantiate applications where one chain has a special role.

Once this application is built and its bytecode published on a Linera chain, the
published bytecode can be used to create different instances, where each
instance represents a different campaign.

# How It Works

The chain that created the campaign is called the "campaign chain". It is owned by the
creator (and beneficiary) of the campaign.

The goal of a crowd-funding campaign is to let people pledge any number of tokens from
their own chain(s). If enough tokens are pledged before the campaign expires, the
campaign is *successful* and the creator can receive all the funds. Otherwise, the
campaign is *unsuccessful* and contributors should be refunded.

# Caveat

Currently, only the owner of the campaign can create blocks that contain the `Cancel`
operation. In the future, campaign chains will not be single-owner chains and should
instead allow contributors (users with a pledge) to cancel the campaign if
appropriate (even without the owner's cooperation).

Optionally, contributors may also be able to create a block to accept a new epoch
(i.e. a change of validators).

# Usage

## Setting Up

The WebAssembly binaries for the bytecode can be built and published using [steps from the
book](https://linera-io.github.io/linera-documentation/getting_started/first_app.html),
summarized below.

First, setup a local network with two wallets, and keep it running in a separate terminal:

```bash
./scripts/run_local.sh
```

Compile the `crowd-funding` application WebAssembly binaries, and publish them as an application
bytecode:

```bash
export LINERA_WALLET="$(realpath target/debug/wallet.json)"
export LINERA_STORAGE="rocksdb:$(dirname "$LINERA_WALLET")/linera.db"
export LINERA_WALLET_2="$(realpath target/debug/wallet_2.json)"
export LINERA_STORAGE_2="rocksdb:$(dirname "$LINERA_WALLET_2")/linera_2.db"

cd examples/crowd-funding && cargo build --release && cd ../..
linera --wallet "$LINERA_WALLET" --storage "$LINERA_STORAGE" publish-bytecode \
examples/target/wasm32-unknown-unknown/release/crowd-funding_{contract,service}.wasm
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
linera --storage "$LINERA_STORAGE" --wallet "$LINERA_WALLET" wallet show
```

A table will be shown with the chains registered in the wallet and their meta-data. The default
chain should be highlighted in green. Each chain has an `Owner` field, and that is what is used
for the account.

The example below creates a token application where two accounts start with the minted tokens,
one with 100 of them and another with 200 of them:

```bash
linera --storage "$LINERA_STORAGE" --wallet "$LINERA_WALLET" create-application $BYTECODE_ID \
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
for the initial state, because it will already have some initial tokens to send. The target
address should be from a separate wallet due to current technical limitations (`linera service`
can only handle one chain per wallet at the same time). To see the available chains in the
secondary wallet, use:

```bash
linera --storage "$LINERA_STORAGE_2" --wallet "$LINERA_WALLET_2" wallet show
```

First, a node service has to be started for each wallet, using two different ports. The
`$SOURCE_CHAIN_ID` and `$TARGET_CHAIN_ID` can be left blank to use the default chains from each
wallet:

```bash
linera --wallet "$LINERA_WALLET" --storage "$LINERA_STORAGE" service --port 8080 $SOURCE_CHAIN_ID &
linera --wallet "$LINERA_WALLET_2" --storage "$LINERA_STORAGE_2" service --port 8081 $TARGET_CHAIN_ID &
```

Then the web frontend has to be started

```bash
cd examples/fungible/web-frontend
npm install
npm start
```

The web UI can then be opened by navigating to
`http://localhost:3000/$APPLICATION_ID?owner=$SOURCE_ACCOUNT&port=$PORT`, where:

- `$APPLICATION_ID` is the token application ID obtained when creating the token
- `$SOURCE_ACCOUNT` is the owner of the chosen sender account
- `$PORT` is the port the sender wallet service is listening to (`8080` for the sender wallet
  and `8081` for the receiver wallet as per the previous commands)

Two browser instances can be opened, one for the sender account and one for the receiver
account. In the sender account browser, the target chain ID and account can be specified, as
well as the amount to send. Once sent, the balance on the receiver account browser should
automatically update.

<!-- cargo-rdme end -->
