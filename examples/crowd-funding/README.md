<!-- cargo-rdme start -->

# Crowd-funding Example Application

This example application implements crowd-funding campaigns using fungible tokens in
the `fungible` application. This demonstrates how to compose applications together and
how to instantiate applications where one chain has a special role.

Once an application is built and its bytecode published on a Linera chain, the
published bytecode can be used to create different instances. Each instance or crowd-funding
represents a different campaign.

# How It Works

The chain that created the campaign is called the "campaign chain". It is owned by the
creator (and beneficiary) of the campaign.

The goal of a crowd-funding campaign is to let people pledge any number of tokens from
their own chain(s). If enough tokens are pledged before the campaign expires, the campaign is
*successful* and the creator can receive all the funds, including ones exceeding the funding
target. Otherwise, the campaign is *unsuccessful* and contributors should be refunded.

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

Compile the Wasm binaries for the two applications `fungible` and `crowd-funding`
application, and publish them as an application bytecode:

```bash
alias linera="$PWD/target/debug/linera"
export LINERA_WALLET1="$PWD/target/debug/wallet.json"
export LINERA_STORAGE1="rocksdb:$(dirname "$LINERA_WALLET1")/linera.db"
export LINERA_WALLET2="$PWD/target/debug/wallet_2.json"
export LINERA_STORAGE2="rocksdb:$(dirname "$LINERA_WALLET2")/linera_2.db"

(cargo build && cd examples && cargo build --release)
linera --wallet "$LINERA_WALLET1" --storage "$LINERA_STORAGE1" publish-bytecode \
  examples/target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm
linera --wallet "$LINERA_WALLET1" --storage "$LINERA_STORAGE1" publish-bytecode \
  examples/target/wasm32-unknown-unknown/release/crowd_funding_{contract,service}.wasm
```

This will output two new bytecode IDs, for instance:

```rust
e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000
e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
```

We will refer to them as `$BYTECODE_ID1` and `$BYTECODE_ID2`.

## Creating a Token

In order to use the published bytecode to create a token application, the initial state must be
specified. This initial state is where the tokens are minted. After the token is created, no
additional tokens can be minted and added to the application. The initial state is a JSON string
that specifies the accounts that start with tokens.

In order to select the accounts to have initial tokens, the command below can be used to list
the chains created for the test as known by each wallet:

```bash
linera --storage "$LINERA_STORAGE1" --wallet "$LINERA_WALLET1" wallet show
linera --storage "$LINERA_STORAGE2" --wallet "$LINERA_WALLET2" wallet show
```

A table will be shown with the chains registered in the wallet and their meta-data:

```text,ignore
╭──────────────────────────────────────────────────────────────────┬──────────────────────────────────────────────────────────────────────────────────────╮
│ Chain Id                                                         ┆ Latest Block                                                                         │
╞══════════════════════════════════════════════════════════════════╪══════════════════════════════════════════════════════════════════════════════════════╡
│ 1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03 ┆ Public Key:         6555b1c9152e4dd57ecbf3fd5ce2a9159764a0a04a4366a2edc88e1b36ed4873 │
│                                                                  ┆ Owner:              c2f98d76c332bf809d7f91671eb76e5839c02d5896209881368da5838d85c83f │
│                                                                  ┆ Block Hash:         -                                                                │
│                                                                  ┆ Timestamp:          2023-06-28 09:53:51.167301                                       │
│                                                                  ┆ Next Block Height:  0                                                                │
╰──────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────╯
```

The default chain of each wallet should be highlighted in green. Each chain has an
`Owner` field, and that is what is used for the account. Let's pick the owners of the
default chain of each wallet and call them `$OWNER1` and `$OWNER2`. Remember the corresponding
chain IDs as `$CHAIN_ID1` (the chain where we just published the bytecode) and `$CHAIN_ID2`
(some user chain in wallet 2).

Create a fungible token application where two accounts start with the minted tokens,
one with 100 of them and another with 200 of them:

```bash
linera --storage "$LINERA_STORAGE1" --wallet "$LINERA_WALLET1" create-application $BYTECODE_ID1 \
    --json-argument '{ "accounts": { "User:'$OWNER1'": "100", "User:'$OWNER2'": "200" } }'
```

This will output the application ID for the newly created token, e.g.:

```rust
e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
```

Let remember it as `$APP_ID1`.

## Creating a crowd-funding campaign

Similarly, we're going to create a crowd-funding campaign on the default chain.
We have to specify our fungible application as a dependency and a parameter:

```bash
linera --storage "$LINERA_STORAGE1" --wallet "$LINERA_WALLET1" create-application $BYTECODE_ID2 \
   --json-argument '{ "owner": "User:'$OWNER1'", "deadline": 4102473600000000, "target": "100." }'  --required-application-ids=$APP_ID1  --json-parameters='"'$APP_ID1'"'
```

Let's remember the application ID as `$APP_ID2`.

## Interacting with the campaign

First, a node service has to be started for each wallet, using two different ports. The
`$SOURCE_CHAIN_ID` and `$TARGET_CHAIN_ID` can be left blank to use the default chains from each
wallet:

```bash
linera --wallet "$LINERA_WALLET1" --storage "$LINERA_STORAGE1" service --port 8080 $SOURCE_CHAIN_ID &
linera --wallet "$LINERA_WALLET2" --storage "$LINERA_STORAGE2" service --port 8081 $TARGET_CHAIN_ID &
```

Point your browser to http://localhost:8080, and enter the query:

```gql,ignore
query { applications { id link } }
```

The response will have two entries, one for each application.

If you do the same in http://localhost:8081, the node service for the other wallet,
it will have no entries at all, because the applications haven't been registered
there yet. Request `crowd-funding` from the other chain. As an application ID, use
`$APP_ID2`:

```gql,ignore
mutation { requestApplication(applicationId:"e476187…") }
```

If you enter `query { applications { id link } }` again, both entries will
appear in the second wallet as well now. `$APP_ID1` has been registered,
too, because it is a dependency of the other application.

On both http://localhost:8080 and http://localhost:8081, you recognize the crowd-funding
application by its ID. The entry also has a field `link`. If you open that in a new tab, you
see the GraphQL API for that application on that chain.

Let's pledge 30 tokens by the campaign creator themself, i.e. `$OWNER1` on 8080:

```gql,ignore
mutation { pledgeWithTransfer(
    owner:"User:445991f46ae490fe207e60c95d0ed95bf4e7ed9c270d4fd4fa555587c2604fe1",
    amount:"30."
) }
```

This will make the owner show up if we list everyone who has made a pledge so far:

```gql,ignore
query { pledgesKeys }
```

To also have `$OWNER2` make a pledge, they first need to claim their tokens. Those are still
on the other chain, where the application was created. Find the link on http://localhost:8081
for the fungible application, open it and run the following query. Remember to replace the user
with `$OWNER2`. The _source_ chain ID should be the `$CHAIN_ID1`, and the target `$CHAIN_ID2`.

```gql,ignore
mutation { claim(
  sourceAccount: {
    owner: "User:c2f98d76c332bf809d7f91671eb76e5839c02d5896209881368da5838d85c83f",
    chainId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
  },
  amount: "200.",
  targetAccount: {
    owner:"User:c2f98d76c332bf809d7f91671eb76e5839c02d5896209881368da5838d85c83f",
    chainId:"1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03"
  }
) }
```

You can check that the 200 tokens have arrived:

```gql,ignore
query {
    accounts(accountOwner:"User:c2f98d76c332bf809d7f91671eb76e5839c02d5896209881368da5838d85c83f")
}
```

Now, also on 8081, you can open the link for the crowd-funding application and run:

```gql,ignore
mutation { pledgeWithTransfer(
  owner:"User:c2f98d76c332bf809d7f91671eb76e5839c02d5896209881368da5838d85c83f",
  amount:"80."
) }
```

This pledges another 80 tokens. With 110 pledged in total, we have now reached the campaign
goal. Now the campaign owner (on 8080) can collect the funds:

```gql,ignore
mutation { collect }
```

In the fungible application on 8080, check that we have received 110 tokens, in addition to the
70 that we had left after pledging 30:

```gql,ignore
query {accounts(accountOwner:"User:445991f46ae490fe207e60c95d0ed95bf4e7ed9c270d4fd4fa555587c2604fe1")}
```

<!-- cargo-rdme end -->
