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

First, build Linera and add it to the path:

```bash
cargo build
export PATH=$PWD/target/debug:$PATH
```

Using the helper function defined by `linera net helper`, set up a local network with two
wallets, and define variables holding their wallet paths (`$LINERA_WALLET_0`,
`$LINERA_WALLET_1`) and storage paths (`$LINERA_STORAGE_0`, `$LINERA_STORAGE_1`).  These
variables are named according to a convention that we can access using `--with-wallet $n`
to use the variable `LINERA_WALLET_$n` and `LINERA_STORAGE_$n`; e.g.
`linera --with-wallet 0` is equivalent to
`linera --wallet "$LINERA_WALLET_0" --storage "$LINERA_STORAGE_0"`.

```bash
eval "$(linera net helper)"
linera_spawn_and_read_wallet_variables \
    linera net up \
        --extra-wallets 1 \
        --testing-prng-seed 37
```

We use the `--testing-prng-seed` argument to ensure that the chain and owner IDs are
predictable.

```bash
CHAIN_0=e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
OWNER_0=e814a7bdae091daf4a110ef5340396998e538c47c6e7d101027a225523985316
CHAIN_1=1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03
OWNER_1=481ac53cc3171261fe6185a070390b6f74e7b264b99fcdb012e0bd5e2ea0685f
```

Alternatively, the command below can be used to list the chains created for the test as
known by each wallet:

```bash
linera --with-wallet 0 wallet show
linera --with-wallet 1 wallet show
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
default chain of each wallet and call them `$OWNER_0` and `$OWNER_1`. Remember the corresponding
chain IDs as `$CHAIN_0` (the chain where we just published the application) and `$CHAIN_1`
(some user chain in wallet 2).


## Creating tokens

Compile the Wasm binaries for the two applications `fungible` and `crowd-funding`, publish
them as applications, and give them initial states. This initial state is where the tokens
are minted. After the token is created, no additional tokens can be minted and added to
the application. The initial state is a JSON string that specifies the accounts that start
with tokens.

Create a fungible token application where two accounts start with the minted tokens, one
with 100 of them and another with 200 of them:

```bash
APP_ID_0=$(linera --with-wallet 0 project publish-and-create \
           examples/fungible \
           --json-argument '{ "accounts": { "User:'$OWNER_0'": "100", "User:'$OWNER_1'": "200" } }')

# Wait for it to fully complete
sleep 8
```

We will remember the application ID for the newly created token as `$APP_ID_0`.


## Creating a crowd-funding campaign

Similarly, we're going to create a crowd-funding campaign on the default chain.  We have
to specify our fungible application as a dependency and a parameter:

```bash
APP_ID_1=$(linera --with-wallet 0 \
           project publish-and-create \
           examples/crowd-funding \
           crowd_funding \
           --required-application-ids $APP_ID_0 \
           --json-argument '{ "owner": "User:'$OWNER_0'", "deadline": 4102473600000000, "target": "100." }' \
           --json-parameters '"'"$APP_ID_0"'"')

# Wait for it to fully complete
sleep 5
```

## Interacting with the campaign

First, a node service has to be started for each wallet, using two different ports:

```bash
linera --with-wallet 0 service --port 8080 &

# Wait for it to fully complete
sleep 2

linera --with-wallet 1 service --port 8081 &

# Wait for it to fully complete
sleep 2
```

Point your browser to http://localhost:8080, and enter the query:

```gql,ignore
query { applications(
    chainId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
) { id link } }
```

The response will have two entries, one for each application.

If you do the same with the other chain ID in http://localhost:8081, the node service for the
other wallet, it will have no entries at all, because the applications haven't been registered
there yet. Request `crowd-funding` from the other chain. As an application ID, use `$APP_ID2`:

```gql,ignore
mutation { requestApplication(
    chainId: "1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03"
    applicationId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65070000000000000000000000"
) }
```

If you enter the `applications` query again, both entries will appear in the second wallet as
well now. `$APP_ID_0` has been registered, too, because it is a dependency of the other
application.

On both http://localhost:8080 and http://localhost:8081, you recognize the crowd-funding
application by its ID. The entry also has a field `link`. If you open that in a new tab, you
see the GraphQL API for that application on that chain.

Let's pledge 30 tokens by the campaign creator themself, i.e. `$OWNER_0` on 8080:

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

To also have `$OWNER_1` make a pledge, they first need to claim their tokens. Those are still
on the other chain, where the application was created. Find the link on http://localhost:8081
for the fungible application, open it and run the following query. Remember to replace the user
with `$OWNER_1`. The _source_ chain ID should be the `$CHAIN_0`, and the target `$CHAIN_1`.

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
    accounts { entry(key: "User:c2f98d76c332bf809d7f91671eb76e5839c02d5896209881368da5838d85c83f") { value } }
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
query {
    accounts {
        entry(
            key: "User:445991f46ae490fe207e60c95d0ed95bf4e7ed9c270d4fd4fa555587c2604fe1"
        ) {
            value
        }
    }
}
```

<!-- cargo-rdme end -->
