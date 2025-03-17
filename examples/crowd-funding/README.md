# Crowd-funding Example Application

This example application implements crowd-funding campaigns using fungible tokens in
the `fungible` application. This demonstrates how to compose applications together and
how to instantiate applications where one chain has a special role.

Once an application is built and its module published on a Linera chain, the
published module can be used to create different instances. Each instance or crowd-funding
represents a different campaign.

## How It Works

The chain that created the campaign is called the "campaign chain". It is owned by the
creator (and beneficiary) of the campaign.

The goal of a crowd-funding campaign is to let people pledge any number of tokens from
their own chain(s). If enough tokens are pledged before the campaign expires, the campaign is
_successful_ and the creator can receive all the funds, including ones exceeding the funding
target. Otherwise, the campaign is _unsuccessful_ and contributors should be refunded.

## Caveat

Currently, only the owner of the campaign can create blocks that contain the `Cancel`
operation. In the future, campaign chains will not be single-owner chains and should
instead allow contributors (users with a pledge) to cancel the campaign if
appropriate (even without the owner's cooperation).

Optionally, contributors may also be able to create a block to accept a new epoch
(i.e. a change of validators).

<!--
TODO: The following documentation involves sleep to avoid some race conditions. See:
  - https://github.com/linera-io/linera-protocol/issues/1176
  - https://github.com/linera-io/linera-protocol/issues/1177
-->

## Usage

### Setting Up

The WebAssembly binaries for the module can be built and published using [steps from the
book](https://linera.dev/developers/getting_started.html),
summarized below.

Set up the path and the helper function. From the root of Linera repository, this can be
achieved as follows:

```bash
export PATH="$PWD/target/debug:$PATH"
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"
```

Start the local Linera network and run a faucet:

```bash
FAUCET_PORT=8079
FAUCET_URL=http://localhost:$FAUCET_PORT
linera_spawn linera net up --with-faucet --faucet-port $FAUCET_PORT

# If you're using a testnet, run this instead:
#   LINERA_TMP_DIR=$(mktemp -d)
#   FAUCET_URL=https://faucet.testnet-XXX.linera.net  # for some value XXX
```

Create the user wallets and add chains to them:

```bash
export LINERA_WALLET_0="$LINERA_TMP_DIR/wallet_0.json"
export LINERA_STORAGE_0="rocksdb:$LINERA_TMP_DIR/client_0.db"
export LINERA_WALLET_1="$LINERA_TMP_DIR/wallet_1.json"
export LINERA_STORAGE_1="rocksdb:$LINERA_TMP_DIR/client_1.db"

linera --with-wallet 0 wallet init --faucet $FAUCET_URL
linera --with-wallet 1 wallet init --faucet $FAUCET_URL

INFO_0=($(linera --with-wallet 0 wallet request-chain --faucet $FAUCET_URL))
INFO_1=($(linera --with-wallet 1 wallet request-chain --faucet $FAUCET_URL))
CHAIN_0="${INFO_0[0]}"
CHAIN_1="${INFO_1[0]}"
OWNER_0="${INFO_0[3]}"
OWNER_1="${INFO_1[3]}"
```

Note that `linera --with-wallet 0` is equivalent to `linera --wallet "$LINERA_WALLET_0"
--storage "$LINERA_STORAGE_0"`.

The command below can be used to list the chains created for the test as known by each
wallet:

```bash
linera --with-wallet 0 wallet show
linera --with-wallet 1 wallet show
```

A table will be shown with the chains registered in the wallet and their meta-data:

```text,ignore
╭──────────────────────────────────────────────────────────────────┬──────────────────────────────────────────────────────────────────────────────────────╮
│ Chain Id                                                         ┆ Latest Block                                                                         │
╞══════════════════════════════════════════════════════════════════╪══════════════════════════════════════════════════════════════════════════════════════╡
│ 582843bc9322ed1928239ce3f6a855f6cd9ea94c8690907f113d6d7a8296a119 ┆ Public Key:         84eddaaafce7fb923c3b2494b3d25e54e910490a726ad9b3a2228d3fb18f9874 │
│                                                                  ┆ Owner:              849baa540589d95e167d2622018fa341553bf2aff9f328d760443282c6654deb │
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

### Creating tokens

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
           --json-argument '{ "accounts": { "User:'$OWNER_0'": "100", "User:'$OWNER_1'": "200" } }' \
           --json-parameters "{ \"ticker_symbol\": \"FUN\" }")

# Wait for it to fully complete
sleep 8
```

We will remember the application ID for the newly created token as `$APP_ID_0`.

### Creating a crowd-funding campaign

Similarly, we're going to create a crowd-funding campaign on the default chain. We have
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

### Interacting with the campaign

First, a node service has to be started for each wallet, using two different ports:

```bash
linera --with-wallet 0 service --port 8080 &

# Wait for it to fully complete
sleep 2

linera --with-wallet 1 service --port 8081 &

# Wait for it to fully complete
sleep 2
```

Type each of these in the GraphiQL interface and substitute the env variables with their actual values that we've defined above.

Point your browser to http://localhost:8080, and enter the query:

```gql,uri=http://localhost:8080
query { applications(
  chainId: "$CHAIN_0"
) { id link } }
```

The response will have two entries, one for each application.

On both http://localhost:8080 and http://localhost:8081, you recognize the crowd-funding
application by its ID. The entry also has a field `link`. If you open that in a new tab, you
see the GraphQL API for that application on that chain.

Let's pledge 30 tokens by the campaign creator themselves.
For `$OWNER_0` on 8080, run `echo "http://localhost:8080/chains/$CHAIN_0/applications/$APP_ID_1"` to get the URL, open it
and run the following query:

```gql,uri=http://localhost:8080/chains/$CHAIN_0/applications/$APP_ID_1
mutation { pledge(
  owner:"User:$OWNER_0",
  amount:"30."
) }
```

This will make the owner show up if we list everyone who has made a pledge so far:

```gql,uri=http://localhost:8080/chains/$CHAIN_0/applications/$APP_ID_1
query { pledges { keys } }
```

To also have `$OWNER_1` make a pledge, they first need to claim their tokens. Those are still
on the other chain, where the application was created. To get the link on 8081
for the fungible application, run `echo "http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID_0"`,
open it and run the following query:

```gql,uri=http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID_0
mutation { claim(
  sourceAccount: {
    owner: "User:$OWNER_1",
    chainId: "$CHAIN_0"
  },
  amount: "200.",
  targetAccount: {
    owner: "User:$OWNER_1",
    chainId: "$CHAIN_1"
  }
) }
```

You can check that the 200 tokens have arrived:

```gql,uri=http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID_0
query {
  accounts { entry(key: "User:$OWNER_1") { value } }
}
```

Now, also on 8081, you can open the link for the crowd-funding
application you get when you run `echo "http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID_1"`
and run:

```gql,uri=http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID_1
mutation { pledge(
  owner:"User:$OWNER_1",
  amount:"80."
) }
```

This pledges another 80 tokens. With 110 pledged in total, we have now reached the campaign
goal. Now the campaign owner (on 8080) can collect the funds:

```gql,uri=http://localhost:8080/chains/$CHAIN_0/applications/$APP_ID_1
mutation { collect }
```

Get the fungible application on
8080 URL by running `echo "http://localhost:8080/chains/$CHAIN_0/applications/$APP_ID_0"`,
then check that we have received 110 tokens, in addition to the
70 that we had left after pledging 30 by running the following query:

```gql,uri=http://localhost:8080/chains/$CHAIN_0/applications/$APP_ID_0
query {
  accounts { entry(key: "User:$OWNER_0") { value } }
}
```
