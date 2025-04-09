# Request For Quotes (RFQ) Example Application

This example implements a Request For Quotes (RFQ) application, which demonstrates atomic swaps on
the Linera protocol. Prerequisites for the RFQ application are the `fungible` application, as the
application needs some tokens to be exchanged.

# How it works

Each user is supposed to run the application on their own chain. Once user A wants to exchange
tokens with another user B, they need to look up user B's chain ID and submit a `RequestQuote`
operation. Such an operation defines the pair of tokens that are intended to be exchanged by their
application IDs, as well as the amount to be exchanged.

Once user B receives the request, they can respond with a quote using the `ProvideQuote` operation.
In it, they specify the amount of the other token they are willing to offer, as well as their
owner ID (which will be required for setting up the temporary chain for the atomic swap). It is
possible that multiple requests could have been received: the user specifies which one they are
responding to using a request ID, consisting of the other party's chain ID and a sequence number.

User A, after receiving the quote, has the option to either cancel the whole request using the
`CancelRequest` operation, or accept it using the `AcceptQuote` operation. Canceling the request
removes it from the application state and notifies the other party. Accepting the request launches
the exchange process.

Initially, a temporary chain for the atomic swap is automatically created. Tokens are transferred
to the chain (they are initially assumed to exist on user A's chain - if they don't, the operation
will fail) and put in the RFQ application's custody, and user B is notified.

The temporary chain is owned by both users, which means both users can withdraw from the exchange at
any time using the `CancelRequest` operation, and they don't depend on the other user for chain
liveness.

User B can then either cancel the request, or send their tokens to the temporary chain using the
`FinalizeDeal` operation. If they choose to finalize the deal, the tokens are sent (like before, they
are assumed to exist on user B's chain) to the RFQ application's custody. The application will then
return each batch of tokens to the other owner and close the temporary chain automatically.

# Usage

## Setting Up

Before getting started, make sure that the binary tools `linera*` corresponding to
your version of `linera-sdk` are in your PATH. For scripting purposes, we also assume
that the BASH function `linera_spawn` is defined.

From the root of the Linera repository, this can be achieved as follows:

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

Create the user wallets and add chains to them:

```bash
export LINERA_WALLET_1="$LINERA_TMP_DIR/wallet_1.json"
export LINERA_STORAGE_1="rocksdb:$LINERA_TMP_DIR/client_1.db"
export LINERA_WALLET_2="$LINERA_TMP_DIR/wallet_2.json"
export LINERA_STORAGE_2="rocksdb:$LINERA_TMP_DIR/client_2.db"

linera --with-wallet 1 wallet init --faucet $FAUCET_URL
linera --with-wallet 2 wallet init --faucet $FAUCET_URL

INFO_1=($(linera --with-wallet 1 wallet request-chain --faucet $FAUCET_URL))
INFO_2=($(linera --with-wallet 2 wallet request-chain --faucet $FAUCET_URL))
CHAIN_1="${INFO_1[0]}"
CHAIN_2="${INFO_2[0]}"
OWNER_1="${INFO_1[3]}"
OWNER_2="${INFO_2[3]}"
```

Note that `linera --with-wallet 1` is equivalent to `linera --wallet "$LINERA_WALLET_1"
--storage "$LINERA_STORAGE_1"`.

Now, we can publish the fungible module and create the fungible applications.

```bash
(cd examples/fungible && cargo build --release --target wasm32-unknown-unknown)

APP_ID_0=$(linera --with-wallet 1 project publish-and-create \
           examples/fungible \
           --json-argument '{ "accounts": { "'$OWNER_1'": "500", "'$OWNER_2'": "500" } }' \
           --json-parameters "{ \"ticker_symbol\": \"FUN1\" }")

APP_ID_1=$(linera --with-wallet 1 project publish-and-create \
           examples/fungible \
           --json-argument '{ "accounts": { "'$OWNER_1'": "500", "'$OWNER_2'": "500" } }' \
           --json-parameters "{ \"ticker_symbol\": \"FUN2\" }")
```

Each user is granted 500 tokens of each type.

Lastly, we have to create the RFQ application.

```bash
APP_RFQ=$(linera -w 1 --wait-for-outgoing-messages \
    project publish-and-create examples/rfq \
    --required-application-ids $APP_ID_0 $APP_ID_1)
```

## Using the RFQ Application

First, node services for both users' wallets have to be started:

```bash
linera -w 1 service --port 8080 &
linera -w 2 service --port 8081 &
sleep 5
```

### Using GraphiQL

Type each of these in the GraphiQL interface and substitute the env variables with their actual
values that we've defined above.

First, user B has to claim their tokens on their chain, since they were created on user A's chain.

Claim 500 FUN1 from `$OWNER_2` in `$CHAIN_1` to `$OWNER_2` in `$CHAIN_2`, so they're in the proper chain.
Run `echo "http://localhost:8081/chains/$CHAIN_2/applications/$APP_ID_0"` to print the URL
of the GraphiQL interface for the FUN1 app. Navigate to that URL and enter:

```gql,uri=http://localhost:8081/chains/$CHAIN_2/applications/$APP_ID_0
mutation {
  claim(
    sourceAccount: {
      chainId: "$CHAIN_1",
      owner: "$OWNER_2",
    }
    amount: "500.",
    targetAccount: {
      chainId: "$CHAIN_2",
      owner: "$OWNER_2"
    }
  )
}
```

Claim 500 FUN2 from `$OWNER_2` in `$CHAIN_1` to `$OWNER_2` in `$CHAIN_2`, so they're in the proper chain.
Run `echo "http://localhost:8081/chains/$CHAIN_2/applications/$APP_ID_1"` to print the URL
of the GraphiQL interface for the FUN1 app. Navigate to that URL and enter the same request:

```gql,uri=http://localhost:8081/chains/$CHAIN_2/applications/$APP_ID_1
mutation {
  claim(
    sourceAccount: {
      chainId: "$CHAIN_1",
      owner: "$OWNER_2",
    }
    amount: "500.",
    targetAccount: {
      chainId: "$CHAIN_2",
      owner: "$OWNER_2"
    }
  )
}
```

Now we are ready to submit a request for quote. In this scenario, user B wants a quote from user A
for 50 FUN2 tokens.

First, it will be convenient to open GraphiQL interfaces for both users in two browser tabs.

For user A's tab, run `echo "http://localhost:8080/chains/$CHAIN_1/applications/$APP_RFQ"` to print
the URL for the interface and navigate to that URL.

For user B's tab, run `echo "http://localhost:8081/chains/$CHAIN_2/applications/$APP_RFQ"` to print
the URL for the interface and navigate to that URL.

In user B's tab, perform the following mutation:

```gql,uri=http://localhost:8081/chains/$CHAIN_2/applications/$APP_RFQ
mutation {
  requestQuote(
    target: "$CHAIN_1",
    tokenPair: {
      tokenAsked: "$APP_ID_0",
      tokenOffered: "$APP_ID_1",
    },
    amount: "50"
  )
}
```

User A will now provide a quote to user B - they are willing to exchange 50 FUN2 for 100 FUN1.
In user A's tab, perform the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_RFQ
mutation {
  provideQuote(
    requestId: {
      otherChainId:"$CHAIN_2",
      seqNum:0,
      weRequested:false
    },
    quote: "100",
    quoterOwner: "$OWNER_1",
  )
}
```

User B can now accept the quote. In user B's tab, perform the following mutation. This will create
the temporary chain and send tokens to it.

```gql,uri=http://localhost:8081/chains/$CHAIN_2/applications/$APP_RFQ
mutation {
  acceptQuote(
    requestId:{
      otherChainId:"$CHAIN_1",
      seqNum:0,
      weRequested:true
    },
    owner:"$OWNER_2",
    feeBudget:"0",
  )
}
```

In order to finalize the exchange, user A has to run the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_RFQ
mutation {
  finalizeDeal(
    requestId: {
      otherChainId:"$CHAIN_2",
      seqNum:0,
      weRequested:false
    },
  )
}
```

At this point, the RFQ application should have performed the swap and closed the chain. You can
check whether the amounts of tokens are correct by navigating to the following URLs and performing
the following queries:

`http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID_0` - user A's FUN1 account should have
400 tokens:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID_0
query {
  accounts { entry(key: "$OWNER_1") { value } }
}
```

`http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID_1` - user A's FUN2 account should have
550 tokens:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID_1
query {
  accounts { entry(key: "$OWNER_1") { value } }
}
```

`http://localhost:8081/chains/$CHAIN_2/applications/$APP_ID_0` - user B's FUN1 account should have
600 tokens:

```gql,uri=http://localhost:8081/chains/$CHAIN_2/applications/$APP_ID_0
query {
  accounts { entry(key: "$OWNER_2") { value } }
}
```

`http://localhost:8081/chains/$CHAIN_2/applications/$APP_ID_1` - user B's FUN2 account should have
450 tokens:

```gql,uri=http://localhost:8081/chains/$CHAIN_2/applications/$APP_ID_1
query {
  accounts { entry(key: "$OWNER_2") { value } }
}
```
