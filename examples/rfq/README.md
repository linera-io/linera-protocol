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
that the BASH function `linera_spawn_and_read_wallet_variables` is defined.

From the root of the Linera repository, this can be achieved as follows:

```bash
export PATH="$PWD/target/debug:$PATH"
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"
```

To start the local Linera network:

```bash
linera_spawn_and_read_wallet_variables linera net up --extra-wallets 1 --testing-prng-seed 37
```

We use the test-only CLI option `--testing-prng-seed` to make keys deterministic and simplify our
explanation.

```bash
export CHAIN_0=aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8
export CHAIN_1=582843bc9322ed1928239ce3f6a855f6cd9ea94c8690907f113d6d7a8296a119
export OWNER_0=513bb0b9fdf2d671fa3c44add540f383aada343b34260cff6220d390f2336c4b
export OWNER_1=79539f482c1a7fefd5c1fe66572498fd343c0410e29bc83fc59939e6804fdf1b
```

The `--extra-wallets 1` option creates an additional user chain and wallet - we will use it for the
user requesting a quote.

Now we have to publish and create the fungible applications.

```bash
(cd examples/fungible && cargo build --release --target wasm32-unknown-unknown)

APP_ID_0=$(linera --with-wallet 0 project publish-and-create \
           examples/fungible \
           --json-argument '{ "accounts": { "User:'$OWNER_0'": "500", "User:'$OWNER_1'": "500" } }' \
           --json-parameters "{ \"ticker_symbol\": \"FUN1\" }")

APP_ID_1=$(linera --with-wallet 0 project publish-and-create \
           examples/fungible \
           --json-argument '{ "accounts": { "User:'$OWNER_0'": "500", "User:'$OWNER_1'": "500" } }' \
           --json-parameters "{ \"ticker_symbol\": \"FUN2\" }")
```

Each user is granted 500 tokens of each type.

Lastly, we have to create the RFQ application.

```bash
APP_RFQ=$(linera -w 0 --wait-for-outgoing-messages \
    project publish-and-create examples/rfq \
    --required-application-ids $APP_ID_0 $APP_ID_1)
```

We also need to make sure that both users can access the RFQ application, so we have to request it
on the second user's chain.

```bash
linera -w 1 request-application $APP_RFQ
sleep 2
```

## Using the RFQ Application

First, node services for both users' wallets have to be started:

```bash
linera -w 0 service --port 8080 &
linera -w 1 service --port 8081 &
sleep 5
```

### Using GraphiQL

Type each of these in the GraphiQL interface and substitute the env variables with their actual
values that we've defined above.

First, user B has to claim their tokens on their chain, since they were created on user A's chain.

Claim 500 FUN1 from `$OWNER_1` in `$CHAIN_0` to `$OWNER_1` in `$CHAIN_1`, so they're in the proper chain.
Run `echo "http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID_0"` to print the URL
of the GraphiQL interface for the FUN1 app. Navigate to that URL and enter:

```gql,uri=http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID_0
mutation {
  claim(
    sourceAccount: {
      chainId: "$CHAIN_0",
      owner: "User:$OWNER_1",
    }
    amount: "500.",
    targetAccount: {
      chainId: "$CHAIN_1",
      owner: "User:$OWNER_1"
    }
  )
}
```

Claim 500 FUN2 from `$OWNER_1` in `$CHAIN_0` to `$OWNER_1` in `$CHAIN_1`, so they're in the proper chain.
Run `echo "http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID_1"` to print the URL
of the GraphiQL interface for the FUN1 app. Navigate to that URL and enter the same request:

```gql,uri=http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID_1
mutation {
  claim(
    sourceAccount: {
      chainId: "$CHAIN_0",
      owner: "User:$OWNER_1",
    }
    amount: "500.",
    targetAccount: {
      chainId: "$CHAIN_1",
      owner: "User:$OWNER_1"
    }
  )
}
```

Now we are ready to submit a quote request. In this scenario, user B wants a quote from user A
for 50 FUN2 tokens.

First, it will be convenient to open GraphiQL interfaces for both users in two browser tabs.

For user A's tab, run `echo "http://localhost:8080/chains/$CHAIN_0/applications/$APP_RFQ"` to print
the URL for the interface and navigate to that URL.

For user B's tab, run `echo "http://localhost:8081/chains/$CHAIN_1/applications/$APP_RFQ"` to print
the URL for the interface and navigate to that URL.

In user B's tab, perform the following mutation:

```gql,uri=http://localhost:8081/chains/$CHAIN_1/applications/$APP_RFQ
mutation {
  requestQuote(
    target: "$CHAIN_0",
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

```gql,uri=http://localhost:8080/chains/$CHAIN_0/applications/$APP_RFQ
mutation {
  provideQuote(
    requestId: {
      otherChainId:"$CHAIN_1",
      seqNum:0,
      weRequested:false
    },
    quote: "100",
    quoterOwner: "$OWNER_0",
  )
}
```

User B can now accept the quote. In user B's tab, perform the following mutation. This will create
the temporary chain and send tokens to it.

```gql,uri=http://localhost:8081/chains/$CHAIN_1/applications/$APP_RFQ
mutation {
  acceptQuote(
    requestId:{
      otherChainId:"$CHAIN_0",
      seqNum:0,
      weRequested:true
    },
    owner:"$OWNER_1",
    feeBudget:"0",
  )
}
```

In order to finalize the exchange, user A has to run the following mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_0/applications/$APP_RFQ
mutation {
  finalizeDeal(
    requestId: {
      otherChainId:"$CHAIN_1",
      seqNum:0,
      weRequested:false
    },
  )
}
```

At this point, the RFQ application should have performed the swap and closed the chain. You can
check whether the amounts of tokens are correct by navigating to the following URLs and performing
the following queries:

`http://localhost:8080/chains/$CHAIN_0/applications/$APP_ID_0` - user A's FUN1 account should have
400 tokens:

```gql,uri=http://localhost:8080/chains/$CHAIN_0/applications/$APP_ID_0
query {
  accounts { entry(key: "User:$OWNER_0") { value } }
}
```

`http://localhost:8080/chains/$CHAIN_0/applications/$APP_ID_1` - user A's FUN2 account should have
550 tokens:

```gql,uri=http://localhost:8080/chains/$CHAIN_0/applications/$APP_ID_1
query {
  accounts { entry(key: "User:$OWNER_0") { value } }
}
```

`http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID_0` - user B's FUN1 account should have
600 tokens:

```gql,uri=http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID_0
query {
  accounts { entry(key: "User:$OWNER_1") { value } }
}
```

`http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID_1` - user B's FUN2 account should have
450 tokens:

```gql,uri=http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID_1
query {
  accounts { entry(key: "User:$OWNER_1") { value } }
}
```
