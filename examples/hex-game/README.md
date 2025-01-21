# Hex Game

Hex is a game where player `One` tries to connect the left and right sides of the board and player
`Two` tries to connect top to bottom. The board is rhombic and has a configurable side length `s`.
It consists of `s * s` hexagonal cells, indexed like this:

```rust
(0, 0)      (1, 0)      (2, 0)

      (0, 1)      (1, 1)      (2, 1)

            (0, 2)      (1, 2)      (2, 2)
```

The players alternate placing a stone in their color on an empty cell until one of them wins.

This implementation shows how to write a game that is played on a shared temporary chain:
Users make turns by submitting operations to the chain, not by sending messages, so a player
does not have to wait for any other chain owner to accept any message.

## Usage

### Setting up

Make sure you have the `linera` binary in your `PATH`, and that it is compatible with your
`linera-sdk` version.

For scripting purposes, we also assume that the BASH function
`linera_spawn_and_read_wallet_variables` is defined. From the root of Linera repository, this can
be achieved as follows:

```bash
export PATH="$PWD/target/debug:$PATH"
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"
```

To start the local Linera network and create two wallets:

```bash
linera_spawn_and_read_wallet_variables linera net up --testing-prng-seed 37 --extra-wallets 1
```

We use the test-only CLI option `--testing-prng-seed` to make keys deterministic and simplify our
explanation.

```bash
CHAIN_1=e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
```

### Creating the Game Chain

We open a new chain owned by both `$OWNER_1` and `$OWNER_2`, create the application on it, and
start the node service.

```bash
APP_ID=$(linera -w0 --wait-for-outgoing-messages \
  project publish-and-create examples/hex-game hex_game $CHAIN_1 \
    --json-argument "{
        \"startTime\": 600000000,
        \"increment\": 600000000,
        \"blockDelay\": 100000000
    }")

OWNER_1=$(linera -w0 keygen)
OWNER_2=$(linera -w1 keygen)

linera -w0 service --port 8080 &
sleep 1
```

Type each of these in the GraphiQL interface and substitute the env variables with their actual values that we've defined above.

The `start` mutation starts a new game. We specify the two players using their new public keys,
on the URL you get by running `echo "http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID"`:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
mutation {
  start(
    players: [
        "$OWNER_1",
        "$OWNER_2"
    ],
    boardSize: 11,
    feeBudget: "1"
  )
}
```

The app's main chain keeps track of the games in progress, by public key:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
query {
  gameChains {
    keys(count: 3)
  }
}
```

It contains the temporary chain's ID, and the ID of the message that created it:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
query {
  gameChains {
    entry(key: "$OWNER_1") {
      value {
        messageId chainId
      }
    }
  }
}
```

Set the `QUERY_RESULT` variable to have the result returned by the previous query, and `HEX_CHAIN` and `MESSAGE_ID` will be properly set for you.
Alternatively you can set the variables to the `chainId` and `messageId` values, respectively, returned by the previous query yourself.
Using the message ID, we can assign the new chain to the key in each wallet:

```bash
kill %% && sleep 1    # Kill the service so we can use CLI commands for wallet 0.

HEX_CHAIN=$(echo "$QUERY_RESULT" | jq -r '.gameChains.entry.value[0].chainId')
MESSAGE_ID=$(echo "$QUERY_RESULT" | jq -r '.gameChains.entry.value[0].messageId')

linera -w0 assign --owner $OWNER_1 --message-id $MESSAGE_ID
linera -w1 assign --owner $OWNER_2 --message-id $MESSAGE_ID

linera -w0 service --port 8080 &
linera -w1 service --port 8081 &
sleep 1
```

### Playing the Game

Now the first player can make a move by navigating to the URL you get by running `echo "http://localhost:8080/chains/$HEX_CHAIN/applications/$APP_ID"`:

```gql,uri=http://localhost:8080/chains/$HEX_CHAIN/applications/$APP_ID
mutation { makeMove(x: 4, y: 4) }
```

And the second player player at the URL you get by running `echo "http://localhost:8080/chains/$HEX_CHAIN/applications/$APP_ID"`:

```gql,uri=http://localhost:8081/chains/$HEX_CHAIN/applications/$APP_ID
mutation { makeMove(x: 4, y: 5) }
```
