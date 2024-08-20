<!-- cargo-rdme start -->

# Hex

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


# Usage

## Setting up

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

## Creating the Game Chain

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

PUB_KEY_1=$(linera -w0 keygen)
PUB_KEY_2=$(linera -w1 keygen)

linera -w0 service --port 8080 &
sleep 1
```

The `start` mutation starts a new game. We specify the two players using their new public keys,
on [`http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID`][main_chain]:

```gql,uri=http://localhost:8080/chains/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
mutation {
  start(
    players: [
        "8a21aedaef74697db8b676c3e03ddf965bf4a808dc2bcabb6d70d6e6e3022ff7",
        "80265761fee067b68ba47cce7464cbc7f1da5b7044d8f68ffc898db5ccb563a5"
    ],
    boardSize: 11
  )
}
```

The app's main chain keeps track of the games in progress, by public key:

```gql,uri=http://localhost:8080/chains/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
query {
  gameChains {
    keys(count: 3)
  }
}
```

It contains the temporary chain's ID, and the ID of the message that created it:

```gql,uri=http://localhost:8080/chains/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
query {
  gameChains {
    entry(key: "8a21aedaef74697db8b676c3e03ddf965bf4a808dc2bcabb6d70d6e6e3022ff7") {
      value {
        messageId chainId
      }
    }
  }
}
```

Using the message ID, we can assign the new chain to the key in each wallet:

```bash
kill %% && sleep 1    # Kill the service so we can use CLI commands for wallet 0.

HEX_CHAIN=a393137daba303e8b561cb3a5bff50efba1fb7f24950db28f1844b7ac2c1cf27
MESSAGE_ID=e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65050000000000000000000000

linera -w0 assign --key $PUB_KEY_1 --message-id $MESSAGE_ID
linera -w1 assign --key $PUB_KEY_2 --message-id $MESSAGE_ID

linera -w0 service --port 8080 &
linera -w1 service --port 8081 &
sleep 1
```

## Playing the Game

Now the first player can make a move by navigating to [`http://localhost:8080/chains/$HEX_CHAIN/applications/$APP_ID`][first_player]

```gql,uri=http://localhost:8080/chains/$HEX_CHAIN/applications/$APP_ID
mutation { makeMove(x: 4, y: 4) }
```

And the second player player at [`http://localhost:8080/chains/$HEX_CHAIN/applications/$APP_ID`][second_player]

```gql,uri=http://localhost:8081/chains/$HEX_CHAIN/applications/$APP_ID
mutation { makeMove(x: 4, y: 5) }
```

[main_chain]: http://localhost:8080/chains/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
[first_player]: http://localhost:8080/chains/a393137daba303e8b561cb3a5bff50efba1fb7f24950db28f1844b7ac2c1cf27/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
[second_player]: http://localhost:8081/chains/a393137daba303e8b561cb3a5bff50efba1fb7f24950db28f1844b7ac2c1cf27/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000

<!-- cargo-rdme end -->
