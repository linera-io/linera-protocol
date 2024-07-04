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

This implementation shows how to write a game that is meant to be played on a shared chain:
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
OWNER_1=df44403a282330a8b086603516277c014c844a4b418835873aced1132a3adcd5
OWNER_2=43c319a4eab3747afcd608d32b73a2472fcaee390ec6bed3e694b4908f55772d
CHAIN_1=e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
```

## Creating the Game Chain

We open a new chain owned by both `$OWNER_1` and `$OWNER_2`, create the application on it, and
start the node service.

```bash
PUB_KEY_1=$(linera -w0 keygen)
PUB_KEY_2=$(linera -w1 keygen)

read -d '' MESSAGE_ID HEX_CHAIN < <(linera -w0 --wait-for-outgoing-messages open-multi-owner-chain \
    --from $CHAIN_1 \
    --owner-public-keys $PUB_KEY_1 $PUB_KEY_2 \
    --initial-balance 1; printf '\0')

linera -w0 assign --key $PUB_KEY_1 --message-id $MESSAGE_ID
linera -w1 assign --key $PUB_KEY_2 --message-id $MESSAGE_ID

APP_ID=$(linera -w0 --wait-for-outgoing-messages \
  project publish-and-create examples/hex-game hex_game $HEX_CHAIN \
    --json-argument "{
        \"players\": [\"$OWNER_1\", \"$OWNER_2\"],
        \"boardSize\": 9,
        \"startTime\": 600000000,
        \"increment\": 600000000,
        \"blockDelay\": 100000000
    }")

linera -w1 process-inbox $HEX_CHAIN
linera -w0 process-inbox $HEX_CHAIN

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

[first_player]: http://localhost:8080/chains/c06f52a2a3cc991e6981d5628c11b03ad39f7509c4486893623a41d1f7ec49a0/applications/c06f52a2a3cc991e6981d5628c11b03ad39f7509c4486893623a41d1f7ec49a0000000000000000000000000c06f52a2a3cc991e6981d5628c11b03ad39f7509c4486893623a41d1f7ec49a0020000000000000000000000
[second_player]: http://localhost:8081/chains/c06f52a2a3cc991e6981d5628c11b03ad39f7509c4486893623a41d1f7ec49a0/applications/c06f52a2a3cc991e6981d5628c11b03ad39f7509c4486893623a41d1f7ec49a0000000000000000000000000c06f52a2a3cc991e6981d5628c11b03ad39f7509c4486893623a41d1f7ec49a0020000000000000000000000

<!-- cargo-rdme end -->
