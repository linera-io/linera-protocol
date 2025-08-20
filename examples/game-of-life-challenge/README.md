# Game-of-Life (GoL) Challenge

This application allows users to play with Conway's Game of Life (GoL) and solve specific
puzzles to earn points.

## Usage

### Setting up

Make sure you have the `linera` binary in your `PATH`, and that it is compatible with your
`linera-sdk` version.

For scripting purposes, we also assume that the BASH function `linera_spawn` is defined.
From the root of Linera repository, this can be achieved as follows:

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
export LINERA_WALLET_1="$LINERA_TMP_DIR/wallet_1.json"
export LINERA_KEYSTORE_1="$LINERA_TMP_DIR/keystore_1.json"
export LINERA_STORAGE_1="rocksdb:$LINERA_TMP_DIR/client_1.db"
export LINERA_WALLET_2="$LINERA_TMP_DIR/wallet_2.json"
export LINERA_KEYSTORE_2="$LINERA_TMP_DIR/keystore_2.json"
export LINERA_STORAGE_2="rocksdb:$LINERA_TMP_DIR/client_2.db"

linera --with-wallet 1 wallet init --faucet $FAUCET_URL
linera --with-wallet 2 wallet init --faucet $FAUCET_URL

INFO_1=($(linera --with-wallet 1 wallet request-chain --faucet $FAUCET_URL))
# INFO_2=($(linera --with-wallet 2 wallet request-chain --faucet $FAUCET_URL))
CHAIN_1="${INFO_1[0]}"
# CHAIN_2="${INFO_2[0]}"
OWNER_1="${INFO_1[1]}"
# OWNER_2="${INFO_2[1]}"
```

Note that `linera --with-wallet 1` or `linera -w1` is equivalent to `linera --wallet
"$LINERA_WALLET_1"  --keystore "$LINERA_KEYSTORE_1" --storage "$LINERA_STORAGE_1"`.

### Creating the GoL challenge application

We use the default chain of the first wallet to create the application on it and start the
node service.

```bash
APP_ID=$(linera -w1 --wait-for-outgoing-messages \
  project publish-and-create examples/game-of-life-challenge gol_challenge $CHAIN_1 \
    --json-argument "null")

linera -w1 service --port 8080 &
sleep 1
```

### Creating a new puzzle

TODO

### Submitting a solution to a puzzle

TODO
