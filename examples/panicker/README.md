# Panicker Example Application

Reproducer for contract panics on the web target. The single operation makes
the contract call `panic!` immediately. Click the button in the browser, watch
how the panic surfaces in the developer console.

## Setup and Deployment

From the root of the repository:

```bash
export PATH="$PWD/target/debug:$PATH"
eval "$(linera net helper 2>/dev/null)"

LINERA_FAUCET_PORT=8079
LINERA_FAUCET_URL=http://localhost:$LINERA_FAUCET_PORT
linera_spawn linera net up --with-faucet --faucet-port $LINERA_FAUCET_PORT

export LINERA_TMP_DIR=$(mktemp -d)
export LINERA_WALLET="$LINERA_TMP_DIR/wallet.json"
export LINERA_KEYSTORE="$LINERA_TMP_DIR/keystore.json"
export LINERA_STORAGE="rocksdb:$LINERA_TMP_DIR/client.db"

linera wallet init --faucet $LINERA_FAUCET_URL
linera wallet request-chain --faucet $LINERA_FAUCET_URL

cd examples/panicker
cargo build --release --target wasm32-unknown-unknown

LINERA_APPLICATION_ID=$(linera publish-and-create \
  ../target/wasm32-unknown-unknown/release/panicker_{contract,service}.wasm \
  --json-argument "null")
```

## Web Frontend

```bash
export LINERA_APPLICATION_ID LINERA_FAUCET_URL
pnpm install
pnpm dev
```

Open the printed URL, then click `Trigger panic`. A fresh in-memory key is
generated on every page load.
