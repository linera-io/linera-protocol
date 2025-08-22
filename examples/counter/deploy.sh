export PATH="$PWD/target/debug:$PATH"
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"
FAUCET_PORT=8080
FAUCET_URL=http://localhost:$FAUCET_PORT
LINERA_TMP_DIR=$(mktemp -d)
export LINERA_WALLET="$LINERA_TMP_DIR/wallet.json"
export LINERA_KEYSTORE="$LINERA_TMP_DIR/keystore.json"
export LINERA_STORAGE="rocksdb:$LINERA_TMP_DIR/client.db"
linera wallet init --faucet $FAUCET_URL
INFO_1=($(linera wallet request-chain --faucet $FAUCET_URL))
APPLICATION_ID=$(linera publish-and-create \
  examples/target/wasm32-unknown-unknown/release/counter_{contract,service}.wasm \
  --json-argument "1")
echo $APPLICATION_ID
