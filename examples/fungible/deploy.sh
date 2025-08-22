#!/bin/sh

echo "Deploying fungible token example to a local network"

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
CHAIN_1="${INFO_1[0]}"
OWNER_1="${INFO_1[1]}"
INFO_2=($(linera wallet request-chain --faucet $FAUCET_URL))
CHAIN_2="${INFO_2[0]}"
OWNER_2="${INFO_2[1]}"

MODULE_ID=$(linera publish-module \
    examples/target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm)

APPLICATION_ID=$(linera create-application $MODULE_ID \
    --json-argument "{ \"accounts\": {
        \"0xad32a6482a37f73e93590ad70640c6611b5177f7@8d353b75d1c4d6d2c1b7f9689da021507637d4de71cb445d8568b77edbd63b7a\": \"100.\"
    } }" \
    --json-parameters "{ \"ticker_symbol\": \"FUN\" }" \
)
echo $APPLICATION_ID