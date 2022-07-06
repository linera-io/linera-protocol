#!/bin/bash -x

hostname

SERVER_ID="$(hostname | cut -f2 -d-)"
SHARD_ID="$(hostname | cut -f4 -d-)"

./fetch-config-file.sh genesis.json
./fetch-config-file.sh "server_${SERVER_ID}.json"

./server run \
    --storage "shard_data.db" \
    --server "server_${SERVER_ID}.json" \
    --shard "$SHARD_ID" \
    --genesis genesis.json &

sleep 3600
