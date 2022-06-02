#!/bin/bash

SERVER_ID="$1"
NUM_SHARDS="$2"

if [ -z "$SERVER_ID" ] || [ -z "$NUM_SHARDS" ]; then
    echo "USAGE: ./run-server.sh SERVER_ID NUM_SHARDS" >&2
    exit 1
fi

./fetch-config-file.sh genesis.json
./fetch-config-file.sh "server_${SERVER_ID}.json"

for shard in $(seq 0 $(expr "${NUM_SHARDS}" - 1)); do
    ./server run \
        --storage "shard_${shard}.db" \
        --server "server_${SERVER_ID}.json" \
        --shard "$shard" \
        --genesis genesis.json &
done

sleep 30
