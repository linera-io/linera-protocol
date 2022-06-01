#!/bin/bash

NUM_SHARDS="$1"

if [ -z "$NUM_SHARDS" ]; then
    echo "USAGE: ./run-server.sh NUM_SHARDS" >&2
    exit 1
fi

while ! [ -f "/config/server/config.json" ]; do
    sleep 1
done

for shard in $(seq 0 $(expr "${NUM_SHARDS}" - 1)); do
    ./server run \
        --storage "shard_${shard}.db" \
        --server "/config/server/config.json" \
        --shard "$shard" \
        --genesis /config/common/genesis.json &
done

sleep 30
