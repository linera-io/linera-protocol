#!/bin/bash -x

NUM_SHARDS="$1"

if [ -z "$NUM_SHARDS" ]; then
    echo "USAGE: ./run-server.sh NUM_SHARDS" >&2
    exit 1
fi

SERVER_ID="$(hostname | cut -f2 -d-)"

./fetch-config-file.sh genesis.json
./fetch-config-file.sh "server_${SERVER_ID}.json"

for shard in $(seq 0 "$(expr "${NUM_SHARDS}" - 1)"); do
    ./server run \
        --storage "shard_${shard}.db" \
        --server "server_${SERVER_ID}.json" \
        --shard "$shard" \
        --genesis genesis.json &
done

sleep 3600
