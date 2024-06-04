#!/bin/sh

while [ ! -f /shared/init_done ]; do
  echo "waiting for db init"
  sleep 5
done

exec ./linera-server run \
  --storage scylladb:tcp:scylla:9042 \
  --server /config/server.json \
  --shard 0 \
  --genesis /config/genesis.json
