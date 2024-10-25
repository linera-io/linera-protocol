#!/bin/sh

exec ./linera-server run \
  --storage scylladb:tcp:scylla:9042 \
  --server /config/server.json \
  --shard 0 \
  --genesis /config/genesis.json
