#!/bin/sh

# Extract the ordinal number from the hostname
ORDINAL="${HOSTNAME##*-}"

exec ./linera-server run \
  --storage scylladb:tcp:scylla:9042 \
  --server /config/server.json \
  --shard $ORDINAL \
  --genesis /config/genesis.json
