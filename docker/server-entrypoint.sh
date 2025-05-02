#!/bin/sh

# Extract the ordinal number from the pod hostname
ORDINAL="${HOSTNAME##*-}"
storage=$1

exec ./linera-server run \
  --storage $storage \
  --server /config/server.json \
  --shard $ORDINAL \
  --genesis /config/genesis.json
