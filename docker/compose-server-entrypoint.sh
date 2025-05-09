#!/bin/sh

storage=$1

exec ./linera-server run \
  --storage $storage \
  --server /config/server.json \
  --shard 0 \
  --genesis /config/genesis.json
