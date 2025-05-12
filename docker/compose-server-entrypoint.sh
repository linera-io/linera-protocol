#!/bin/sh

storage=$1
storage_replication_factor=$2

exec ./linera-server run \
  --storage $storage \
  --server /config/server.json \
  --shard 0 \
  --genesis /config/genesis.json \
  --storage-replication-factor $storage_replication_factor
