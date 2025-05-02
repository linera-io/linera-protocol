#!/bin/sh

storage_replication_factor=$1

# Extract the ordinal number from the pod hostname
ORDINAL="${HOSTNAME##*-}"
storage=$1

exec ./linera-server run \
  --storage $storage \
  --server /config/server.json \
  --shard $ORDINAL \
  --genesis /config/genesis.json \
  --storage-replication-factor $storage_replication_factor
