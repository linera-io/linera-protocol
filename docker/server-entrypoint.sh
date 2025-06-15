#!/bin/sh

storage=$1
storage_replication_factor=$2

# Extract the ordinal number from the pod hostname
ORDINAL="${HOSTNAME##*-}"

exec ./linera-server run \
  --storage $storage \
  --server /config/server.json \
  --shard $ORDINAL \
  --storage-replication-factor $storage_replication_factor
