#!/bin/sh

storage_replication_factor=$1

# Extract the ordinal number from the pod hostname
ORDINAL="${HOSTNAME##*-}"

exec ./linera-server run \
  --storage scylladb:tcp:scylla-client.scylla.svc.cluster.local:9042 \
  --server /config/server.json \
  --shard $ORDINAL \
  --genesis /config/genesis.json \
  --storage-replication-factor $storage_replication_factor
