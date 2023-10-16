#!/bin/sh

# Extract the ordinal number from the pod hostname
ORDINAL="${HOSTNAME##*-}"

exec ./linera-server run \
  --storage scylladb:tcp:scylladb.default.svc.cluster.local:9042 \
  --server server_1.json \
  --shard $ORDINAL \
  --genesis genesis.json