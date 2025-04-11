#!/bin/sh

# Extract the ordinal number from the pod hostname
ORDINAL="${HOSTNAME##*-}"

exec ./linera-server run \
  --storage dualrocksdbscylladb:/mnt/stateful_partition/kube-ephemeral-ssd/linera.db:spawn_blocking:tcp:scylla-client.scylla.svc.cluster.local:9042 \
  --server /config/server.json \
  --shard $ORDINAL \
  --genesis /config/genesis.json
