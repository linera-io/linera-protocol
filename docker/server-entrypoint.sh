#!/bin/sh

# Extract the ordinal number from the pod hostname
ORDINAL="${HOSTNAME##*-}"

# export RUST_LOG=trace

exec ./linera-server run \
  --storage scylladb:tcp:scylladb.default.svc.cluster.local:9042 \
  --server /config/server.json \
  --shard $ORDINAL \
  --genesis /config/genesis.json
