#!/bin/sh

# Extract the ordinal number from the pod hostname
ORDINAL="${HOSTNAME##*-}"

exec ./linera-proxy \
  --storage scylladb:tcp:scylla-client.scylla.svc.cluster.local:9042 \
  --genesis /config/genesis.json \
  --id "$ORDINAL" \
  /config/server.json
