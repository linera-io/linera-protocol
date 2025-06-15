#!/bin/sh

storage_replication_factor=$1
ORDINAL="${HOSTNAME##*-}"

exec ./linera-proxy \
    --storage scylladb:tcp:scylla-client.scylla.svc.cluster.local:9042 \
    --storage-replication-factor $storage_replication_factor \
    --id "$ORDINAL" \
    /config/server.json
