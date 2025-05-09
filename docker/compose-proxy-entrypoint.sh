#!/bin/sh

storage_replication_factor=$1

exec ./linera-proxy \
  --storage scylladb:tcp:scylla:9042 \
  --genesis /config/genesis.json \
  --storage-replication-factor $storage_replication_factor \
  /config/server.json
