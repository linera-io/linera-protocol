#!/bin/sh

exec ./linera-proxy \
  --storage scylladb:tcp:scylla:9042 \
  --genesis /config/genesis.json \
  /config/server.json
