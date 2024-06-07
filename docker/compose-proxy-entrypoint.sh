#!/bin/sh

while [ ! -f /shared/init_done ]; do
  echo "waiting for db init"
  sleep 5
done

exec ./linera-proxy \
  --storage scylladb:tcp:scylla:9042 \
  --genesis /config/genesis.json \
  /config/server.json
