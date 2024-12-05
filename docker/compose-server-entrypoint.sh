#!/bin/sh

# Check if the ordinal number is provided
if [ -z "$1" ]; then
  echo "Error: No ordinal number provided."
  echo "Usage: $0 <ordinal_number>"
  exit 1
fi

exec ./linera-server run \
  --storage scylladb:tcp:scylla:9042 \
  --server /config/server.json \
  --shard $1 \
  --genesis /config/genesis.json
