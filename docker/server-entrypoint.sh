#!/bin/sh

# Extract the ordinal number from the pod hostname
ORDINAL="${HOSTNAME##*-}"

exec ./linera-server run \
  --storage rocksdb:/usr/share/rocksdb/shard_data.db \
  --server server_1.json \
  --shard $ORDINAL \
  --genesis genesis.json