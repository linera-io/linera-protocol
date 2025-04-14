#!/bin/sh

storage_replication_factor=$1
while true; do
  ./linera storage check-existence --storage "dualrocksdbscylladb:/rocksdb/linera.db:spawn_blocking:tcp:scylla-client.scylla.svc.cluster.local:9042"
  status=$?

  if [ $status -eq 0 ]; then
    echo "Database already exists, no need to initialize."
    exit 0
  elif [ $status -eq 1 ]; then
    echo "Database does not exist, attempting to initialize..."
    if ./linera-server initialize \
      --storage dualrocksdbscylladb:/rocksdb/linera.db:spawn_blocking:tcp:scylla-client.scylla.svc.cluster.local:9042 \
      --genesis /config/genesis.json \
      --storage-replication-factor $storage_replication_factor; then
      echo "Initialization successful."
      exit 0
    else
      echo "Initialization failed, retrying in 5 seconds..."
      sleep 5
    fi
  else
    echo "An unexpected error occurred (status: $status), retrying in 5 seconds..."
    sleep 5
  fi
done
