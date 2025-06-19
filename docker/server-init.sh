#!/bin/sh

storage=$1
storage_replication_factor=$2

while true; do
  ./linera storage check-existence --storage $storage
  status=$?

  if [ $status -eq 0 ]; then
    echo "Database already exists, no need to initialize."
    exit 0
  elif [ $status -eq 1 ]; then
    echo "Database does not exist, attempting to initialize..."
    if ./linera storage initialize \
      --storage $storage \
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
