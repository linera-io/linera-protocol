#!/bin/sh

while true; do
  ./linera storage check_existence --storage "scylladb:tcp:scylla-client.scylla.svc.cluster.local:9042"
  status=$?

  if [ "$status" -eq 0 ]; then
    echo "Database already exists, no need to initialize."
    exit 0
  else
    # We rely on the shards to initialize the database, so just wait here
    if [ "$status" -eq 1 ]; then
        echo "Database does not exist, retrying in 5 seconds..."
    else
        echo "An unexpected error occurred (status: $status), retrying in 5 seconds..."
    fi
    sleep 5
  fi
done
