#!/bin/sh

./linera-db check_existence --storage "scylladb:tcp:scylladb.default.svc.cluster.local:9042"
status=$?

if [ $status -eq 0 ]; then
  exit 0
elif [ $status -eq 1 ]; then
  ./linera-server initialize \
    --storage scylladb:tcp:scylladb.default.svc.cluster.local:9042 \
    --genesis genesis.json || exit 1;
else
  exit $status
fi