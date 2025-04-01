#!/bin/sh

# Extract the ordinal number from the pod hostname
#ORDINAL="${HOSTNAME##*-}"
ORDINAL=$(echo "$( v="$( nslookup "$( hostname -i )" | head -n 1 )"; v="${v##* = }"; v="${v%%.*}"; v="${v##*-}"; v="${v##*_}"; echo "$v" )")

exec ./linera-proxy \
  --storage scylladb:tcp:scylla:9042 \
  --genesis /config/genesis.json \
  --id "$ORDINAL" \
  /config/server.json
