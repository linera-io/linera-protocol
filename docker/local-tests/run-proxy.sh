#!/bin/bash -x

SERVER_ID="$(hostname | cut -f2 -d-)"

./fetch-config-file.sh "server_${SERVER_ID}.json"

./linera-proxy "server_${SERVER_ID}.json"
