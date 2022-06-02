#!/bin/bash

SERVER_ID="$1"

if [ -z "$SERVER_ID" ]; then
    echo "USAGE: ./run-proxy.sh SERVER_ID" >&2
    exit 1
fi

./fetch-config-file.sh "server_${SERVER_ID}.json"

./proxy "server_${SERVER_ID}.json"
