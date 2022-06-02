#!/bin/bash

FILE="$1"

if [ -z "$FILE" ]; then
    echo "Usage: ./fetch-config-file.sh FILE" >&2
    exit 1
fi

while ! curl -sO "http://zefchain-setup-1:8080/$FILE"; do
    sleep 1
done
