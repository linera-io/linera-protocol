#!/bin/bash -x

NUM_VALIDATORS="$1"
NUM_SHARDS="$2"

if [ -z "$NUM_VALIDATORS" ] || [ -z "$NUM_SHARDS" ]; then
    echo "USAGE: ./setup.sh NUM_VALIDATORS NUM_SHARDS" >&2
    exit 1
fi

# Clean up data files
rm -rf config/*

# Creare validator configuration directories and generate the command line options
validator_options() {
    for server in $(seq 0 "$(expr "${NUM_VALIDATORS}" - 1)"); do
        shards="$(seq -s':' 9101 "$(expr 9100 + ${NUM_SHARDS})" | sed -e "s/[0-9]\\+/server-${server}.servers:&/g")"
        echo "server_${server}.json:server-${server}.servers:9100:tcp:${shards}"
    done
}

# Create configuration files for ${NUM_VALIDATORS} validators with ${NUM_SHARDS} shards each.
# * Private server states are stored in `server*.json`.
# * `committee.json` is the public description of the FastPay committee.
VALIDATORS=($(validator_options))
./server generate-all --validators ${VALIDATORS[@]} --committee committee.json

# Create configuration files for 1000 user chains.
# * Private chain states are stored in one local wallet `wallet.json`.
# * `genesis.json` will contain the initial balances of chains as well as the initial committee.
./client \
    --wallet wallet.json \
    --genesis genesis.json \
    create_genesis_config 1000 \
    --initial-funding 100 \
    --committee committee.json

mkdir /config/
mv genesis.json /config/
mv wallet.json /config/

for server in $(seq 0 "$(expr "${NUM_VALIDATORS}" - 1)"); do
    mv "server_${server}.json" /config/
done

# Run a HTTP server to serve the configuration files
mini_httpd -p 8080 -d /config -D -r
