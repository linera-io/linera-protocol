#!/bin/bash -x

NUM_VALIDATORS="$1"
NUM_SHARDS="$2"

if [ -z "$NUM_VALIDATORS" ] || [ -z "$NUM_SHARDS" ]; then
    echo "USAGE: ./setup.sh NUM_VALIDATORS NUM_SHARDS" >&2
    exit 1
fi

# Clean up data files
rm -rf config/* validator_*.toml

# Create validator configuration directories and generate the command line options
validator_options() {
    for server in $(seq 1 "${NUM_VALIDATORS}"); do
        cat << EOF > "validator_${server}.toml"
server_config_path = "server_${server}.json"
host = "validator-${server}"
port = 9100
internal_host = "validator-${server}"
internal_port = 10100
external_protocol = { Simple = "Tcp" }
internal_protocol = { Simple = "Tcp" }
EOF
        for ((shard=0; shard<${NUM_SHARDS}; shard++)); do
            cat << EOF >> "validator_${server}.toml"

[[shards]]
host = "server-${server}-shard-${shard}.server-${server}"
port = 9100
metrics_host = "server-${server}-shard-${shard}.server-${server}"
metrics_port = 11100
EOF
        done
        echo "validator_${server}.toml"
    done
}

# Create configuration files for ${NUM_VALIDATORS} validators with ${NUM_SHARDS} shards each.
# * Private server states are stored in `server*.json`.
# * `committee.json` is the public description of the Linera committee.
VALIDATORS=($(validator_options))
./server generate --validators ${VALIDATORS[@]} --committee committee.json

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
mv validator_*.toml /config/
mv server_*.json /config/

# Run a HTTP server to serve the configuration files
mini_httpd -p 8080 -d /config -D -r
