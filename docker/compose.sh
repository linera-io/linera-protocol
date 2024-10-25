#!/bin/bash

set -xe

source ./compose-common.sh

CONF_DIR=$ROOT_DIR/configuration/compose

cleanup_started=false

# Clean up hanging volumes when the script is terminated.
cleanup() {
    if [ "$cleanup_started" = true ]; then
        exit 0
    fi
    cleanup_started=true
    rm committee.json
    rm genesis.json
    rm -r linera.db
    rm server.json
    rm wallet.json
    SCYLLA_VOLUME=docker_linera-scylla-data
    SHARED_VOLUME=docker_linera-shared
    docker rm -f $(docker ps -a -q --filter volume=$SCYLLA_VOLUME)
    docker volume rm $SCYLLA_VOLUME
    docker rm -f $(docker ps -a -q --filter volume=$SHARED_VOLUME)
    docker volume rm $SHARED_VOLUME
}

if [ "${DOCKER_COMPOSE_WAIT:-false}" = "true" ]; then
    trap cleanup INT
else
    trap cleanup EXIT INT
fi

cd "$ROOT_DIR"

build_docker_image

cd "$SCRIPT_DIR"

# Create configuration files.
# * Private server states are stored in `server.json`.
# * `committee.json` is the public description of the Linera committee.
linera-server generate --validators "$CONF_DIR/validator.toml" --committee committee.json --testing-prng-seed 1

# Create configuration files for 10 user chains.
# * Private chain states are stored in one local wallet `wallet.json`.
# * `genesis.json` will contain the initial balances of chains as well as the initial committee.

linera --wallet wallet.json --storage rocksdb:linera.db create-genesis-config 10 --genesis genesis.json --initial-funding 10 --committee committee.json --testing-prng-seed 2

if [ "${DOCKER_COMPOSE_WAIT:-false}" = "true" ]; then
    docker compose up --wait
else
    docker compose up
fi
