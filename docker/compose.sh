#!/bin/sh

rm -r wallet.json linera.db

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONF_DIR=../configuration/compose
set -xe

# Create configuration files.
# * Private server states are stored in `server.json`.
# * `committee.json` is the public description of the Linera committee.
linera-server generate --validators "$CONF_DIR/validator.toml" --committee committee.json --testing-prng-seed 1

# Create configuration files for 10 user chains.
# * Private chain states are stored in one local wallet `wallet.json`.
# * `genesis.json` will contain the initial balances of chains as well as the initial committee.

linera --wallet wallet.json --storage rocksdb:linera.db create-genesis-config 10 --genesis genesis.json --initial-funding 10 --committee committee.json --testing-prng-seed 2

mv server_1.json server.json

docker compose up