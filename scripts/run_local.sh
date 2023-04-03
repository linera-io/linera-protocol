SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CONF_DIR="${SCRIPT_DIR}/../configuration"

cd $SCRIPT_DIR/..

# For debug builds:
cargo build && cd target/debug
# For release builds:
# cargo build --release && cd target/release

# Clean up data files
rm -rf *.json *.txt *.db

# Make sure to clean up child processes on exit.
trap 'kill $(jobs -p)' EXIT

set -x

# Create configuration files for 4 validators with 4 shards each.
# * Private server states are stored in `server*.json`.
# * `committee.json` is the public description of the FastPay committee.
./server generate \
    --validators $CONF_DIR/validator_1.toml $CONF_DIR/validator_2.toml $CONF_DIR/validator_3.toml $CONF_DIR/validator_4.toml \
    --committee committee.json

# Create configuration files for 10 user chains.
# * Private chain states are stored in one local wallet `wallet.json`.
# * `genesis.json` will contain the initial balances of chains as well as the initial committee.
./client --wallet wallet.json --genesis genesis.json create_genesis_config 10 --initial-funding 10 --committee committee.json

# Start servers and create initial chains in DB
for I in 1 2 3 4
do
    ./proxy server_"$I".json &

    for J in $(seq 0 3)
    do
        ./server run --storage rocksdb:server_"$I"_"$J".db --server server_"$I".json --shard "$J" --genesis genesis.json &
    done
done

while :; do
    sleep 5
done
