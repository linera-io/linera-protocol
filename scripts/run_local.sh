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
# * `committee.json` is the public description of the Linera committee.
./server generate --validators $CONF_DIR/validator_{1,2,3,4}.toml --committee committee.json

# Create configuration files for 10 user chains.
# * Private chain states are stored in one local wallet `wallet.json`.
# * `genesis.json` will contain the initial balances of chains as well as the initial committee.
./linera --wallet wallet.json create_genesis_config 10 --genesis genesis.json --initial-funding 10 --committee committee.json

# Initialise the second wallet.
./linera --wallet wallet_2.json wallet init --genesis genesis.json

# Start servers and create initial chains in DB
for I in 1 2 3 4
do
    ./proxy server_"$I".json &

    for J in $(seq 0 3)
    do
        ./server run --storage rocksdb:server_"$I"_"$J".db --server server_"$I".json --shard "$J" --genesis genesis.json &
    done
done

sleep 3;

# Create second wallet with unassigned key.
KEY=$(./linera --wallet wallet_2.json keygen)

# Open chain on behalf of wallet 2.
EFFECT_AND_CHAIN=$(./linera --wallet wallet.json open_chain --to-public-key "$KEY")
EFFECT=$(echo "$EFFECT_AND_CHAIN" | sed -n '1 p')

# Assign newly created chain to unassigned key.
./linera --wallet wallet_2.json --storage rocksdb:linera_2.db assign --key "$KEY" --effect-id "$EFFECT"

read

