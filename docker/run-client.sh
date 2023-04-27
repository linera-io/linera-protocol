#!/bin/bash -xe

./fetch-config-file.sh genesis.json
./fetch-config-file.sh wallet.json

sleep 30

# Command line prefix for client calls
CLIENT=(./linera --storage rocksdb:linera.db --wallet wallet.json)

# Query balance for first and last user chain, root chains 0 and 999
CHAIN1="e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
CHAIN2="9c8a838e8f7b63194f6c7585455667a8379d2b5db19a3300e9961f0b1e9091ea"
${CLIENT[@]} query_balance "$CHAIN1"
${CLIENT[@]} query_balance "$CHAIN2"

# Transfer 10 units then 5 back
${CLIENT[@]} transfer 10 --from "$CHAIN1" --to "$CHAIN2"
${CLIENT[@]} transfer 5 --from "$CHAIN2" --to "$CHAIN1"

# Launch local benchmark using all user chains
${CLIENT[@]} benchmark --max-in-flight 50

# Create derived chain
CHAIN3="`${CLIENT[@]} open-chain --from "$CHAIN1"`"

# Inspect state of derived chain
fgrep '"chain_id":"'$CHAIN3'"' wallet.json

# Query the balance of the first chain
${CLIENT[@]} query_balance "$CHAIN1"
