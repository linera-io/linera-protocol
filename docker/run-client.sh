#!/bin/bash -xe

./fetch-config-file.sh genesis.json
./fetch-config-file.sh wallet.json

sleep 30

# Command line prefix for client calls
CLIENT=(./client --storage rocksdb:client.db --wallet wallet.json --genesis genesis.json)

# Query balance for first and last user chain
CHAIN1="7817752ff06b8266d77df8febf5c4b524cec096bd83dc54f989074fb94f833737ae984f32be2cee1dfab766fe2d0c726503c4d97117eb59023e9cc65a8ecd1f7"
CHAIN2="8ec607f670dd82d6986e95815b6ba64e4aad748e6f023f8dde42439222959c0c4aa9d3af00a983deec3f1ab0e64ef27ff80a1a8e1c1990be677ef5cfb316de85"
${CLIENT[@]} query_balance "$CHAIN1"
${CLIENT[@]} query_balance "$CHAIN2"

# Transfer 10 units then 5 back
${CLIENT[@]} transfer 10 --from "$CHAIN1" --to "$CHAIN2"
${CLIENT[@]} transfer 5 --from "$CHAIN2" --to "$CHAIN1"

# Launch local benchmark using all user chains
${CLIENT[@]} benchmark --max-in-flight 50

# Create derived chain
CHAIN3="`${CLIENT[@]} open_chain --from "$CHAIN1"`"

# Inspect state of derived chain
fgrep '"chain_id":"'$CHAIN3'"' wallet.json

# Query the balance of the first chain
${CLIENT[@]} query_balance "$CHAIN1"
