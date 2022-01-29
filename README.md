[![Build Status](https://github.com/zefchain/zefchain-protocol/actions/workflows/rust.yml/badge.svg)](https://github.com/zef/zef-protocol/actions/workflows/rust.yml)
[![License](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE.md)

# Zefchain Protocol

This repository is dedicated to developping the Zefchain protocol.

## Quickstart with the Zef service CLI

The current code was imported from https://github.com/novifinancial/fastpay/pull/24 then
cleaned up (e.g. removing coins and assets for now). Atomic swaps are still WIP (notably
the client and CLI code is missing).

The following script can be run with `cargo test -- --ignored`.

```bash
cargo build --release
cd target/release
rm -f *.json *.txt

# Create configuration files for 4 authorities with 4 shards each.
# * Private server states are stored in `server*.json`.
# * `committee.json` is the public description of the FastPay committee.

# echo 'null' > committee.json
# for I in 1 2 3 4
# do
#    ./server generate --server server_"$I".json --host 127.0.0.1 --port 9"$I"00 --shards 4 >> committee.json
# done

# In one step:
./server generate-all --authorities \
   server_1.json:udp:127.0.0.1:9100:4 \
   server_2.json:udp:127.0.0.1:9200:4 \
   server_3.json:udp:127.0.0.1:9300:4 \
   server_4.json:udp:127.0.0.1:9400:4 \
--committee committee.json

# Create configuration files for 1000 user accounts.
# * Private account states are stored in one local wallet `accounts.json`.
# * `initial_accounts.txt` is used to mint the corresponding initial balances at startup on the server side.
./client --committee committee.json --accounts accounts.json create_initial_accounts 1000 --initial-funding 100 >> initial_accounts.txt

# Start servers
for I in 1 2 3 4
do
    for J in $(seq 0 3)
    do
        ./server run --server server_"$I".json --shard "$J" --initial-accounts initial_accounts.txt --committee committee.json &
    done
 done

# Query balance for first and last user account
ACCOUNT1="`head -n 1 initial_accounts.txt | awk -F: '{ print $1 }'`"
ACCOUNT2="`tail -n -1 initial_accounts.txt | awk -F: '{ print $1 }'`"
./client --committee committee.json --accounts accounts.json query_balance "$ACCOUNT1"
./client --committee committee.json --accounts accounts.json query_balance "$ACCOUNT2"

# Transfer 10 units
./client --committee committee.json --accounts accounts.json transfer 10 --from "$ACCOUNT1" --to "$ACCOUNT2"

# Query balances again
./client --committee committee.json --accounts accounts.json query_balance "$ACCOUNT1"
./client --committee committee.json --accounts accounts.json query_balance "$ACCOUNT2"

# Launch local benchmark using all user accounts
./client --committee committee.json --accounts accounts.json benchmark

# Create derived account
ACCOUNT3="`./client --committee committee.json --accounts accounts.json open_account --from "$ACCOUNT1"`"

# Inspect state of derived account
fgrep '"account_id"':"$ACCOUNT3" accounts.json

# Query the balance of the first account
./client --committee committee.json --accounts accounts.json query_balance "$ACCOUNT1"

# Kill servers
kill %1 %2 %3 %4 %5 %6 %7 %8 %9 %10 %11 %12 %13 %14 %15 %16

cd ../..
```

## AWS Rust SDK demo with localstack

```
# localstack start
AWS_ACCESS_KEY_ID= AWS_SECRET_ACCESS_KEY= AWS_REGION=us-west-2 LOCALSTACK=true cargo --bin aws-test run
```
