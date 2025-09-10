#!/usr/bin/env bash

faucet_url=${1-http://localhost:8080}; shift
count=${1-100}; shift
wallet_base_path=${1-$(mktemp -dtu linera-faucet-stress-test.XXXXXX)/wallet}; shift

wallets=()

function cleanup {
    rm -rf "${wallets[@]}"
    rmdir --ignore-fail-on-non-empty "$wallet_base_path" 2>/dev/null
}

trap cleanup EXIT

function create_wallet_and_claim_chain {
    local path=$1; shift
    wallets+=("$path")
    mkdir -p $path
    TIMEFORMAT='{ "operation": "init", "duration": %R, "wallet": "'"$path"'" }'
    time {
        LINERA_WALLET=$path/wallet.json \
        LINERA_KEYSTORE=$path/keystore.json \
        LINERA_STORAGE=rocksdb:$path/storage.db \
        >/dev/null \
        2>&3 \
        linera wallet init --faucet $faucet_url
    }
    TIMEFORMAT='{ "operation": "request-chain", "duration": %R, "wallet": "'"$path"'" }'
    time {
        LINERA_WALLET=$path/wallet.json \
        LINERA_KEYSTORE=$path/keystore.json \
        LINERA_STORAGE=rocksdb:$path/storage.db \
        >/dev/null \
        2>&3 \
        linera wallet request-chain --faucet $faucet_url
    }
}

TIMEFORMAT='{ "operation": "stress-test", "duration": %R }'
{
    time {
        for i in $(seq $count)
        do
            create_wallet_and_claim_chain $wallet_base_path/$i &
        done
        wait
    }
} 3>&2 2>&1
