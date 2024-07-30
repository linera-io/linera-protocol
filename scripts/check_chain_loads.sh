#!/bin/bash -x

# Check if there aren't any unexpected chain load operations
#
# Chain states are loaded by the chain worker, and stay in memory while the worker is executing. If
# the same chain is also loaded elsewhere, data races occur and may lead to corrupt data. To
# prevent this, chains should only be loaded by the chain worker, except in some minor situations.

USAGES_FILE="$(mktemp)"

grep -R '\<\(create_chain\|load_chain\|load_active_chain\)\>' linera-* > "$USAGES_FILE"

# linera-storage contains the implementation of the methods
sed -i -e '/linera-storage\/src\/lib\.rs/d' "$USAGES_FILE"
sed -i -e '/linera-storage\/src\/db_storage\.rs/d' "$USAGES_FILE"

# The chain worker is where the most important usage happens
sed -i -e '/linera-core\/src\/chain_worker\/state\/mod\.rs/d' "$USAGES_FILE"

# Worker tests load chains in order to populate them with test data
sed -i -e '/linera-core\/src\/unit_tests\/worker_tests\.rs/d' "$USAGES_FILE"
sed -i -e '/linera-core\/src\/unit_tests\/test_utils\.rs/d' "$USAGES_FILE"

# The SDK integration test framework uses `create_chain` to create a dummy admin chain before the
# test (and the workers) start
if [ "$(grep 'linera-sdk/src/test/validator.rs' "$USAGES_FILE" | wc -l)" -eq 1 ]; then
    sed -i -e '/linera-sdk\/src\/test\/validator\.rs/d' "$USAGES_FILE"
fi

# The `linera wallet init` command uses `load_chain` in an isolated setting without any workers 
if [ "$(grep 'linera-service/src/linera/main.rs' "$USAGES_FILE" | wc -l)" -eq 1 ]; then
    sed -i -e '/linera-service\/src\/linera\/main\.rs/d' "$USAGES_FILE"
fi

# The linera-client uses `create_chain` to initialize the storage from the genesis configuration,
# and this is only called by the `database_tool`
if [ "$(grep 'linera-client/src/config.rs' "$USAGES_FILE" | wc -l)" -eq 1 ]; then
    sed -i -e '/linera-client\/src\/config\.rs/d' "$USAGES_FILE"
fi

cat "$USAGES_FILE"

if grep . "$USAGES_FILE"; then
    echo "Found some unexpected chain load operations:" | cat - "$USAGES_FILE"
    rm "$USAGES_FILE"
    exit 1
else
    rm "$USAGES_FILE"
    exit 0
fi
