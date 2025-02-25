#!/bin/bash

set -e
set -x

export RUSTFLAGS="-D warnings"

# cargo sort -w
# (set -e; cd examples && cargo sort -w)

(set -e; for I in linera-*; do if [ -d "$I" ]; then echo $I; cargo rdme -w $I; fi; done)
(set -e; cd examples; for dir in */; do
    if [ -f "${dir}README.md" ] && grep -q "<!-- cargo-rdme start -->" "${dir}README.md"; then
	dir_name="${dir%/}"
        echo "${dir_name}"
        cargo rdme -fw "${dir_name}"
    fi
done)

# cargo machete
# cargo +nightly fmt
# (set -e; cd examples && cargo +nightly fmt)

# cargo run --locked --bin linera help-markdown > CLI.md
# cargo run --locked --bin linera-schema-export > linera-service-graphql-client/gql/service_schema.graphql
# cargo run --locked --bin linera-indexer schema > linera-indexer/graphql-client/gql/indexer_schema.graphql
# cargo run --locked --bin linera-indexer schema operations > linera-indexer/graphql-client/gql/operations_schema.graphql
# rm -r indexer.db

# cargo build -p linera-sdk
# cargo build -p linera-witty-test-modules --target wasm32-unknown-unknown

# cargo clippy --locked --all-targets --all-features
# cargo clippy --locked --no-default-features
# # (set -e; cd examples; cargo clippy --locked --all-targets --all-features)
# # (set -e; cd examples; cargo clippy --locked --no-default-features)

# cargo run --locked --bin wit-generator

# (cd scripts/check_copyright_header && cargo build --release --locked)
# find linera-* examples -name '*.rs' -a -not -wholename '*/target/*' -print0 | xargs -0 scripts/target/release/check_copyright_header

cargo test --locked -p linera-rpc test_format || (cargo insta accept && cargo test --locked -p linera-rpc test_format)