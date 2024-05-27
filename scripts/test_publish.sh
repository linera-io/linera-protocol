#!/bin/bash

set -x -e

# Usage:
#   cargo install cargo-local-registry
#   cargo install cargo-index
#   grep -v '^#' packages.txt | scripts/test_publish.sh REGISTRY

# Where to store the registry.
mkdir -p "$1"
REGISTRY="$(cd "$1"; pwd)"
shift

# Make sure we're running from a clean repo.
if [ ! -z "$(git status --porcelain)" ]; then
    echo "The git repository has uncommitted changes"
    exit 1
fi

# Synchronize the registry using `Cargo.lock`.
(echo; echo '[source]') >> .cargo/config.toml
cargo local-registry --git -s Cargo.lock "$REGISTRY" | tail -n +2 >> .cargo/config.toml

echo "The following change was applied and should be reverted on exit:"
git diff
LINERA_DIR="$PWD"
trap 'cd "$LINERA_DIR"; git checkout -f HEAD .cargo/config.toml' EXIT

# Initialize the git repository for the index if needed. Ideally, we'd like to use `cargo
# index init` first but the tool refuses to update an existing directory.
git init "$REGISTRY"/index || true
(cd "$REGISTRY"/index; git add .; git commit -m 'update registry')

# Build the packages in order.
while read LINE; do
    ARGS=($LINE)
    CRATE="${ARGS[0]}"
    cargo index add --index "$REGISTRY"/index --upload "$REGISTRY" --index-url local --manifest-path "$CRATE"/Cargo.toml -- -p $LINE
done
