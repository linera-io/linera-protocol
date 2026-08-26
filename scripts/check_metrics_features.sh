#!/bin/bash

# Check that every crate exposing `init_metrics` builds with its own `metrics` feature.
#
# `init_metrics` calls into the `init_metrics` of the crates below it, so a crate's `metrics`
# feature has to turn on `metrics` for those dependencies. Nothing else enforces that: a
# workspace-wide `--all-features` build unifies features across every crate, so a missing
# `<dep>/metrics` still compiles there and only fails for whoever builds the crate on its own.
# That is how `linera-service` shipped calling `linera_exporter::init_metrics` while its own
# `metrics` feature never enabled `linera-exporter/metrics`.
#
# The compiler is the check here rather than a reimplementation of cargo's feature resolution,
# which would have to follow transitive `dep/feature` edges to avoid false positives.

set -uo pipefail

# Make sure we're at the source of the repo.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

status=0

while read -r manifest; do
    directory="$(dirname "$manifest")"
    [ -f "$directory/src/lib.rs" ] || continue
    grep -aq '^pub fn init_metrics()' "$directory/src/lib.rs" || continue
    # Crates whose metrics are unconditional have no feature to test.
    grep -aq '^metrics = ' "$manifest" || continue

    crate="$(grep -am1 '^name = ' "$manifest" | sed 's/name = "\(.*\)"/\1/')"
    echo "checking $crate with --features metrics"
    if ! cargo check --locked -p "$crate" --features metrics --lib; then
        echo "ERROR: $crate does not build with its own metrics feature."
        echo "       Its init_metrics() calls a dependency whose metrics feature it never"
        echo "       enables. Add the missing \"<dependency>/metrics\" to [features] metrics."
        status=1
    fi
done < <(find linera-* -maxdepth 2 -name Cargo.toml -not -path '*/target/*' | sort)

exit "$status"
