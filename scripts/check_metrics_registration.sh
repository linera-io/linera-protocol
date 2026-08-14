#!/bin/bash

# Check that every metric declaration is reachable from an `init_metrics`.
#
# A metric only appears in `/metrics` once it has been registered, and `declare_metrics!`
# statics register on first use. A module left out of its crate's `init_metrics`, or a crate
# left out of a dependent's, therefore reverts silently to the old behaviour: the metric shows
# up only after the code path that observes it first runs, and disappears again whenever the
# process is replaced. Nothing else catches this — the generated `init_metrics` is `pub`, so
# rustc's dead_code lint does not fire when it goes uncalled.

set -uo pipefail

# Make sure we're at the source of the repo.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

status=0

# Nearest ancestor holding a Cargo.toml, so nested members like linera-faucet/server resolve.
crate_dir_of() {
    local dir
    dir="$(dirname "$1")"
    while [ "$dir" != "." ] && [ ! -f "$dir/Cargo.toml" ]; do
        dir="$(dirname "$dir")"
    done
    echo "$dir"
}

# 1. Every module that declares metrics must be called from somewhere in its own crate.
while read -r file; do
    crate="$(crate_dir_of "$file")"

    # Module path of the file itself, relative to src/, with mod.rs and lib.rs collapsed.
    path="${file#"$crate"/src/}"
    path="${path%.rs}"
    path="${path%/mod}"
    [ "$path" = "lib" ] && path=""

    module="$(grep -aoE 'mod [a-z_]*metrics \{' "$file" | head -1 | awk '{print $2}')"
    if [ -n "$module" ]; then
        # An inline `mod <name>metrics` block.
        path="${path:+$path/}$module"
    fi

    # A call only has to name the last two segments to be unambiguous within a crate; at the
    # crate root there is only one.
    token="$(echo "$path" | tr '/' '\n' | tail -2 | paste -sd: - | sed 's/:/::/g')"

    # A two-segment token is unambiguous wherever it appears, so any path may precede it. A
    # single-segment one is a crate-root module, and must not be satisfied by some
    # `<other>::metrics::init_metrics()` belonging to an inline module.
    if [[ "$token" == *::* ]]; then
        found=$(grep -raF "${token}::init_metrics" --include='*.rs' "$crate/src" | head -1)
    else
        found=$(grep -raE "(^|[^:[:alnum:]_])${token}::init_metrics" --include='*.rs' "$crate/src" | head -1)
    fi

    if [ -z "$found" ]; then
        echo "ERROR: $file declares metrics that no init_metrics() in $crate calls."
        echo "       Add a call to ${token}::init_metrics() reachable from that crate's init_metrics()."
        status=1
    fi
done < <(grep -ral 'declare_metrics!' --include='*.rs' linera-*/src linera-*/*/src 2>/dev/null)

# 2. Every crate exposing `init_metrics` must be initialized by each dependent that exposes one,
# so a binary reaching one aggregate reaches everything below it.
#
# linera-bridge is deliberately excluded: it serves no /metrics endpoint, and its `relay`
# feature does not turn on the `metrics` feature of its dependencies, so calling their
# `init_metrics` would not compile.
declare -A HAS_INIT
while read -r manifest; do
    dir="$(dirname "$manifest")"
    [ -f "$dir/src/lib.rs" ] || continue
    grep -aq '^pub fn init_metrics()' "$dir/src/lib.rs" || continue
    name="$(grep -am1 '^name = ' "$manifest" | sed 's/name = "\(.*\)"/\1/')"
    HAS_INIT["$name"]="$dir"
done < <(find linera-* -maxdepth 2 -name Cargo.toml -not -path '*/target/*')

for name in "${!HAS_INIT[@]}"; do
    [ "$name" = "linera-bridge" ] && continue
    dir="${HAS_INIT[$name]}"
    while read -r dependency; do
        [ -n "${HAS_INIT[$dependency]:-}" ] || continue
        [ "$dependency" = "$name" ] && continue
        call="${dependency//-/_}::init_metrics()"
        if ! grep -aqF "$call" "$dir/src/lib.rs"; then
            echo "ERROR: $name depends on $dependency but its init_metrics() never calls $call."
            status=1
        fi
    done < <(awk '/^\[dependencies\]/{f=1;next} /^\[/{f=0} f' "$dir/Cargo.toml" \
             | grep -oE '^linera-[a-z-]+')
done

if [ "$status" -ne 0 ]; then
    echo
    echo "See scripts/check_metrics_registration.sh for why this matters."
fi

exit "$status"
