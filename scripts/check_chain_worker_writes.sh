#!/bin/bash

# Check that every chain-worker storage write goes through `poison_if_must_reload`.
#
# A storage write failing with `must_reload_view` evicts the chain worker from the cache, but
# eviction alone does not stop the evicted instance from writing: `chain_workers` holds a `Weak`,
# and the per-chain lock lives inside the instance, so a writer already queued on that lock keeps
# going while the next request loads a second `ChainStateView` for the same chain. Two live views
# race on storage and split the durable state by write granularity, which is data corruption.
#
# `poison_if_must_reload` sets the `poisoned` flag while the write guard is still held, so the
# queued writer fails its own `check_not_poisoned` instead of writing through a retired instance.
# A write that skips the helper silently reopens that window, and nothing else catches it: the
# error still propagates, the worker is still evicted, and every test still passes.

set -uo pipefail

# Make sure we're at the source of the repo.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

status=0

# Fail loudly rather than vacuously: if the chain worker moves or the helper is renamed, the scan
# below would find nothing to complain about and this check would pass while guarding nothing.
if [ ! -d linera-core/src/chain_worker ]; then
    echo "ERROR: linera-core/src/chain_worker does not exist; update this script's scan root."
    exit 1
fi
if ! grep -rq 'fn poison_if_must_reload' linera-core/src/chain_worker; then
    echo "ERROR: poison_if_must_reload is gone from linera-core/src/chain_worker."
    echo "       If it was renamed, rename it here too; if it was removed, this check is moot"
    echo "       and the invariant it protects needs a new home."
    exit 1
fi

# Rust wraps long method chains, so lines starting with `.` are folded into the statement above
# before matching, and `//` comments are dropped so prose cannot satisfy the check. The helper
# must appear on the same statement or the one immediately after — that is the only shape in use,
# and a wider window would let one guarded write vouch for an unguarded neighbour.
while read -r file; do
    awk -v file="$file" '
        {
            line = $0
            sub(/\/\/.*/, "", line)
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
            if (line ~ /^\./ && n > 0) {
                buf[n] = buf[n] line
            } else {
                n++
                buf[n] = line
                lineno[n] = NR
            }
        }
        END {
            for (i = 1; i <= n; i++) {
                if (buf[i] !~ /storage\.[a-z_]*(write|save|delete|remove|persist)[a-z_]*\(/)
                    continue
                if (buf[i] ~ /poison_if_must_reload/) continue
                if (i < n && buf[i + 1] ~ /poison_if_must_reload/) continue
                printf "ERROR: %s:%d: storage write is not guarded by poison_if_must_reload.\n", \
                    file, lineno[i]
                printf "       %s\n", buf[i]
                bad = 1
            }
            exit bad
        }
    ' "$file" || status=1
done < <(find linera-core/src/chain_worker -name '*.rs')

if [ "$status" -ne 0 ]; then
    echo
    echo "Wrap the write so the failure poisons this instance before the guard is released:"
    echo "    let result = self.storage.write_something(..).await;"
    echo "    self.poison_if_must_reload(result)?;"
    echo
    echo "See scripts/check_chain_worker_writes.sh for why this matters."
fi

exit "$status"
