#!/bin/bash

# Run cargo owner on the current workspace.
#
# Usage: scripts/owner.sh packages.txt ARGS..

set -e

PACKAGES="$1"
shift

echo "Type 'yes' to run the following command on every crate: cargo owner $@"
read INPUT
if [ "$INPUT" != "yes" ]; then exit 0; fi

grep -v '^#' "$PACKAGES" | while read LINE; do
    LINE=($LINE)
    NAME="${LINE[0]}"
    (
        set -x;
        cargo owner "$@" "$NAME"
    )
done
