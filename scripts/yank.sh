#!/bin/bash

# Yank crates from the current workspace.
#
# Usage: scripts/yank.sh VERSION packages.txt

set -e

# Yank the given packages.
grep -v '^#' "$2" | while read LINE; do
    LINE=($LINE)
    NAME="${LINE[0]}"
    (
        set -x;
        cargo yank "$NAME"@"$1"
    )
done
