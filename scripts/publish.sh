#!/bin/bash

# Publish crates from the current workspace.
#
# Usage: scripts/publish.sh packages.txt

set -x -e

# Publish the given packages.
grep -v '^#' "$1" | while read LINE; do
    cargo publish -p "$LINE"
done
