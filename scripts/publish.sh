#!/bin/bash

set -x -e

# Usage: scripts/publish.sh packages.txt

# Publish the given packages.
grep -v '^#' "$1" | while read LINE; do
    cargo publish -p $LINE
done
