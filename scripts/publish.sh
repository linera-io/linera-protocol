#!/bin/bash

set -x -e

# Usage: grep -v '^#' packages.txt | scripts/publish.sh

# Publish the given packages.
while read LINE; do
    cargo publish -p $LINE
done
