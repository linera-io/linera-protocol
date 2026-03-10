#!/bin/bash

# Bump the SDK version number across all relevant files.
#
# Usage: scripts/bump_version.sh <new_version>
#
# Example: scripts/bump_version.sh 0.15.15
#
# This updates:
# - Cargo.toml (workspace.package.version and workspace.dependencies)
# - Cargo.lock files (main, examples, linera-sdk/tests/fixtures)
# - web/@linera/*/package.json
# - docs/RELEASE_VERSION and docs/RELEASE_HASH

set -e

if [ $# -ne 1 ]; then
    echo "Usage: $0 <new_version>"
    echo "Example: $0 0.15.15"
    exit 1
fi

NEW_VERSION="$1"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Validate version format (semver-like).
if ! echo "$NEW_VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$'; then
    echo "Error: Invalid version format: $NEW_VERSION"
    echo "Expected format: X.Y.Z or X.Y.Z-pre"
    exit 1
fi

# Detect current version from workspace Cargo.toml.
OLD_VERSION=$(sed -n '/\[workspace\.package\]/,/^\[/{ s/^version = "\(.*\)"/\1/p; }' "$ROOT/Cargo.toml")

if [ -z "$OLD_VERSION" ]; then
    echo "Error: Could not detect current version from Cargo.toml"
    exit 1
fi

if [ "$OLD_VERSION" = "$NEW_VERSION" ]; then
    echo "Error: New version ($NEW_VERSION) is the same as current version ($OLD_VERSION)"
    exit 1
fi

echo "Bumping version: $OLD_VERSION -> $NEW_VERSION"

# 1. Update workspace version in Cargo.toml.
sed -i '' "s/^version = \"$OLD_VERSION\"/version = \"$NEW_VERSION\"/" "$ROOT/Cargo.toml"

# 2. Update linera-* dependency versions in Cargo.toml.
sed -i '' "s/\(linera-[a-zA-Z_-]* = { version = \"\)$OLD_VERSION\"/\1$NEW_VERSION\"/" "$ROOT/Cargo.toml"

# 3. Regenerate Cargo.lock files using cargo.
LOCK_DIRS=(
    "$ROOT"
    "$ROOT/examples"
    "$ROOT/linera-sdk/tests/fixtures"
)

for dir in "${LOCK_DIRS[@]}"; do
    if [ -f "$dir/Cargo.toml" ]; then
        echo "Updating $dir/Cargo.lock ..."
        (cd "$dir" && cargo update --workspace)
    else
        echo "Warning: $dir/Cargo.toml not found, skipping"
    fi
done

# 4. Update web package.json files.
for pkg in "$ROOT"/web/@linera/*/package.json; do
    if [ -f "$pkg" ]; then
        sed -i '' 's/"version": "'"$OLD_VERSION"'"/"version": "'"$NEW_VERSION"'"/' "$pkg"
        echo "Updated $pkg"
    fi
done

# 5. Update docs/RELEASE_VERSION and docs/RELEASE_HASH.
echo -n "$NEW_VERSION" > "$ROOT/docs/RELEASE_VERSION"
echo "Updated docs/RELEASE_VERSION"
echo -n "$(git -C "$ROOT" rev-parse HEAD)" > "$ROOT/docs/RELEASE_HASH"
echo "Updated docs/RELEASE_HASH"

echo "Done. Version bumped to $NEW_VERSION."
