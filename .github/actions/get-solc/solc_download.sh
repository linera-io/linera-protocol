#!/bin/bash
set -euo pipefail

# CI runs on Linux x86-64. Sources binaries from the Solidity Foundation's
# CDN-fronted distribution (used by Hardhat, Foundry's svm, solc-select,
# py-solc-x, Remix) to avoid the unauthenticated GitHub API rate limit.
BIN_HOST="https://binaries.soliditylang.org/linux-amd64"

SOLC_FOLDER="$HOME/.solc"
mkdir -p "$SOLC_FOLDER"

VERSION="${SOLC_VERSION:-${1:-latest}}"
VERSION_NO_V="${VERSION#v}"

LIST_PATH="$(mktemp)"
trap 'rm -f "$LIST_PATH"' EXIT
echo "Download: $BIN_HOST/list.json"
curl -fsSL -o "$LIST_PATH" "$BIN_HOST/list.json"

if [[ "$VERSION" == "latest" || "$VERSION" == "new" || "$VERSION" == "last" ]]; then
  VERSION_NO_V=$(jq -r '.latestRelease' "$LIST_PATH")
  VERSION="v$VERSION_NO_V"
fi

FILENAME=$(jq -r --arg v "$VERSION_NO_V" '.releases[$v] // empty' "$LIST_PATH")
if [[ -z "$FILENAME" ]]; then
  echo "Solc release $VERSION not found at $BIN_HOST/list.json"
  exit 1
fi

EXPECTED_SHA=$(jq -r --arg p "$FILENAME" '.builds[] | select(.path == $p) | .sha256 // empty' "$LIST_PATH")
EXPECTED_SHA="${EXPECTED_SHA#0x}"

VERSION_DIR="$SOLC_FOLDER/$VERSION"
mkdir -p "$VERSION_DIR"
# Keep the historical filename so the cache key and downstream symlink in
# rust.yml continue to work without changes.
FILE_PATH="$VERSION_DIR/solc-static-linux"

if [[ ! -e "$FILE_PATH" ]]; then
  echo "Download: $BIN_HOST/$FILENAME"
  curl -fsSL -o "$FILE_PATH.tmp" "$BIN_HOST/$FILENAME"
  if [[ -n "$EXPECTED_SHA" ]]; then
    ACTUAL_SHA=$(sha256sum "$FILE_PATH.tmp" | awk '{print $1}')
    if [[ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]]; then
      echo "SHA-256 mismatch for $FILENAME"
      echo "  expected: $EXPECTED_SHA"
      echo "  actual:   $ACTUAL_SHA"
      rm -f "$FILE_PATH.tmp"
      exit 1
    fi
  fi
  mv "$FILE_PATH.tmp" "$FILE_PATH"
fi

chmod 755 "$FILE_PATH"
echo "version: $VERSION"
"$FILE_PATH" --version
