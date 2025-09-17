#!/usr/bin/env bash
# Script to generate .env file from legacy .deployment-info file
# This fixes validators that were deployed before .env support was added

set -euo pipefail

if [ ! -f "docker/.deployment-info" ]; then
	echo "Error: docker/.deployment-info not found."
	echo "Either:"
	echo "  1. Run this from the linera-protocol repository root, or"
	echo "  2. Your deployment already uses .env (check docker/.env)"
	exit 1
fi

# Extract PUBLIC_KEY - handle multi-line values with log messages
# Get the PUBLIC_KEY line and the next line (in case the key is on the next line)
RAW_PUBLIC_KEY=$(grep -A1 "^PUBLIC_KEY=" docker/.deployment-info | tail -n +2)

# If the first line after PUBLIC_KEY= doesn't contain the key, get both lines
if [[ ! "$RAW_PUBLIC_KEY" =~ [0-9a-f]{64},[0-9a-f]{64} ]]; then
	RAW_PUBLIC_KEY=$(grep -A1 "^PUBLIC_KEY=" docker/.deployment-info | cut -d= -f2-)
fi

# Clean up and extract the hex key pattern
PUBLIC_KEY=$(echo "$RAW_PUBLIC_KEY" |
	sed 's/\x1b\[[0-9;]*m//g' |
	tr '\n' ' ' |
	grep -oE '[0-9a-f]{64},[0-9a-f]{64}' |
	head -1)

# If PUBLIC_KEY extraction failed, try getting it from the whole file
if [[ -z "$PUBLIC_KEY" ]]; then
	# Remove ANSI codes from entire file and search for key pattern
	PUBLIC_KEY=$(sed 's/\x1b\[[0-9;]*m//g' docker/.deployment-info |
		grep -oE '[0-9a-f]{64},[0-9a-f]{64}' |
		head -1)
fi

# Validate the PUBLIC_KEY format
if [[ ! "$PUBLIC_KEY" =~ ^[0-9a-f]{64},[0-9a-f]{64}$ ]]; then
	echo "Error: Unable to extract valid PUBLIC_KEY from .deployment-info"
	echo "Expected format: 64 hex chars, comma, 64 hex chars"
	echo "Got: $PUBLIC_KEY"
	exit 1
fi

# Parse other variables normally
eval "$(grep -E '^(HOST|EMAIL|BRANCH|COMMIT|IMAGE|CUSTOM_TAG|GENESIS_BUCKET|GENESIS_PATH_PREFIX|GENESIS_URL|SHARDS|XFS_PATH|CACHE_SIZE)=' docker/.deployment-info)"

echo "Extracted configuration:"
echo "  HOST=${HOST}"
echo "  EMAIL=${EMAIL}"
echo "  PUBLIC_KEY=${PUBLIC_KEY}"
echo "  GENESIS_URL=${GENESIS_URL}"
echo ""

# Create .env file
cat >docker/.env <<EOF
# Validator Deployment Configuration
# Migrated from .deployment-info: $(date -Iseconds)
# This file is now the source of truth for Docker Compose configuration

# Deployment metadata
DEPLOYMENT_HOST=${HOST}
DEPLOYMENT_EMAIL=${EMAIL}
DEPLOYMENT_PUBLIC_KEY=${PUBLIC_KEY}
DEPLOYMENT_BRANCH=${BRANCH}
DEPLOYMENT_COMMIT=${COMMIT}
DEPLOYMENT_CUSTOM_TAG=${CUSTOM_TAG}
DEPLOYMENT_DATE=$(date -Iseconds)

# Domain and SSL configuration (used by docker-compose.yml)
DOMAIN=${HOST}
ACME_EMAIL=${EMAIL}

# Genesis configuration (critical for validator operation)
GENESIS_URL=${GENESIS_URL}
GENESIS_BUCKET=${GENESIS_BUCKET}
GENESIS_PATH_PREFIX=${GENESIS_PATH_PREFIX}

# Validator configuration
VALIDATOR_PUBLIC_KEY=${PUBLIC_KEY}

# Docker image
LINERA_IMAGE=${IMAGE}

# ScyllaDB configuration
NUM_SHARDS=${SHARDS}
$([[ "${XFS_PATH}" != "N/A" ]] && echo "XFS_PATH=${XFS_PATH}" || true)
$([[ "${XFS_PATH}" != "N/A" ]] && echo "CACHE_SIZE=${CACHE_SIZE}" || true)

# Network configuration
FAUCET_PORT=8080
LINERA_STORAGE_SERVICE_PORT=1235
EOF

echo "✅ Successfully created docker/.env from docker/.deployment-info"
echo ""
echo "You can now safely restart your validator with:"
echo "  cd docker && docker compose down && docker compose up -d"
