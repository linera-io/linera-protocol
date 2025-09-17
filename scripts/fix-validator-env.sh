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

# Source the deployment info
source docker/.deployment-info

# Create .env file
cat > docker/.env << EOF
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