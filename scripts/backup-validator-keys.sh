#!/usr/bin/env bash
# Script to backup validator keys and wallet from Docker volumes
# CRITICAL: Run this regularly to protect against data loss

set -euo pipefail

BACKUP_DIR="validator-backup-$(date +%Y%m%d-%H%M%S)"
DOCKER_COMPOSE_DIR="docker"

echo "🔑 Linera Validator Key Backup Tool"
echo "===================================="
echo ""

# Create backup directory
mkdir -p "${BACKUP_DIR}"
cd "${BACKUP_DIR}"

echo "📁 Created backup directory: ${BACKUP_DIR}"
echo ""

# Function to backup from container
backup_container_files() {
	local container="$1"
	local paths="$2"
	local description="$3"

	echo "→ Checking ${description} in container: ${container}"

	# Check if container exists and is running
	if ! docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
		echo "  ⚠️  Container ${container} not running, skipping..."
		return
	fi

	# Create container backup directory
	mkdir -p "${container}"

	# Try to backup each path
	for path in ${paths}; do
		# Check if path exists in container
		if docker exec "${container}" test -e "${path}" 2>/dev/null; then
			echo "  ✓ Found ${path}"
			docker cp "${container}:${path}" "${container}/" 2>/dev/null || true
		fi
	done
}

# Backup from proxy container
backup_container_files "proxy" \
	"/linera/*.json /root/.config/linera /data /linera-storage" \
	"proxy wallet and storage"

# Backup from shard containers (there may be multiple)
for shard in $(docker ps --format '{{.Names}}' | grep -E '^(docker-)?shard'); do
	backup_container_files "${shard}" \
		"/linera/*.json /root/.config/linera /data /linera-storage" \
		"shard wallet and storage"
done

# Backup ScyllaDB data volume info (for reference)
echo ""
echo "→ Getting volume information..."

SCYLLA_VOLUME_NAME=$(docker volume ls --format '{{.Name}}' | grep 'linera-scylla-data' | head -n1)
if [[ -n "$SCYLLA_VOLUME_NAME" ]]; then
	docker volume inspect "$SCYLLA_VOLUME_NAME" >scylla-volume-info.json
	echo "  ✅ Found volume: $SCYLLA_VOLUME_NAME"
else
	echo "  ⚠️  ScyllaDB volume not found"
fi

# Backup docker-compose environment
echo ""
echo "→ Backing up deployment configuration..."
if [[ -f "../${DOCKER_COMPOSE_DIR}/.env" ]]; then
	cp "../${DOCKER_COMPOSE_DIR}/.env" ./env-backup
	echo "  ✓ Backed up .env file"
elif [[ -f "../${DOCKER_COMPOSE_DIR}/.deployment-info" ]]; then
	cp "../${DOCKER_COMPOSE_DIR}/.deployment-info" ./deployment-info-backup
	echo "  ✓ Backed up .deployment-info file"
fi

# Get container configuration
echo ""
echo "→ Saving container configuration..."
docker compose -f "../${DOCKER_COMPOSE_DIR}/docker-compose.yml" config >docker-compose-config.yml 2>/dev/null || true

# Create restore instructions
cat >RESTORE_INSTRUCTIONS.md <<EOF
# Validator Key Restoration Instructions

## ⚠️ CRITICAL FILES

Look for these files in your backup:
- \`wallet.json\` - Your validator wallet
- \`keystore.json\` - Your validator keystore
- Any \`.json\` files containing keys
- \`.config/linera/\` directory

## To Restore

1. Stop your validator:
   \`\`\`bash
   cd docker && docker compose down
   \`\`\`

2. Copy wallet files back to container volumes:
   \`\`\`bash
   # After starting containers
   docker cp wallet.json proxy:/linera/
   docker cp keystore.json proxy:/linera/
   \`\`\`

3. Restart services:
   \`\`\`bash
   docker compose up -d
   \`\`\`

## Emergency Recovery

If volumes are corrupted, you may need to:
1. Delete the old volumes: \`docker volume rm $SCYLLA_VOLUME_NAME\`
2. Recreate from this backup
3. Re-sync with the network

## Backup Contents

This backup was created: $(date)
Host: $(hostname)
EOF

# Create tarball of entire backup
cd ..
tar -czf "${BACKUP_DIR}.tar.gz" "${BACKUP_DIR}"

echo ""
echo "✅ Backup completed successfully!"
echo ""
echo "📦 Backup saved to:"
echo "   Directory: $(pwd)/${BACKUP_DIR}/"
echo "   Archive:   $(pwd)/${BACKUP_DIR}.tar.gz"
echo ""
echo "⚠️  IMPORTANT: Copy ${BACKUP_DIR}.tar.gz to a safe location OFF this server!"
echo ""
echo "Suggested backup locations:"
echo "  - Your local machine: scp $(hostname):$(pwd)/${BACKUP_DIR}.tar.gz ~/"
echo "  - Cloud storage: gsutil cp ${BACKUP_DIR}.tar.gz gs://your-backup-bucket/"
echo "  - Another server: rsync -av ${BACKUP_DIR}.tar.gz user@backup-server:~/"
