#!/usr/bin/env bash
# Quick script to extract wallet/keys from Docker volumes
# For operators who need their keys NOW

set -euo pipefail

echo "🔑 Extracting Validator Keys from Docker Volumes"
echo "================================================"
echo ""

# Method 1: Direct copy from running containers
echo "Method 1: Checking running containers..."
for container in proxy shard; do
	if docker exec ${container} ls /linera/*.json 2>/dev/null; then
		echo "✓ Found keys in ${container}, extracting..."
		docker cp ${container}:/linera/. ./validator-keys-${container}/ 2>/dev/null || true
	fi
done

# Method 2: Mount volume to temporary container
echo ""
echo "Method 2: Checking Docker volumes..."
for vol in $(docker volume ls -q | grep -E 'linera|scylla'); do
	echo "→ Inspecting volume: ${vol}"
	# Create temp container to access volume
	docker run --rm -v ${vol}:/backup -v $(pwd):/output alpine sh -c "
        if find /backup -name '*.json' -o -name 'wallet*' -o -name 'keystore*' 2>/dev/null; then
            echo '  ✓ Found potential key files'
            cp -r /backup/* /output/volume-${vol}/ 2>/dev/null || true
        fi
    " 2>/dev/null || true
done

# Method 3: Direct volume path access (requires root)
echo ""
echo "Method 3: Direct volume access (may require sudo)..."
DOCKER_ROOT="/var/lib/docker/volumes"
if [ -d "${DOCKER_ROOT}" ]; then
	for vol_path in ${DOCKER_ROOT}/*/; do
		vol_name=$(basename ${vol_path})
		if [[ ${vol_name} == *"linera"* ]] || [[ ${vol_name} == *"scylla"* ]]; then
			echo "→ Checking ${vol_name}"
			sudo find ${vol_path} -name "*.json" -o -name "wallet*" -o -name "keystore*" 2>/dev/null | while read f; do
				echo "  ✓ Found: ${f}"
				sudo cp -p "${f}" "./direct-${vol_name}-$(basename ${f})" 2>/dev/null || true
			done
		fi
	done
fi

echo ""
echo "🔍 Searching complete. Check current directory for extracted files:"
ls -la *.json wallet* keystore* 2>/dev/null || echo "No key files found in standard locations"
echo ""
echo "⚠️  If no files were found, try:"
echo "  1. docker exec proxy cat /linera/wallet.json > wallet.json"
echo "  2. docker exec proxy cat /linera/keystore.json > keystore.json"
echo "  3. Check if keys are in environment: docker exec proxy env | grep -i key"
