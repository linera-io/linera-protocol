#!/bin/bash
# NOTE: This script assumes to be called from within the `/docker` directory.
echo "Building linera images..."
set -e
docker build -f ./Dockerfile.indexer -t linera-indexer ..
docker build -f ./Dockerfile.exporter -t linera-exporter ..
docker build -f ./Dockerfile -t linera-test ..
docker build -f ./Dockerfile.explorer -t linera-explorer-new .. --build-arg VITE_API_BASE_URL=http://localhost:3002/api

echo "Done building images"