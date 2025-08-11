#!/bin/bash
# NOTE: This script assumes to be called from within the `/docker` directory.
echo "Building linera images..."
docker build -f ./Dockerfile.indexer-test -t linera-all-test ..

echo "Building linera explorer image..."
docker build -f ./Dockerfile.explorer -t linera-explorer-new ..


echo "Done building images"