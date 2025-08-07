#!/bin/bash

# Removing data directories so that the next run starts with a clean state
echo "Removing exporter data..."
rm -rf ./exporter-data/
echo "Removing indexer data..."
rm -rf ./indexer-data/
echo "All data removed"
docker-compose -f ./docker-compose.indexer-test.yml up --force-recreate --remove-orphans