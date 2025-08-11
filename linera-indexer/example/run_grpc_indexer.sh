#!/bin/bash

# Example script to run the gRPC indexer server

# Set default values
INDEXER_PORT=${INDEXER_PORT:-8081}
INDEXER_DATABASE_PATH=${INDEXER_DATABASE_PATH:-"./indexer.db"}

echo "Starting gRPC Indexer Server"
echo "Port: $INDEXER_PORT"
echo "Database: $INDEXER_DATABASE_PATH"

# Run the gRPC indexer
cargo run --bin linera-indexer-grpc -- --port $INDEXER_PORT --database-path "$INDEXER_DATABASE_PATH"
