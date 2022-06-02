#!/bin/bash

NUM_VALIDATORS="$1"
NUM_SHARDS="$2"

if [ -z "$NUM_VALIDATORS" ] || [ -z "$NUM_SHARDS" ]; then
    echo "USAGE: ./integration-test.sh NUM_VALIDATORS NUM_SHARDS" >&2
    exit 1
fi

# Generate one service for each validator
server_services() {
    for server in $(seq 1 ${NUM_VALIDATORS}); do
        cat << EOF
  server_${server}:
    build:
      context: .
      target: server
    command: ./run-server.sh ${server} ${NUM_SHARDS}
    depends_on:
      - setup
  proxy_${server}:
    build:
      context: .
      target: proxy
    command: ./run-proxy.sh ${server}
    depends_on:
      - setup
EOF
    done
}

# Generate final Docker Compose configuration
cat > docker-compose.yml << EOF
services:
  setup:
    build:
      context: .
      target: setup
    command: ./setup.sh ${NUM_VALIDATORS} ${NUM_SHARDS}
$(server_services)
  client:
    build:
      context: .
      target: client
    command: ./run-client.sh
    depends_on:
      - setup
EOF

docker compose up
