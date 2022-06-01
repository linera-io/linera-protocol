#!/bin/bash

NUM_VALIDATORS="$1"
NUM_SHARDS="$2"

if [ -z "$NUM_VALIDATORS" ] || [ -z "$NUM_SHARDS" ]; then
    echo "USAGE: ./integration-test.sh NUM_VALIDATORS NUM_SHARDS" >&2
    exit 1
fi

# Generate one volume for each validator's configuration files
server_volumes() {
    for server in $(seq 1 ${NUM_VALIDATORS}); do
        echo "  server_${server}:"
    done
}

# Generate server volume mounts for the setup container
server_volumes_for_setup() {
    for server in $(seq 1 ${NUM_VALIDATORS}); do
        echo "      - server_${server}:/config/server_${server}"
    done
}

# Generate one service for each validator
server_services() {
    for server in $(seq 1 ${NUM_VALIDATORS}); do
        cat << EOF
  server_${server}:
    build:
      context: .
      target: server
    command: ./run-server.sh ${NUM_SHARDS}
    volumes:
      - common:/config/common:ro
      - server_${server}:/config/server:ro
    depends_on:
      - setup
  proxy_${server}:
    build:
      context: .
      target: proxy
    command: ./run-proxy.sh
    volumes:
      - server_${server}:/config/server:ro
    depends_on:
      - setup
EOF
    done
}

# Generate final Docker Compose configuration
cat > docker-compose.yml << EOF
volumes:
  common:
  client:
$(server_volumes)

services:
  setup:
    build:
      context: .
      target: setup
    command: ./setup.sh ${NUM_VALIDATORS} ${NUM_SHARDS}
    volumes:
      - common:/config/common
      - client:/config/client
$(server_volumes_for_setup)
$(server_services)
  client:
    build:
      context: .
      target: client
    command: ./run-client.sh
    volumes:
      - common:/config/common:ro
      - client:/config/client
    depends_on:
      - setup
EOF

docker compose up
