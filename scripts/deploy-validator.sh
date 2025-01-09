#!/bin/bash

set -e

# Make sure we're at the source of the repo.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

docker_compose_plugin_installed() {
    docker compose version >/dev/null 2>&1
}

if ! command_exists docker; then
    echo "Error: Docker is not installed. Please install Docker before running this script."
    exit 1
fi

if ! docker_compose_plugin_installed; then
    echo "Error: Docker Compose is not installed. Please install Docker Compose before running this script."
    exit 1
fi

HOST=$1

# Check if the host is provided as an argument
if [ -z "$HOST" ]; then
  echo "Usage: $0 <host> [--remote-image]"
  exit 1
fi

if [ -n "$2" ]; then
  if [ "$2" != "--remote-image" ]; then
    echo "Usage: $0 <host> [--remote-image]"
    exit 1
  fi
  REMOTE_IMAGE="$2"
fi

# Get the current branch name and replace underscores with dashes
BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
FORMATTED_BRANCH_NAME="${BRANCH_NAME//_/-}"
GIT_COMMIT=$(git rev-parse --short HEAD)

# Variables
PORT="19100"
METRICS_PORT="21100"
GENESIS_URL="https://storage.googleapis.com/linera-io-dev-public/$FORMATTED_BRANCH_NAME/genesis.json"
VALIDATOR_CONFIG="docker/validator-config.toml"
GENESIS_CONFIG="docker/genesis.json"

if [ -z "$REMOTE_IMAGE" ]; then
  echo "Building local image from commit $GIT_COMMIT..."
  docker build --build-arg git_commit="$GIT_COMMIT" -f  docker/Dockerfile . -t linera
  export LINERA_IMAGE=linera
else
  export LINERA_IMAGE="us-docker.pkg.dev/linera-io-dev/linera-public-registry/linera:${BRANCH_NAME}_release"
  echo "Using remote image $LINERA_IMAGE..."
fi

# Create validator configuration file
echo "Creating validator configuration..."
cat > $VALIDATOR_CONFIG <<EOL
server_config_path = "server.json"
host = "$HOST"
port = $PORT
metrics_host = "proxy"
metrics_port = $METRICS_PORT
internal_host = "proxy"
internal_port = 20100
[external_protocol]
Grpc = "ClearText"
[internal_protocol]
Grpc = "ClearText"

[[shards]]
host = "docker-shard-1"
port = $PORT
metrics_host = "docker-shard-1"
metrics_port = $METRICS_PORT

[[shards]]
host = "docker-shard-2"
port = $PORT
metrics_host = "docker-shard-2"
metrics_port = $METRICS_PORT

[[shards]]
host = "docker-shard-3"
port = $PORT
metrics_host = "docker-shard-3"
metrics_port = $METRICS_PORT

[[shards]]
host = "docker-shard-4"
port = $PORT
metrics_host = "docker-shard-4"
metrics_port = $METRICS_PORT
EOL

# Download genesis configuration
echo "Downloading genesis configuration..."
wget -O $GENESIS_CONFIG $GENESIS_URL

cd docker

# Generate validator keys
echo "Generating validator keys..."
PUBLIC_KEY=$(docker run --rm -v "$(pwd):/config" -w /config $LINERA_IMAGE /linera-server generate --validators validator-config.toml)

echo "Validator setup completed successfully."
echo "Starting docker compose..."

docker compose up --wait

echo "Public Key: $PUBLIC_KEY"
