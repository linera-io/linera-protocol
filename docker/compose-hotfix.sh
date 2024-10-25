#!/bin/bash

set -xe

source ./compose-common.sh

BRANCH_NAME="$1"
ACTUAL_BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)

if [[ "$BRANCH_NAME" != "$ACTUAL_BRANCH_NAME" ]]; then
    echo "Error: Expected branch '$BRANCH_NAME', but you are on '$ACTUAL_BRANCH_NAME'."
    exit 1
fi

cd "$ROOT_DIR"
git pull

build_docker_image

cd "$SCRIPT_DIR"

SHARDS=$(docker compose config --services | grep '^shard' | grep -v 'shard-init')
docker compose up --wait $SHARDS proxy
