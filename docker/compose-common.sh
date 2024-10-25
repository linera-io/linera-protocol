#!/bin/bash

build_docker_image() {
    GIT_COMMIT=$(git rev-parse --short HEAD)

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        docker build --build-arg git_commit="$GIT_COMMIT" -f docker/Dockerfile . -t linera
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        CPU_ARCH=$(sysctl -n machdep.cpu.brand_string)
        if [[ "$CPU_ARCH" == *"Apple"* ]]; then
            docker build --build-arg git_commit="$GIT_COMMIT" --build-arg target=aarch64-unknown-linux-gnu -f docker/Dockerfile -t linera .
        else
            echo "Unsupported Architecture: $CPU_ARCH"
            exit 1
        fi
    else
        echo "Unsupported OS: $OSTYPE"
        exit 1
    fi
}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
ROOT_DIR=$SCRIPT_DIR/..
