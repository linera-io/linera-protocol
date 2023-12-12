#!/bin/bash
set -euo pipefail

# Guard clause check if required binaries are installed
type -P gcloud >/dev/null || {
    echo "Error: gcloud not installed."
    exit 1
}

# Function to display script usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "This script will build a Docker image using Google Cloud Build"
    echo "Options:"
    echo " -h, --help           Display this help message"
}

# Function to handle options and arguments
handle_options() {
    while [ $# -gt 0 ]; do
        case $1 in
        -h | --help)
            usage
            exit 0
            ;;
        *)
            echo "Invalid option: $1" >&2
            usage >&2
            exit 1
            ;;
        esac
        shift
    done
}

# Main script execution
handle_options "$@"

current_dir=$(pwd)
github_root=$(git rev-parse --show-toplevel 2>/dev/null)

# Got to repo root to run GCloud build
cd "$github_root"
timestamp_tag=$(date +%Y%m%d%H%M%S)
docker_image="us-docker.pkg.dev/linera-io-dev/linera-docker-repo/linera-test-local:$timestamp_tag"

gcloud builds submit --config build-image.yaml --substitutions="_IMAGE_NAME=$docker_image" --timeout="3h" --machine-type=e2-highcpu-32

# Back to current dir to run redeploy
cd "$current_dir"

echo "Pulling $docker_image to local docker"
docker pull "$docker_image"

echo "Docker image built: $docker_image"
