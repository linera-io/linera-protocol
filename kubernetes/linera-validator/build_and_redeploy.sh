#!/bin/bash
# Helper script for re-installing helm charts locally.

# Default variable values
cloud_mode=false
port_forward=false
do_build=true

# Function to display script usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo " -h, --help       Display this help message"
    echo " --cloud          Use the Docker Image from Cloud build"
    echo " --port-forward   Start port forwarding at the end of the script, so that the validator is accessible. Don't use this if you plan to use this terminal for something else after running this script"
    echo " --no-build       Don't actually build another version of the Docker image, just use the existing one for the current mode (cloud or not)"
}

# Function to handle options and arguments
handle_options() {
    while [ $# -gt 0 ]; do
        case $1 in
        -h | --help)
            usage
            exit 0
            ;;
        --cloud)
            cloud_mode=true
            ;;
        --port-forward)
            port_forward=true
            ;;
        --no-build)
            do_build=false
            ;;
        *)
            echo "Invalid option: $1" >&2
            usage
            exit 1
            ;;
        esac
        shift
    done
}

# Main script execution
handle_options "$@"

# Perform the desired actions based on the provided flags and arguments
if [ "$cloud_mode" = true ]; then
    if [ "$do_build" = true ]; then
        current_dir=$(pwd)
        github_root=$(git rev-parse --show-toplevel 2>/dev/null)

        # Got to repo root to run GCloud build
        cd "$github_root"
        gcloud builds submit --config test-cloudbuild-local.yaml --timeout="3h" --machine-type=e2-highcpu-32 || exit 1

        # Back to current dir to run redeploy
        cd "$current_dir"
    fi

    # If there's already a kind cluster running, this will fail, and that's fine. We just want to make sure there's a kind
    # cluster running
    kind create cluster

    docker_image="us-docker.pkg.dev/linera-io-dev/linera-docker-repo/linera-test-local:latest"
    docker pull $docker_image || exit 1
    kind load docker-image $docker_image || exit 1
    if [ "$port_forward" = true ]; then
        ./redeploy.sh --cloud --port-forward
    else
        ./redeploy.sh --cloud
    fi
else
    docker_image="linera-test:latest"
    if [ "$do_build" = true ]; then
        docker build -f ../../Dockerfile.aarch64 ../../ -t $docker_image || exit 1
    fi

    kind create cluster
    kind load docker-image $docker_image || exit 1
    if [ "$port_forward" = true ]; then
        ./redeploy.sh --port-forward
    else
        ./redeploy.sh
    fi
fi
