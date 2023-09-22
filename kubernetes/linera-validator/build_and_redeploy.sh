#!/bin/bash
# Helper script for re-installing helm charts locally.

# Default variable values
cloud_mode=false
port_forward=false
do_build=true
clean=false

# Guard clause check if required binaries are installed
which kind > /dev/null || { echo "Error: kind not installed." ; exit 1 ; }
which helm > /dev/null || { echo "Error: egrep not installed." ; exit 1 ; }

# Function to display script usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo " -h, --help       Display this help message"
    echo " --cloud          Use the Docker Image from Cloud build"
    echo " --port-forward   Start port forwarding at the end of the script, so that the validator is accessible. Don't use this if you plan to use this terminal for something else after running this script"
    echo " --no-build       Don't actually build another version of the Docker image, just use the existing one for the current mode (cloud or not)"
    echo " --clean          Clean up DB state and delete kind cluster before starting a new one. This will guarantee that the Validator state will be clean for the new run"
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
        --clean)
            clean=true
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

if [ "$clean" = true ]; then
    rm -rf /tmp/linera.db
    kind delete cluster
fi

# If there's already a kind cluster running, this will fail, and that's fine. We just want to make sure there's a kind
# cluster running
kind create cluster

opt_list=""
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

    docker_image="us-docker.pkg.dev/linera-io-dev/linera-docker-repo/linera-test-local:latest"
    opt_list+=" --cloud"
    
    docker pull $docker_image || exit 1
else
    docker_image="linera-test:latest"
    if [ "$do_build" = true ]; then
        if [ "$(uname -m)" = "x86_64" ]; then
            docker build -f ../../docker/Dockerfile.local ../../ -t $docker_image || exit 1
        else
            docker build -f ../../docker/Dockerfile.local-aarch64 ../../ -t $docker_image || exit 1
        fi
    fi
fi

kind load docker-image $docker_image || exit 1

if [ "$port_forward" = true ]; then
    opt_list+=" --port-forward"
fi

./redeploy.sh $opt_list
