#!/bin/bash
set -euo pipefail
# Helper script for re-installing helm charts locally.

# Default variable values
cloud_mode=
port_forward=
do_build=1
clean=
copy=

# Guard clause check if required binaries are installed
type -P kind >/dev/null || {
    echo "Error: kind not installed."
    exit 1
}
type -P helm >/dev/null || {
    echo "Error: helm not installed."
    exit 1
}

# Function to display script usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo " -h, --help       Display this help message"
    echo " --cloud          Use the Docker Image from Cloud build"
    echo " --port-forward   Start port forwarding at the end of the script, so that the validator is accessible. Don't use this if you plan to use this terminal for something else after running this script"
    echo " --no-build       Don't actually build another version of the Docker image, just use the existing one for the current mode (cloud or not)"
    echo " --clean          Clean up DB state and delete kind cluster before starting a new one. This will guarantee that the Validator state will be clean for the new run"
    echo " --copy           Have the Dockerfile copy over the already built binaries in the target/release directory. Binaries need to be built beforehand. Works only when --cloud is NOT set"
}

# Function to handle options and arguments
handle_options() {
    while [ $# -gt 0 ]; do
        case $1 in
        -h | --help)
            usage
            exit 0
            ;;
        --cloud) cloud_mode=1 ;;
        --port-forward) port_forward=1 ;;
        --no-build) do_build= ;;
        --clean) clean=1 ;;
        --copy) copy=1 ;;
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

if [ -n "$clean" ]; then
    rm -rf /tmp/linera.db
    kind delete cluster
fi

# If there's already a kind cluster running, this will fail, and that's fine. We just want to make sure there's a kind
# cluster running
kind create cluster

# Perform the desired actions based on the provided flags and arguments
if [ -n "$cloud_mode" ]; then
    if [ -n "$do_build" ]; then
        current_dir=$(pwd)
        github_root=$(git rev-parse --show-toplevel 2>/dev/null)

        # Got to repo root to run GCloud build
        cd "$github_root"
        gcloud builds submit --config test-cloudbuild-local.yaml --timeout="3h" --machine-type=e2-highcpu-32

        # Back to current dir to run redeploy
        cd "$current_dir"
    fi

    docker_image="us-docker.pkg.dev/linera-io-dev/linera-docker-repo/linera-test-local:latest"

    docker pull "$docker_image"
else
    docker_image="linera-test:latest"
    if [ -n "$do_build" ]; then
        arch="$(uname -m)"
        docker build \
            -f ../../docker/Dockerfile \
            ${copy:+--build-arg binaries=target/release} \
            --build-arg environment=k8s-local \
            --build-arg target="${arch/#arm/aarch}"-unknown-linux-gnu \
            ../../ \
            -t "$docker_image"
    fi
fi

kind load docker-image "$docker_image"

helm uninstall linera-core --wait || true

if [ -n "$cloud_mode" ]; then
    helm install linera-core . --values values-local-with-cloud-build.yaml --wait --set installCRDs=true
else
    helm install linera-core . --values values-local.yaml --wait --set installCRDs=true
fi

echo "Pods:"
kubectl get pods
echo -e "\nServices:"
kubectl get svc

docker rm linera-test-local || true
docker run -d --name linera-test-local "$docker_image"
docker cp linera-test-local:wallet.json /tmp/
docker cp linera-test-local:linera.db /tmp/

echo -e "\nMake sure the terminal you'll run the linera client from has these exports:"
echo 'export LINERA_WALLET=/tmp/wallet.json'
echo 'export LINERA_STORAGE="rocksdb:/tmp/linera.db"'

export LINERA_WALLET=/tmp/wallet.json
export LINERA_STORAGE="rocksdb:/tmp/linera.db"

# Get the Grafana pod name
grafana_pod_name=$(kubectl get pods | grep grafana | awk '{ print $1 }')
grafana_pass=$(
    kubectl get secret linera-core-grafana -o jsonpath="{.data.admin-password}" | base64 --decode
    echo
)
echo -e "\nTo access Grafana, you need to port forward yourself, that won't be done here. Run:"
echo -e "kubectl port-forward $grafana_pod_name 3000"
echo -e "Grafana Username: admin"
echo -e "Granfa Password: $grafana_pass"
# Get the Validator pod name
validator_pod_name=$(kubectl get pods | grep validator | awk '{ print $1 }')
echo -e "\nTo port forward yourself, run:"
echo -e "kubectl port-forward $validator_pod_name 19100:19100\n"

if [ -n "$port_forward" ]; then
    kubectl port-forward $validator_pod_name 19100:19100
fi
