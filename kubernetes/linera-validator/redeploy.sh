#!/bin/bash
# Helper script for re-installing helm charts locally.

# Default variable values
cloud_mode=false
port_forward=false

# Function to display script usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo " -h, --help       Display this help message"
    echo " --cloud          Use the Docker Image from Cloud build"
    echo " --port-forward   Start port forwarding at the end of the script, so that the validator is accessible. Don't use this if you plan to use this terminal for something else after running this script"
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

helm uninstall linera-core;
sleep 0.5;

if [ "$cloud_mode" = true ]; then
    helm install linera-core . --values values-local-with-cloud-build.yaml;
else
    helm install linera-core . --values values-local.yaml;
fi

sleep 0.5;
echo "Pods:";
kubectl get pods;
echo -e "\nServices:";
kubectl get svc;
sleep 2;
if [ "$cloud_mode" = true ]; then
    docker rm linera-test-local;
    docker run -d --name linera-test-local us-docker.pkg.dev/linera-io-dev/linera-docker-repo/linera-test-local:latest && docker cp linera-test-local:/opt/linera/wallet.json /tmp/
else
    docker rm linera-test-local;
    docker run -d --name linera-test-local linera-test:latest && docker cp linera-test-local:/opt/linera/wallet.json /tmp/
fi

echo -e "\nMake sure the terminal you'll run the linera client from has these exports:"
echo 'export LINERA_WALLET=/tmp/wallet.json'
echo 'export LINERA_STORAGE="rocksdb:/tmp/linera.db"'

export LINERA_WALLET=/tmp/wallet.json
export LINERA_STORAGE="rocksdb:/tmp/linera.db"

# Get the pod name
validator_pod_name=$(kubectl get pods | grep validator | awk '{ print $1 }')
echo -e "\nTo port forward yourself, run:"
echo -e "kubectl port-forward $validator_pod_name 19100:19100\n"

if [ "$port_forward" = true ]; then
    kubectl port-forward $validator_pod_name 19100:19100
fi