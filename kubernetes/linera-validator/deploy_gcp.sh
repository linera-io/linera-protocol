#!/bin/bash
# Helper script for re-installing helm charts on GCP.
# Will be parameterised over time.

# Guard clause check if required binaries are installed
which gcloud > /dev/null || { echo "Error: gcloud not installed." ; exit 1 ; }
which helm > /dev/null || { echo "Error: helm not installed." ; exit 1 ; }

gcloud container clusters get-credentials linera-io-dev-validator-1 --zone us-east1-b --project linera-io-dev || exit 1;
helm uninstall linera-core
helm install linera-core . --values values-dev-gcp.yaml --wait --set installCRDs=true || exit 1;

echo "Copying wallet and storage from cluster..."
kubectl cp shards-0:/opt/linera/linera.db /tmp/linera.db
kubectl cp shards-0:/opt/linera/wallet.json /tmp/wallet.json

echo -e "\nMake sure the terminal you'll run the linera client from has these exports:"
echo 'export LINERA_WALLET=/tmp/wallet.json'
echo 'export LINERA_STORAGE="rocksdb:/tmp/linera.db"'

export LINERA_WALLET=/tmp/wallet.json
export LINERA_STORAGE="rocksdb:/tmp/linera.db"
