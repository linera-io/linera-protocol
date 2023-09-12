#!/bin/bash

# Helper script for re-installing helm charts locally.
helm uninstall linera-core;
sleep 0.5;
helm install linera-core . --values values-local.yaml;
sleep 0.5;
echo "Pods:";
kubectl get pods;
echo -e "\nServices:";
kubectl get svc;
sleep 2;
kubectl exec -it shards-0  -- cat wallet.json > /tmp/wallet.json;

echo 'export LINERA_WALLET=/tmp/wallet.json'
echo 'export LINERA_STORAGE="rocksdb:/tmp/linera.db"'


