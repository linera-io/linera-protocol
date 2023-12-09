validator_number=$1

echo "Updating linera-core on validator-$validator_number"
helm upgrade linera-core . \
	--values "working/devnet/validator-$validator_number-gcp.yaml" \
        --wait \
        --set installCRDs=true \
        --set "validator.serverConfig=working/server_$validator_number.json" \
        --set validator.genesisConfig=working/genesis.json

kubectl rollout restart statefulset shards
