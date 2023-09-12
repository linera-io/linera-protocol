#!/bin/bash

# Helper script for re-installing helm charts locally.
docker build -f ../../Dockerfile.aarch64 ../../ -t linera-test:latest || exit 1;
kind load docker-image linera-test:latest || exit 1;
./redeploy.sh
