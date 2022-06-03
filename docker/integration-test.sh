#!/bin/bash

NUM_VALIDATORS="$1"
NUM_SHARDS="$2"

if [ -z "$NUM_VALIDATORS" ] || [ -z "$NUM_SHARDS" ]; then
    echo "USAGE: ./integration-test.sh NUM_VALIDATORS NUM_SHARDS" >&2
    exit 1
fi

# Generate final Kubernetes description
cat > zefchain-k8s.yml << EOF
---
apiVersion: v1
kind: Service
metadata:
  name: servers
  labels:
    app: server
spec:
  clusterIP: None
  selector:
    app: server
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: server
spec:
  selector:
    matchLabels:
      app: server
  serviceName: servers
  replicas: ${NUM_VALIDATORS}
  template:
    metadata:
      labels:
        app: server
    spec:
      terminationGracePeriodSeconds: 10
      containers:
        - name: server
          image: zefchain-test-server
          imagePullPolicy: Never
          command: ["./run-server.sh"]
          args:
            - "${NUM_SHARDS}"
        - name: proxy
          image: zefchain-test-proxy
          imagePullPolicy: Never
          command: ["./run-proxy.sh"]
---
apiVersion: v1
kind: Service
metadata:
  name: setup
  labels:
    app: setup
spec: 
  selector:
    app: setup
  ports:
    - name: http
      protocol: TCP
      port: 80
      targetPort: http-port
---
apiVersion: v1
kind: Pod
metadata:
  name: setup-pod
  labels:
    app: setup
spec:
  containers:
    - name: setup
      image: zefchain-test-setup
      imagePullPolicy: Never
      ports:
        - containerPort: 8080
          name: http-port
      command: ["./setup.sh"]
      args:
        - "${NUM_VALIDATORS}"
        - "${NUM_SHARDS}"
---
apiVersion: batch/v1
kind: Job
metadata:
  name: client
spec:
  completions: 1
  parallelism: 1
  backoffLimit: 1
  template:
    metadata:
      labels:
        app: client
    spec:
      restartPolicy: Never
      containers:
        - name: client
          image: zefchain-test-client
          imagePullPolicy: Never
          command: ["./run-client.sh"]
EOF
