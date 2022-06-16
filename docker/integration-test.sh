#!/bin/bash

NUM_VALIDATORS="$1"
NUM_SHARDS="$2"

if [ -z "$NUM_VALIDATORS" ] || [ -z "$NUM_SHARDS" ]; then
    echo "USAGE: ./integration-test.sh NUM_VALIDATORS NUM_SHARDS" >&2
    exit 1
fi

generate_validators() {
    for server in $(seq 1 $NUM_VALIDATORS); do
        cat << EOF
---
apiVersion: v1
kind: Service
metadata:
  name: server-${server}
  labels:
    app: server-${server}-shards
spec:
  clusterIP: None
  selector:
    app: server-${server}-shards
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: server-${server}-shard
spec:
  selector:
    matchLabels:
      app: server-${server}-shards
  serviceName: server-${server}
  replicas: ${NUM_SHARDS}
  template:
    metadata:
      labels:
        app: server-${server}-shards
    spec:
      terminationGracePeriodSeconds: 10
      containers:
        - name: server
          image: zefchain-test-server
          imagePullPolicy: Never
          command: ["./run-server.sh"]
          args:
            - "${NUM_SHARDS}"
---
apiVersion: v1
kind: Service
metadata:
  name: validator-${server}
  labels:
    app: validator-${server}
spec: 
  type: LoadBalancer
  selector:
    app: validator-${server}
  ports:
    - name: zef
      protocol: TCP
      port: 9100
      targetPort: zef-port
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: validator-${server}
spec:
  selector:
    matchLabels:
      app: validator-${server}
  replicas: 1
  template:
    metadata:
      labels:
        app: validator-${server}
    spec:
      terminationGracePeriodSeconds: 10
      containers:
        - name: proxy
          image: zefchain-test-proxy
          imagePullPolicy: Never
          ports:
            - containerPort: 9100
              name: zef-port
          command: ["./run-proxy.sh"]
EOF
    done
}

# Generate final Kubernetes description
cat > zefchain-k8s.yml << EOF
$(generate_validators)
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
