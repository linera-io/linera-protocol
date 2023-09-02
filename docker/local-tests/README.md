# Integration test using Kubernetes

To build the Docker images, the `linera`, `linera-proxy` and `linera-server` binaries have to be built and copied
into this directory. The steps to do this are:

```
cd $repo

cargo build --release
cp target/release/{linera,linera-proxy,linera-server} docker/local-tests

cd docker/local-tests

for image in client proxy server setup; do
    docker build -t "linera-test-$image" . --target "$image"
done
```

To run the test on a local Kubernetes cluster simulated by KinD, the following steps can be used
(assuming that `kubectl` and `kind` have been installed on the host):

```
kind create cluster

for image in client proxy server setup; do
    kind load docker-image "linera-test-$image"
done

$repo/docker/local-tests/integration_test.sh $NUM_VALIDATORS $NUM_SHARDS
kubectl apply -f linera-k8s.yml
```

To see the logs of the test, use:

```
kubectl logs -l app=client
```

To access the validators from outside the cluster, kind should be configured to support load
balancers, using this [guide](https://kind.sigs.k8s.io/docs/user/loadbalancer/).
