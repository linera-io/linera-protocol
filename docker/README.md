# Integration test using Kubernetes

To build the Docker images, I suggest to do it in a new temporary directory, because SSH keys for
accessing GitHub need to be used when building the images. The steps to do this are:

```
mkdir /tmp/zefchain-docker
cd /tmp/zefchain-docker

cp $repo/docker/{Dockerfile,fetch-config-file.sh,run-client.sh,run-proxy.sh,run-server.sh,setup.sh} .

sed -e 's|/home/[^/]*/|/root/|g' ~/.ssh/config > ssh-config
cp ~/.ssh/{github_rsa,known_hosts} .

for image in client proxy server setup; do
    docker build -t "zefchain-test-$image" . --target "$image"
done
```

To run the test on a local Kubernetes cluster simulated by KinD, the following steps can be used
(assuming that `kubectl` and `kind` have been installed on the host):

```
kind create cluster

for image in client proxy server setup; do
    kind load docker-image "zefchain-test-$image"
done

$repo/docker/integration_test.sh $NUM_VALIDATORS $NUM_SHARDS
kubectl apply -f zefchain-k8s.yml
```

To see the logs of the test, use:

```
kubectl logs -l app=client
```

To access the validators from outside the cluster, kind should be configured to support load
balancers, using this [guide](https://kind.sigs.k8s.io/docs/user/loadbalancer/).
