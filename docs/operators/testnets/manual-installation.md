# Manual Installation

If you don't want to use the provided deploy script, you can instead choose to
manually roll out your validator deployment.

## Setting up a Linera Validator

For the next section, we'll be working out of the `docker` subdirectory in the
`linera-protocol` repository.

### Creating your Validator Configuration

Validators are configured using a TOML file. You can use the following template
to set up your own validator configuration:

```toml
server_config_path = "server.json"
host = "<your-host>" # e.g. my-subdomain.my-domain.net
port = 443

[external_protocol]
Grpc = "Tls"

[internal_protocol]
Grpc = "ClearText"

[[proxies]]
host = "proxy"
public_port = 443
private_port = 20100
metrics_port = 21100

# Adjust depending on the number of shards you have
[[shards]]
host = "docker-shard-1"
port = 19100
metrics_port = 21100

[[shards]]
host = "docker-shard-2"
port = 19100
metrics_port = 21100

[[shards]]
host = "docker-shard-3"
port = 19100
metrics_port = 21100

[[shards]]
host = "docker-shard-4"
port = 19100
metrics_port = 21100

```

### Genesis Configuration

The genesis configuration describes the committee of validators and chains at
the point of network creation. It is required for validators to function.

Initially, the genesis configuration for each Testnet will be found in a public
bucket managed by the Linera Protocol core team.

An example can be found here:

```bash
wget "https://storage.googleapis.com/linera-io-dev-public/{{#include ../../TESTNET_DOMAIN}}/genesis.json"
```

### Creating Your Keys

Now that the
[validator configuration](manual-installation.md#creating-your-validator-configuration)
has been created and the
[genesis configuration](manual-installation.md#genesis-configuration) is
available, the validator private keys can be generated.

To generate the private keys, the `linera-server` binary is used:

```bash
linera-server generate --validators /path/to/validator/configuration.toml
```

This will generate a file called `server.json` with the information required for
a validator to operate, including a cryptographic keypair.

The public key will be printed after the command has finished executing, for
example:

```bash
$ linera-server generate --validators /path/to/validator/configuration.toml
2024-07-01T16:51:32.881519Z  INFO linera_server: Wrote server config server.json
02a580bbda90f0ab10f015422d450b3e873166703af05abd77d8880852a3504e4d,009b2ecc5d39645e81ff01cfe4ceeca5ec207d822762f43b35ef77b2367666a7f8
```

The public key and account key, in this case beginning with `02a` and `009`
respectively, must be communicated to the Linera Protocol core team along with
the chosen host name for onboarding in the next epoch.

> Note: Before being included in the next epoch, validator nodes will receive no
> traffic from existing users.

### Using the Linera Docker image

#### Option 1: Use Pre-built Image (Recommended)

By default, the Linera Docker images are available from the official registry.
The images are tagged based on the branch:

- `latest` for the main branch
- `{branch}_release` for release branches

The default image path is:

```text
us-docker.pkg.dev/linera-io-dev/linera-public-registry/linera:{tag}
```

You can pull the image directly:

```bash
docker pull us-docker.pkg.dev/linera-io-dev/linera-public-registry/linera:latest
```

#### Option 2: Build Locally

If you prefer to build the Linera Docker image locally, run the following
command from the root of the `linera-protocol` repository:

```bash
docker build --build-arg git_commit="$(git rev-parse --short HEAD)" -f docker/Dockerfile . -t linera
```

This can take several minutes.

When using docker-compose, set the `LINERA_IMAGE` environment variable to use
your locally built image:

```bash
export LINERA_IMAGE=linera
```

### Configuring Caddy for SSL/TLS

The validator deployment includes Caddy for automatic SSL certificate
management. Before starting the validator, ensure you have:

1. **Set environment variables**:

```bash
export DOMAIN="your-validator.example.com"
export ACME_EMAIL="admin@example.com"
```

2. **Opened required ports**:
   - Port 80 (HTTP - for ACME challenge)
   - Port 443 (HTTPS - main validator endpoint)

The `docker/Caddyfile` is pre-configured to automatically obtain Let's Encrypt
certificates and handle gRPC traffic.

### Running a Validator Node

Now that the genesis configuration is available at `docker/genesis.json` and the
server configuration is available at `docker/server.json`, the validator can be
started by running from inside the `docker` directory:

```bash
# Using the default pre-built image
cd docker && docker compose up -d

# Or, if you built the image locally
cd docker && LINERA_IMAGE=linera docker compose up -d
```

This will run the Docker Compose deployment in a detached mode, which includes:

- **Caddy**: Web server with automatic SSL (ports 80/443)
- **ScyllaDB**: High-performance database
- **Proxy**: Main validator proxy service
- **Shards**: 4 validator shard replicas
- **Prometheus & Grafana**: Monitoring stack
- **Watchtower**: Automatic container updates

It can take a few minutes for all services to initialize, especially ScyllaDB.

#### ScyllaDB Performance Optimization (Optional)

For production deployments with high I/O requirements, you can optimize ScyllaDB
performance by using a dedicated XFS partition:

1. **Create an XFS partition** (if not already available):

   ```bash
   sudo mkfs.xfs /dev/nvme1n1  # Replace with your device
   sudo mkdir -p /mnt/xfs-scylla
   sudo mount /dev/nvme1n1 /mnt/xfs-scylla
   ```

2. **Create a docker-compose.override.yml** file in the `docker` directory:

   ```yaml
   services:
     scylla:
       volumes:
         - /mnt/xfs-scylla/scylla-data:/var/lib/scylla
       environment:
         SCYLLA_AUTO_CONF: 1
         SCYLLA_DIRECT_IO_MODE: 'true'
         SCYLLA_CACHE_SIZE: '8G' # Adjust based on available RAM
   ```

3. **Set proper permissions**:
   ```bash
   sudo mkdir -p /mnt/xfs-scylla/scylla-data
   sudo chown -R 999:999 /mnt/xfs-scylla/scylla-data
   ```

Note: Standard Docker volumes work fine for most deployments. XFS optimization
is only needed for very high throughput scenarios.
