## One-Click Deploy

After downloading the `linera-protocol` repository and checking out the testnet
branch `{{#include ../../RELEASE_BRANCH}}`, you can run
`scripts/deploy-validator.sh <hostname> <email>` to deploy a Linera validator.

For example:

```bash
$ git fetch origin
$ git checkout -t origin/{{#include ../../RELEASE_BRANCH}}
$ scripts/deploy-validator.sh linera.mydomain.com admin@mydomain.com
```

The email address is required for ACME/Let's Encrypt SSL certificate generation.

### What the Deploy Script Does

The one-click deploy script automatically:

1. **Configures Caddy** for automatic SSL/TLS certificates via Let's Encrypt
2. **Opens only ports 80 and 443** - no manual load balancer needed!
3. **Sets up ScyllaDB** with automatic configuration and developer mode
4. **Configures monitoring** with Prometheus and Grafana
5. **Enables automatic updates** via Watchtower (checks every 30 seconds)
6. **Generates validator keys** and configuration files
7. **Downloads genesis configuration** from the testnet bucket

The deployment automatically listens for new image updates and will pull them
automatically.

### Deploy Script Options

The deploy script accepts the following arguments:

- `<hostname>` (required): The domain name for your validator
- `<email>` (required): Email address for ACME/Let's Encrypt certificates

And the following optional flags:

- `--local-build`: Build Docker image locally instead of using registry image
- `--remote-image`: Explicitly use remote Docker image from registry
  (deprecated, now default)
- `--skip-genesis`: Skip downloading the genesis configuration (use existing)
- `--force-genesis`: Force re-download of genesis configuration even if it
  exists
- `--custom-tag TAG`: Use a custom Docker image tag for testing (no \_release
  suffix)
- `--xfs-path PATH`: Optional XFS partition path for optimal ScyllaDB
  performance
- `--cache-size SIZE`: Optional ScyllaDB cache size (default: 4G, e.g. 2G, 8G,
  16G)
- `--dry-run`: Preview what would be done without executing
- `--verbose` or `-v`: Enable verbose output
- `--help` or `-h`: Show help message

> Note: By default, the script uses the pre-built Docker image from the official
> registry. Use `--local-build` if you want to build from source.

### Environment Variables

You can customize the deployment further using environment variables:

- `ACME_EMAIL`: Override the email for Let's Encrypt certificates
- `LINERA_IMAGE`: Use a custom Docker image (overrides all image settings)
- `DOCKER_REGISTRY`: Override the Docker registry (default:
  us-docker.pkg.dev/linera-io-dev/linera-public-registry)
- `IMAGE_NAME`: Override the image name (default: linera)
- `IMAGE_TAG`: Override the image tag (default: `<branch>_release`)
- `GENESIS_URL`: Override the genesis configuration URL
- `GENESIS_BUCKET`: GCP bucket for genesis files
- `GENESIS_PATH_PREFIX`: Path prefix in bucket (default: uses branch name)
- `NUM_SHARDS`: Number of validator shards (default: 4)
- `PORT`: Internal validator port (default: 19100)
- `METRICS_PORT`: Metrics collection port (default: 21100)
- `SCYLLA_XFS_PATH`: Optional XFS partition mount path for ScyllaDB data
- `SCYLLA_CACHE_SIZE`: ScyllaDB cache size (default: 4G)

For example:

```bash
# Deploy with more shards
NUM_SHARDS=8 scripts/deploy-validator.sh validator.example.com admin@example.com

# Deploy with XFS partition for optimal ScyllaDB performance
scripts/deploy-validator.sh validator.example.com admin@example.com \
  --xfs-path /mnt/xfs-scylla --cache-size 8G

# Deploy with custom image tag (for testing)
scripts/deploy-validator.sh validator.example.com admin@example.com \
  --custom-tag devnet_2025_08_21

# Build Docker image locally instead of using registry
scripts/deploy-validator.sh validator.example.com admin@example.com --local-build
```

The public key and account key will be printed after the command has finished
executing, for example:

```bash
$ scripts/deploy-validator.sh linera.mydomain.com admin@mydomain.com
...
Public Key: 02a580bbda90f0ab10f015422d450b3e873166703af05abd77d8880852a3504e4d,009b2ecc5d39645e81ff01cfe4ceeca5ec207d822762f43b35ef77b2367666a7f8
```

The public key and account key, in this case beginning with `02a` and `009`
respectively, must be communicated to the Linera Protocol core team along with
the chosen host name for onboarding in the next epoch.

For a more bespoke deployment, refer to the manual installation instructions
below.

> **Note**: If you have previously deployed a validator you may need to remove
> old docker volumes (`docker_linera-scylla-data` and `docker_linera-shared`).

### System Requirements

Before running the deploy script, ensure your system meets these requirements:

1. **Ports**: Ensure ports 80 and 443 are open and not in use
2. **Domain**: Your domain must point to this server's IP address
3. **Kernel tuning**: The deploy script will automatically configure AIO
   settings via the scylla-setup container. If automatic configuration fails,
   you may need to manually run:
   ```bash
   echo 1048576 | sudo tee /proc/sys/fs/aio-max-nr
   echo "fs.aio-max-nr = 1048576" | sudo tee -a /etc/sysctl.conf
   sudo sysctl -p
   ```
4. **XFS partition** (optional, for maximum performance): For production
   deployments with high I/O requirements, consider using a dedicated XFS
   partition for ScyllaDB:
   ```bash
   # Example: Create and mount XFS partition
   sudo mkfs.xfs /dev/nvme1n1  # Replace with your device
   sudo mkdir -p /mnt/xfs-scylla
   sudo mount /dev/nvme1n1 /mnt/xfs-scylla
   # Then use: --xfs-path /mnt/xfs-scylla
   ```
   Note: Standard Docker volumes work fine for most deployments
