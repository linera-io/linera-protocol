# Requirements

This section covers requirements for setting up a Linera Validator and joining a
Testnet. The validator deployment has been comprehensively tested under
**Linux**. There is no official support for MacOS or Windows yet.

## Installing the Linera Toolchain

> When installing the Linera Toolchain, you **must** check out the
> `{{#include ../../RELEASE_BRANCH}}` branch.

To install the Linera Toolchain refer to the
[installation section](../../developers/getting_started/installation.md#installing-from-github).

You want to install the toolchain from GitHub, as you'll be using the repository
to run the Docker Compose validator service.

## Docker Compose Requirements

Linera validators run under Docker compose.

To install Docker Compose see the
[installing Docker Compose](https://docs.docker.com/compose/install/) section in
the Docker docs.

## Key Management

Currently keys in Linera are stored in a JSON file in your local filesystem. For
convenience, they are currently plaintext. The key is usually called
`server.json` and is found in the `docker/` directory in the core protocol
repository.

Make sure to back up your keys once they are generated because if they are lost,
they are currently unrecoverable.

## Infrastructure Requirements

### Automatic SSL with Caddy (Recommended)

Starting with the Conway testnet, validators now include **Caddy** as a built-in
web server that automatically handles:

1. **SSL/TLS certificates** via Let's Encrypt (ACME protocol)
2. **HTTP/2 and gRPC support** out of the box
3. **Automatic HTTPS redirection** from port 80 to 443
4. **CORS headers** for Web client support
5. **Security headers** (HSTS, X-Frame-Options, etc.)

**Required ports:**

- **Port 80**: HTTP (for ACME challenge and redirect to HTTPS)
- **Port 443**: HTTPS (main validator endpoint)

The deploy script automatically configures Caddy when you provide your email
address for Let's Encrypt certificates.

### Manual Load Balancer Configuration (Optional)

If you prefer to use your own load balancer instead of the built-in Caddy
server, ensure it has:

1. Support HTTP/2 connections
2. Support gRPC connections
3. Support long-lived HTTP/2 connections
4. Support a maximum body size of up to 20 MB
5. Provide TLS termination with a certificate signed by a known CA
6. CORS headers for Web client support
7. Redirect traffic from port 443 to port 19100 (the internal proxy port)

### Using Nginx

Minimum supported version: 1.18.0.

Below is an example Nginx configuration which upholds the infrastructure
requirements found in `/etc/nginx/sites-available/default`:

```ignore
server {
        listen 80 http2;

        location / {
                grpc_pass grpc://127.0.0.1:19100;
        }
}

server {
    listen 443 ssl http2;
    server_name <hostname>; # e.g. my-subdomain.my-domain.net

    # SSL certificates
    ssl_certificate <ssl-cert-path>; # e.g. /etc/letsencrypt/live/my-subdomain.my-domain.net/fullchain.pem
    ssl_certificate_key <ssl-key-path>; # e.g. /etc/letsencrypt/live/my-subdomain.my-domain.net/privkey.pem;

    # Proxy traffic to the service running on port 19100.
    location / {
        grpc_pass grpc://127.0.0.1:19100;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Access-Control-Allow-Origin *;
    }

    keepalive_timeout 10m 60s;
    grpc_read_timeout 10m;
    grpc_send_timeout 10m;

    client_header_timeout 10m;
    client_body_timeout 10m;
}
```

### Using External Caddy

If you're running Caddy separately (not using the built-in Docker Compose
service), minimum supported version is v2.4.3.

The built-in Caddy configuration automatically handles SSL certificates and
proxying. If you need to customize it, you can modify `docker/Caddyfile`:

```caddy
{
    email {$EMAIL}
}

{$DOMAIN:localhost} {
    # Automatic HTTPS with Let's Encrypt

    # Reverse proxy to the Linera proxy container (gRPC over HTTPS)
    reverse_proxy https://proxy:443 {
        transport http {
            versions h2c 2
            dial_timeout 60s
            response_header_timeout 60s
            tls_insecure_skip_verify
        }

        header_up Host {host}
        header_up X-Real-IP {remote}
        header_up X-Forwarded-For {remote}
        header_up X-Forwarded-Proto {scheme}
    }

    # Security headers
    header {
        Strict-Transport-Security "max-age=31536000; includeSubDomains"
        X-Frame-Options "SAMEORIGIN"
        X-Content-Type-Options "nosniff"
        X-XSS-Protection "1; mode=block"
        -Server
    }

    encode gzip
}
```

### ScyllaDB Configuration

ScyllaDB is an open-source distributed NoSQL database built for high-performance
and low-latency. Linera validators use ScyllaDB as their persistent storage.

#### Automatic Configuration

The Docker Compose setup includes ScyllaDB with automatic configuration:

- **Developer mode** enabled for simplified setup
- **Overprovisioned mode** for resource-constrained environments
- **Auto-configuration** via `SCYLLA_AUTO_CONF=1` environment variable

#### Manual Kernel Tuning (If Required)

ScyllaDB performs best with certain kernel parameters tuned. The most important
is the number of events allowed in asynchronous I/O contexts.

To check current value:

```bash
cat /proc/sys/fs/aio-max-nr
```

If the value is less than 1048576, increase it:

```bash
echo 1048576 | sudo tee /proc/sys/fs/aio-max-nr
```

To make this change persistent across reboots:

```bash
echo "fs.aio-max-nr = 1048576" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p /etc/sysctl.conf
```

> **Note**: The Kubernetes deployment automatically sets this via `sysctls` in
> the pod spec, but Docker Compose deployments may require manual configuration
> on the host.
