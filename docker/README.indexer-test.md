# Linera Indexer and Block Processor Testing Setup

This Docker Compose setup provides all components needed to test the Linera indexer and block processor in a containerized environment. It will start storage service, single-validator network (with a faucet), block exporter and an indexer destination.

**NOTE**: Currently, all components are packaged into the same docker image called `linera-all-test`.

## Components

The setup includes the following services in startup order:

1. **Linera Storage Service** - storage backend for validator
2. **Linera Indexer** - gGRPC-based indexer service
3. **Linera Block Exporter** - Exports blocks with metrics
4. **Linera Network** - Validator with faucet chain

## Quick Start

### 1. Build the Docker Image

```bash
cd docker
./make-all-build.sh
```

### 2. Start the Services

```bash
# in /docker directory
./make-all-up.sh
```

### 3. Verify Services are Running

- Storage Service: `curl http://localhost:1235`
- Indexer: `curl http://localhost:8081`  
- Block Exporter: `curl http://localhost:8882`
    - Metrics `curl http://localhost:9091/metrics`
- Faucet: `curl http://localhost:8080`


### 4. Stop the services
```bash
# in /docker directory
./make-all-down.sh
```

## Configuration

### Environment Variables

All ports and paths are configurable via environment variables in `.env.indexer-test`:

- `LINERA_STORAGE_SERVICE_PORT` (default: 1235) - Storage service port
- `INDEXER_PORT` (default: 8081) - Indexer gRPC port
- `INDEXER_DATABASE_PATH` (default: /data/indexer.db) - Indexer database path
- `BLOCK_EXPORTER_PORT` (default: 8882) - Block exporter port  
- `METRICS_PORT` (default: 9091) - Metrics endpoint port
- `FAUCET_PORT` (default: 8080) - Faucet service port
- `LINERA_INDEXER_IMAGE` (default: linera-all-test) - Docker image name

### Custom Configuration

To override default settings:

1. Copy `.env.indexer-test` to `.env.local`
2. Modify values in `.env.local`
3. Run with: `docker-compose -f docker-compose.indexer-test.yml --env-file .env.local up`

## Service Dependencies

The services start in the correct order with health checks:

1. Storage Service starts first
2. Indexer waits for Storage Service to be healthy
3. Block Exporter waits for Indexer to be healthy  
4. Network waits for Block Exporter to be healthy

## Data Persistence

The following volumes are created for data persistence:

- `indexer-data` - Indexer database files
    - `./indexer-data` contains an `indexer.db` file which is an SQLite db file of the indexer. It can be used for viewing the current state of the database.
- `exporter-data` - Block exporter logs and data
    - `./exporter-data` contains `linera-exporter.log` file which is a destination for "log exporter" part of the block exporter. It contains a record of all blocks and blobs processed by it.
- `network-data` - Network configuration and state

## Troubleshooting

### View Logs

```bash
# All services
docker-compose -f docker-compose.indexer-test.yml logs

# Specific service
docker-compose -f docker-compose.indexer-test.yml logs linera-indexer
```

### Check Service Health

```bash
docker-compose -f docker-compose.indexer-test.yml ps
```

### Reset Data

```bash
docker-compose -f docker-compose.indexer-test.yml down -v
```

## Development

### Rebuilding Services

After code changes, rebuild and restart:

```bash
docker build -f Dockerfile.indexer-test -t linera-all-test ..
docker-compose -f docker-compose.indexer-test.yml up --force-recreate
```

### Debugging

To access a service container:

```bash
docker-compose -f docker-compose.indexer-test.yml exec linera-indexer bash
```