# Linera Indexer and Block Processor Testing Setup

This Docker Compose setup provides all components needed to test the Linera indexer and block processor in a containerized environment.

## Components

The setup includes the following services in startup order:

1. **Linera Storage Service** - In-memory storage backend
2. **Linera Indexer** - GRPC indexer service  
3. **Linera Block Exporter** - Exports blocks with metrics
4. **Linera Network** - Validator with faucet chain

## Quick Start

### 1. Build the Docker Image

```bash
cd docker
docker build -f Dockerfile.indexer-test -t linera-indexer-test ..
```

### 2. Start the Services

```bash
docker-compose -f docker-compose.indexer-test.yml --env-file ../.env.indexer-test up
```

### 3. Verify Services are Running

- Storage Service: `curl http://localhost:1235`
- Indexer: `curl http://localhost:8081`  
- Block Exporter: `curl http://localhost:8882`
- Faucet: `curl http://localhost:8080`
- Metrics: `curl http://localhost:9091/metrics`

## Configuration

### Environment Variables

All ports and paths are configurable via environment variables in `.env.indexer-test`:

- `LINERA_STORAGE_SERVICE_PORT` (default: 1235) - Storage service port
- `INDEXER_PORT` (default: 8081) - Indexer GRPC port
- `INDEXER_DATABASE_PATH` (default: /data/indexer.db) - Indexer database path
- `BLOCK_EXPORTER_PORT` (default: 8882) - Block exporter port  
- `METRICS_PORT` (default: 9091) - Metrics endpoint port
- `FAUCET_PORT` (default: 8080) - Faucet service port
- `LINERA_INDEXER_IMAGE` (default: linera-indexer-test) - Docker image name

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
- `exporter-data` - Block exporter logs and data
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
docker build -f Dockerfile.indexer-test -t linera-indexer-test ..
docker-compose -f docker-compose.indexer-test.yml up --force-recreate
```

### Debugging

To access a service container:

```bash
docker-compose -f docker-compose.indexer-test.yml exec linera-indexer bash
```