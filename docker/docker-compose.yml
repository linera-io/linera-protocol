services:
  web:
    image: caddy:2.10.2-alpine
    container_name: web
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy_data:/data
      - caddy_config:/config
    environment:
      - DOMAIN=${DOMAIN:-localhost}
    labels:
      com.centurylinklabs.watchtower.enable: "true"

  scylla-setup:
    image: ubuntu:24.04
    container_name: scylla-setup
    privileged: true
    volumes:
      - /proc:/host/proc
      - /sys:/host/sys
      - /etc/sysctl.d:/host/sysctl.d
      - ./compose-scylla-setup.sh:/setup.sh:ro
    command: ["/bin/bash", "/setup.sh", "--persist"]
    restart: "no"

  scylla:
    image: scylladb/scylla:6.2.3
    container_name: scylla
    volumes:
      - linera-scylla-data:/var/lib/scylla
    environment:
      SCYLLA_AUTO_CONF: 1
    command:
      - "--developer-mode"
      - "0"
      - "--overprovisioned"
      - "1"
    healthcheck:
      test: ["CMD-SHELL", "cqlsh -e 'describe cluster' || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 20
      start_period: 60s
    depends_on:
      scylla-setup:
        condition: service_completed_successfully

  proxy:
    image: "${LINERA_IMAGE:-us-docker.pkg.dev/linera-io-dev/linera-public-registry/linera:latest}"
    container_name: proxy
    ports:
      - "19100:19100"
    command:
      - ./linera-proxy
      - --storage
      - scylladb:tcp:scylla:9042
      - --storage-replication-factor
      - "1"
      - /config/server.json
    volumes:
      - .:/config
    labels:
      com.centurylinklabs.watchtower.enable: "true"
    healthcheck:
      test: ["CMD-SHELL", "bash -c 'exec 3<>/dev/tcp/localhost/21100' || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 10
      start_period: 30s
    depends_on:
      shard-init:
        condition: service_completed_successfully
      scylla:
        condition: service_healthy

  # Each shard must run with a unique --shard number matching the server.json configuration.
  # Docker Compose replicas don't support per-replica arguments, so we define separate services.
  shard-0:
    image: "${LINERA_IMAGE:-us-docker.pkg.dev/linera-io-dev/linera-public-registry/linera:latest}"
    container_name: docker-shard-1
    hostname: docker-shard-1
    command:
      - ./linera-server
      - run
      - --storage
      - scylladb:tcp:scylla:9042
      - --server
      - /config/server.json
      - --shard
      - "0"
      - --storage-replication-factor
      - "1"
    volumes:
      - .:/config
    labels:
      com.centurylinklabs.watchtower.enable: "true"
    depends_on:
      shard-init:
        condition: service_completed_successfully

  shard-1:
    image: "${LINERA_IMAGE:-us-docker.pkg.dev/linera-io-dev/linera-public-registry/linera:latest}"
    container_name: docker-shard-2
    hostname: docker-shard-2
    command:
      - ./linera-server
      - run
      - --storage
      - scylladb:tcp:scylla:9042
      - --server
      - /config/server.json
      - --shard
      - "1"
      - --storage-replication-factor
      - "1"
    volumes:
      - .:/config
    labels:
      com.centurylinklabs.watchtower.enable: "true"
    depends_on:
      shard-init:
        condition: service_completed_successfully

  shard-2:
    image: "${LINERA_IMAGE:-us-docker.pkg.dev/linera-io-dev/linera-public-registry/linera:latest}"
    container_name: docker-shard-3
    hostname: docker-shard-3
    command:
      - ./linera-server
      - run
      - --storage
      - scylladb:tcp:scylla:9042
      - --server
      - /config/server.json
      - --shard
      - "2"
      - --storage-replication-factor
      - "1"
    volumes:
      - .:/config
    labels:
      com.centurylinklabs.watchtower.enable: "true"
    depends_on:
      shard-init:
        condition: service_completed_successfully

  shard-3:
    image: "${LINERA_IMAGE:-us-docker.pkg.dev/linera-io-dev/linera-public-registry/linera:latest}"
    container_name: docker-shard-4
    hostname: docker-shard-4
    command:
      - ./linera-server
      - run
      - --storage
      - scylladb:tcp:scylla:9042
      - --server
      - /config/server.json
      - --shard
      - "3"
      - --storage-replication-factor
      - "1"
    volumes:
      - .:/config
    labels:
      com.centurylinklabs.watchtower.enable: "true"
    depends_on:
      shard-init:
        condition: service_completed_successfully

  shard-init:
    image: "${LINERA_IMAGE:-us-docker.pkg.dev/linera-io-dev/linera-public-registry/linera:latest}"
    container_name: shard-init
    command:
      - sh
      - -c
      - |
        while true; do
          ./linera storage check-existence --storage scylladb:tcp:scylla:9042
          status=$$?
          if [ $$status -eq 0 ]; then
            echo "Database already exists, no need to initialize."
            exit 0
          elif [ $$status -eq 1 ]; then
            echo "Database does not exist, attempting to initialize..."
            if ./linera storage initialize --storage scylladb:tcp:scylla:9042 --genesis /config/genesis.json; then
              echo "Initialization successful."
              exit 0
            else
              echo "Initialization failed, retrying in 5 seconds..."
              sleep 5
            fi
          else
            echo "An unexpected error occurred (status: $$status), retrying in 5 seconds..."
            sleep 5
          fi
        done
    volumes:
      - .:/config
    depends_on:
      scylla-setup:
        condition: service_completed_successfully
      scylla:
        condition: service_started

  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    ports:
      - "3000:3000"
    volumes:
      - grafana-storage:/var/lib/grafana
      - ./provisioning/dashboards:/etc/grafana/provisioning/dashboards
      - ./dashboards:/var/lib/grafana/dashboards

  watchtower:
    image: containrrr/watchtower:latest
    container_name: watchtower
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    command: ["--interval", "30"]

volumes:
  linera-scylla-data:
    driver: local
  grafana-storage:
  caddy_data:
    driver: local
  caddy_config:
    driver: local
