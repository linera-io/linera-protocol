# Observability (metrics / logs / traces)

## Purpose
How the protocol emits and exposes telemetry. Read this for adding/finding metrics,
understanding the Prometheus/Grafana/Alloy stack, or mapping a metric back to its source.

## Entry points
- `linera-base/src/prometheus_util.rs` — registration helpers
  (`register_int_counter_vec`, `register_histogram_vec`, …). **Mirror these to add a metric.**
- `linera-metrics/src/monitoring_server.rs` — `MonitoringServer`, serves `/metrics`.
- `linera-metrics/src/runtime_metrics.rs` — Tokio runtime metrics
  (`linera_tokio_worker_*`).
- `linera-metrics/src/memory_profiler.rs` — jemalloc profiling.
- `linera-views/src/metrics.rs` — example per-subsystem metric statics
  (`LOAD_VIEW_LATENCY`, `SAVE_VIEW_LATENCY`).
- `linera-exporter/src/main.rs` — `linera-exporter` scrape/forward binary.
- Stack config: `docker/prometheus.yml`, `docker/alloy-config.river` (Alloy → OTel),
  `docker/dashboards/linera-general.json`, `docker/provisioning/dashboards/`,
  `docker/docker-compose.yml` (proxy, shard-0..3, scylla, prometheus, grafana).

## How it works
~310 `register_*` call sites define metrics, namespaced and registered via
`prometheus_util`. Each process exposes `/metrics` on `:21100`. In the Docker stack,
**Alloy** scrapes proxy + shards and forwards metrics/logs/traces over OTel; Prometheus
also scrapes directly; Grafana renders `linera-general.json`. Tracing is available via
the `linera-rpc` opentelemetry feature.

## Invariants & gotchas
- **Bucket ceilings**: new histograms need buckets near observed p99 or dashboards clip
  at the top bucket (worked example: #6440 widened outbox ceilings).
- **Label cardinality**: never label by `chain_id`/`application_id`/address — unbounded
  cardinality OOMs Prometheus. See `../known-pitfalls/performance.md`.
- Two scrape paths exist (Prometheus direct + Alloy/OTel); keep job names consistent
  (`proxy`/`linera-proxy`, `shards`/`linera-shard`).
- Dashboard-as-code / recording rules / log-pattern diffing are being formalized (#6243).

## Related
- Packs: `proxy.md`, `shard-workers.md`, `scylla-storage.md`, `deployment-gcp-k8s.md`.
- `company-context` exposes `metrics_summary` / `logs_summary` — agents call those,
  **not** raw PromQL/LogQL (see `.context/mcp/company-context/`).

## How agents should use this
To add a metric: read `prometheus_util.rs` + the nearest existing metric static, copy the
pattern, then add a dashboard panel. To inspect live behaviour: `metrics_summary(metric=…)`
or `logs_summary(service=…)`. To map a metric → code, `search_code(query="<metric_name>")`.

## Freshness
- Depends on: `linera-base/src/prometheus_util.rs`, `linera-metrics/src/*.rs`,
  `linera-views/src/metrics.rs`, `docker/prometheus.yml`, `docker/alloy-config.river`,
  `docker/dashboards/linera-general.json`.
- verified_commit: `cfa6e9e`
- verified_at: 2026-06-03
