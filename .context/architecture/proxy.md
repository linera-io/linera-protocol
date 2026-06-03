# Proxy (validator front-end)

## Purpose
`linera-proxy` is the validator's public-facing gRPC reverse proxy. It terminates client
connections and routes each request to the `linera-server` shard that owns the target
chain. Read this when a task touches request routing, public endpoints, gRPC plumbing,
or proxy-level metrics/timeouts.

## Entry points
- `linera-service/src/proxy/main.rs` — `ProxyOptions` (CLI/config, network timeouts).
- `linera-service/src/proxy/grpc.rs` — `GrpcProxy`, the routing/forwarding handler.
- `linera-rpc/src/config.rs` — `ValidatorInternalNetworkPreConfig` /
  `ValidatorPublicNetworkPreConfig`, `ShardConfig`, `ShardId` (topology).
- `docker/prometheus.yml` / `docker/alloy-config.river` — proxy is scraped as job
  `proxy` / `linera-proxy` on `:21100`.

## How it works
The proxy holds the shard map (which shard owns which chain) and forwards binary RPC /
GraphQL to the right server. It is **stateless** w.r.t. consensus: it does not execute
blocks. Latency is tracked via the `linera_proxy_request_latency` metric.

## Invariants & gotchas
- Keep the proxy **thin**: routing, fan-in/fan-out, timeouts, backpressure. Business and
  consensus logic belongs in the worker.
- Shard routing depends on the topology config; a mismatch between proxy and server shard
  config silently misroutes. Validate topology changes against both.
- Public surface — treat all inbound data as untrusted; this is the DoS-exposed edge.

## Related
- Packs: `validator.md`, `shard-workers.md`, `observability.md`.
- Metrics: `linera_proxy_request_latency`.
- A/B testing seam: the gRPC proxy boundary is the chosen ingress recorder point
  (issue #6246).

## How agents should use this
`get_architecture_context(area="proxy")`, then read `proxy/grpc.rs`. For "how is proxy
latency right now", use `metrics_summary(area="proxy")`, not raw PromQL.

## Freshness
- Depends on: `linera-service/src/proxy/main.rs`, `linera-service/src/proxy/grpc.rs`,
  `linera-rpc/src/config.rs`.
- verified_commit: `cfa6e9e`
- verified_at: 2026-06-03
