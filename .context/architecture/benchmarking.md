# Benchmarking

## Purpose
Two layers of benchmarking: (1) a synthetic-load CLI (`linera-benchmark`) that drives a
running network, and (2) Criterion micro-benchmarks (`cargo bench`) for hot paths. Read
this for throughput/latency experiments, regression tracking, or adding a benchmark.

## Entry points
- `linera-service/src/benchmark.rs` — `linera-benchmark` binary (`BenchmarkCommand`),
  synthetic transaction generator.
- `linera-client/src/benchmark.rs` — client-side benchmark driver library.
- `linera-service/benches/transfers.rs` — end-to-end transfer Criterion bench.
- `linera-views/benches/{stores,queue_view,reentrant_collection_view}.rs` — storage benches.
- `linera-core/benches/{client_benchmarks,hashing_benchmarks}.rs` — core benches.
- `linera-storage-service/benches/store.rs` — remote-store bench.
- CI: `.github/workflows/benchmarks.yml` (currently `workflow_dispatch` only —
  push trigger disabled, TODO #5173), `benchmark-test.yml`, `post-benchmark-comment.yml`,
  `performance_summary.yml`.
- Scripts: `scripts/faucet-stress-test.bash`, `scripts/check_chain_loads.sh`,
  `scripts/cpu-profile.sh`.

## How it works
Criterion benches use `harness = false` and live under each crate's `benches/`. The
`linera-benchmark` binary spins synthetic load against a network for macro throughput.
CI can post benchmark comments on PRs; the regular push-triggered run is disabled pending
#5173.

## Invariants & gotchas
- The push-triggered benchmark workflow is **disabled** (#5173) — don't assume benches run
  automatically on every PR; trigger manually via `workflow_dispatch`.
- Criterion results are noisy on shared CI runners; compare against a baseline, not an
  absolute number.
- A/B performance testing infra (traffic replay, snapshot/restore harness) is being built
  out separately — see the `[A/B]` tracker issues (#6243–#6248).

## Related
- Packs: `scylla-storage.md`, `observability.md`.
- Pitfalls: `../known-pitfalls/performance.md`.

## How agents should use this
To add a bench, copy the closest `benches/*.rs` and register `[[bench]]` in that crate's
`Cargo.toml`. For "did this regress", pair the bench with `metrics_summary` on the
relevant prod metric rather than trusting CI numbers alone.

## Freshness
- Depends on: `linera-service/src/benchmark.rs`, `linera-client/src/benchmark.rs`,
  `**/benches/*.rs`, `.github/workflows/benchmarks.yml`.
- verified_commit: `cfa6e9e`
- verified_at: 2026-06-03
