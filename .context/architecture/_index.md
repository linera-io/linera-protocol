# Architecture packs

Curated, token-budgeted **navigation aids** for coding agents working on
`linera-protocol`. Each pack tells an agent *which files to read* and *what invariants
hold* — it does **not** paraphrase source (that loses to agentic search and rots fast;
see issue #6216). Concept pages that drift toward "explain what `fn` does" should be
rejected in review.

## Pack contract

Every `*.md` in this directory MUST have these sections, in this order:

1. **Purpose** — one paragraph: what this subsystem is and when an agent needs this pack.
2. **Entry points** — the 3–6 files/symbols to read first (repo-relative paths).
3. **How it works** — short, pointer-heavy. Invariants and data flow, not line-by-line.
4. **Invariants & gotchas** — the things that aren't obvious from reading one file.
5. **Related** — neighboring packs, canonical docs (linera.dev), key PRs/issues.
6. **How agents should use this** — explicit retrieval recipe.
7. **Freshness** — source files this pack depends on (for the PR-time drift detector),
   `verified_commit`, and `verified_at`.

Target size: **150–400 tokens of content** per pack. If a pack grows past ~1 screen,
split it.

## Packs

| Pack | Subsystem | Primary crate(s) |
|---|---|---|
| [`validator.md`](./validator.md) | Validator as a whole (server + shards) | `linera-service`, `linera-core` |
| [`proxy.md`](./proxy.md) | Public-facing gRPC proxy | `linera-service/src/proxy` |
| [`shard-workers.md`](./shard-workers.md) | Sharded chain workers | `linera-core` |
| [`cross-chain-messaging.md`](./cross-chain-messaging.md) | Inbox/outbox, message bundles | `linera-chain`, `linera-rpc` |
| [`scylla-storage.md`](./scylla-storage.md) | Storage backends & views | `linera-views`, `linera-storage` |
| [`benchmarking.md`](./benchmarking.md) | Benchmark tooling | `linera-service`, `linera-core` |
| [`deployment-gcp-k8s.md`](./deployment-gcp-k8s.md) | Build/deploy topology | `docker/`, external infra |
| [`observability.md`](./observability.md) | Metrics/logs/traces | `linera-metrics`, `linera-base` |

Performance gotchas live in [`../known-pitfalls/performance.md`](../known-pitfalls/performance.md).

## Drift detection

Each pack's **Freshness** block lists the source paths it depends on. The PR-time drift
detector (DESIGN.md §9, §10 Phase 2) flags a pack when any listed path is touched and
asks the PR author to confirm/update. Humans confirm; agents do not silently rewrite the
shared map.
