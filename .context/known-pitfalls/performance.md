# Performance pitfalls

> Symptom → cause → fix. Cite a PR/issue/file for each.

## Prometheus label cardinality blowup
- **Symptom:** Prometheus memory grows unbounded; scrapes slow; OOM.
- **Cause:** A metric labeled by an unbounded value (`chain_id`, `application_id`,
  account address) creates one series per value.
- **Fix:** Never label by per-chain/per-app identifiers. Aggregate or bucket. See
  `.context/architecture/observability.md`.

## Histogram bucket clipping
- **Symptom:** A latency dashboard flatlines at the top bucket; p99 looks fake.
- **Cause:** Histogram buckets don't extend past real p99.
- **Fix:** Set bucket ceilings near observed p99 (worked example: **#6440** widened
  outbox metric ceilings).

## Reading huge worker/state files into context
- **Symptom:** Agent burns 30k+ tokens reading `linera-core/src/chain_worker/state.rs`
  (~100KB) or `worker.rs` (~77KB) top-to-bottom.
- **Cause:** No symbol-level entry point.
- **Fix:** Use `search_code`/`find_tests_for` to land on the symbol; read the span, not
  the file. (This is exactly the cost #6216 is trying to cut.)

## Cloning hash-consed types instead of sharing
- **Symptom:** Extra allocations / hashing on `Block`, `Blob`, certificates.
- **Cause:** Constructing new owned values instead of using the content-addressed cache.
- **Fix:** Pass content-addressed immutable data as `linera_cache::Arc<T>`
  (re-exported as `linera_storage::Arc`); obtain it via `ValueCache::insert`, never a new
  `Arc`. See `CONTRIBUTING.md` "Hash-consed types".

## Storage backend cost surprises
- **Symptom:** A change that's cheap on RocksDB is expensive on Scylla (or vice-versa).
- **Cause:** Different I/O/partition semantics between backends; metering layer bypassed.
- **Fix:** Test against the target backend; keep the `metering` layer in the stack; watch
  the `key_value_store_view_*_latency` metrics. See `storage.md`.

## Trusting CI benchmark numbers as absolutes
- **Symptom:** "Regression!" that's just runner noise.
- **Cause:** Criterion on shared GitHub runners is noisy; the push-triggered bench job is
  disabled (#5173).
- **Fix:** Compare against a baseline; confirm against a prod metric via `metrics_summary`.

## Freshness
- Depends on: `CONTRIBUTING.md`, `linera-base/src/prometheus_util.rs`,
  `docker/dashboards/linera-general.json`. verified_commit: `cfa6e9e`; verified_at: 2026-06-03.
