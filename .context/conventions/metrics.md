# Adding a Prometheus metric

Recipe (see `.context/architecture/observability.md` for the full map):

1. **Register** using the helpers in `linera-base/src/prometheus_util.rs`
   (`register_int_counter_vec`, `register_histogram_vec`, …). Mirror an existing call.
2. **Place** the metric as a `LazyLock`/once-cell static next to related metrics in the
   owning subsystem (e.g. `linera-views/src/metrics.rs` for storage). ~310 such sites exist
   — copy the closest one.
3. **Observe/increment** at the call site.
4. **Buckets** (histograms): choose ceilings near observed p99 or dashboards clip
   (worked example #6440). 
5. **Labels**: keep cardinality bounded — **never** label by `chain_id`, `application_id`,
   or address (OOM risk).
6. **Dashboard**: add a panel to `docker/dashboards/linera-general.json` (dashboard-as-code
   is being formalized in #6243).
7. **Verify**: `curl localhost:21100/metrics | grep <name>` against the Docker stack.

## Freshness
- Depends on: `linera-base/src/prometheus_util.rs`, `linera-views/src/metrics.rs`,
  `docker/dashboards/linera-general.json`. verified_commit: `cfa6e9e`; verified_at: 2026-06-03.
