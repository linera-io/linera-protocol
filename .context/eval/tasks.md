# Evaluation tasks (10)

Representative engineering tasks where better context should help. Half "internal
codebase", some "design history", some operational. Mirrors and extends #6216's list.
Each row maps to a `company-context` call that *should* short-circuit blind exploration.

| # | Task | Expected context call | Source of truth | Likely OFF failure mode |
|---|---|---|---|---|
| 1 | Add a validator-side Prometheus metric for outbox size | `context_packet("add validator metric")` | `prometheus_util.rs`, `linera-views/src/metrics.rs`, #6440 | Greps 10 files for "metric"; picks wrong registration site; misses bucket/cardinality rules |
| 2 | Add a GraphQL endpoint to `linera-service` returning current epoch | `context_packet` + `search_code("async-graphql")` | `linera-service`, node_service docs | Re-discovers GraphQL wiring from scratch |
| 3 | Trace what happens when chain A receives a message from chain B | `get_architecture_context("cross-chain-messaging")` | `inbox.rs`/`outbox.rs`, `cross_chain_message_queue.rs` | Reads 100KB `chain_worker/state.rs` top-to-bottom |
| 4 | Explain block proposal → certificate flow end-to-end | `get_architecture_context("shard-workers")` | `worker.rs`, `chain_worker/state.rs`, block_creation.md | Burns tokens orienting in `worker.rs` |
| 5 | Difference between RocksDB and Scylla behaviour for a write path | `get_architecture_context("scylla-storage")` + `storage.md` pitfalls | `backends/{rocks_db,scylla_db}.rs` | Assumes backend parity; misses compile-time feature selection |
| 6 | Why does `linera-proxy` request latency spike on testnet? | `metrics_summary(area="proxy")` + `logs_summary(service="proxy")` | Grafana, `proxy/grpc.rs` | Hand-writes PromQL / can't reach metrics at all |
| 7 | Find the PR that widened outbox metric ceilings and why | `search_prs("outbox metric ceiling")` | #6440 | Can't search PRs; guesses |
| 8 | Which tests cover the inbox cursor logic, and how do I run them? | `find_tests_for("linera-chain/src/inbox.rs")` | `unit_tests/inbox_tests.rs` | Greps for `#[test]`, misses the run command |
| 9 | What's deployed on testnet right now (image versions / replicas)? | prod-state read (deploy section of `context_packet`) | `linera-artifacts`, live cluster | Looks for k8s manifests in this repo (they're not here) |
| 10 | Add a new field to a serialized type and propagate it everywhere | `search_code(symbol)` + `recent_changes(path)` | `linera-base`, BCS schema | Misses serialization/schema sites; partial propagation |

## Baseline to beat
~50k tokens of unguided orientation per fresh session (figure from #6216). Target: cut the
orientation portion materially while keeping correctness.
