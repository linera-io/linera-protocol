# Shard workers

## Purpose
The worker is where blocks are actually validated and executed. Each server runs a pool
of shards; each shard drives `ChainWorkerState` instances (one logical state machine per
chain). Read this for block proposal/certificate handling, execution flow, and the
chain-worker channel/concurrency model.

## Entry points
- `linera-core/src/worker.rs` — `WorkerState<S>`, top-level request handling (large file).
- `linera-core/src/chain_worker/mod.rs` — module root.
- `linera-core/src/chain_worker/state.rs` — `ChainWorkerState<S>`: block execution,
  proposals, certificates (the heart; large file).
- `linera-core/src/chain_worker/handle.rs` — `ChainWorkerHandle`, the message/actor seam.
- `linera-core/src/chain_worker/config.rs` — `ChainWorkerConfig`.
- `linera-core/src/chain_worker/delivery_notifier.rs` — delivery notification.
- `docs/developers/advanced_topics/block_creation.md` — canonical block-flow doc.

## How it works
Requests reach `WorkerState`, which dispatches to the owning `ChainWorkerState` via a
handle/channel (actor-style). The chain worker executes blocks (calling into
`linera-execution`), produces/validates certificates, updates inbox/outbox state
(`linera-chain`), and persists via storage. Cross-chain effects are emitted as outbox
entries and later delivered to peer workers (see `cross-chain-messaging.md`).

## Invariants & gotchas
- One chain is processed by one chain-worker at a time — per-chain serialization is the
  concurrency model. Don't assume cross-chain ordering beyond what inbox cursors give.
- The chain-worker channel/actor code is concurrency-sensitive; it's an explicit target
  for cancellation-safety proptests and loom/MIRI pilots (issues #6109, #6112). Be careful
  with `await` points and cancellation — see `../known-pitfalls/async-concurrency.md`.
- `worker.rs` and `chain_worker/state.rs` are very large; use `search_code`/`find_tests_for`
  to land on the right symbol instead of reading top-to-bottom.

## Related
- Packs: `validator.md`, `cross-chain-messaging.md`, `scylla-storage.md`.
- Pitfalls: `../known-pitfalls/async-concurrency.md`.
- Tests: `linera-core/src/unit_tests/worker_tests.rs`,
  `linera-core/src/unit_tests/worker_backup_tests.rs`.

## How agents should use this
`find_tests_for(target="ChainWorkerState")` to find behavior anchors, then read the named
symbol. Avoid loading the whole 100KB+ files into context.

## Freshness
- Depends on: `linera-core/src/worker.rs`, `linera-core/src/chain_worker/*.rs`.
- verified_commit: `cfa6e9e`
- verified_at: 2026-06-03
