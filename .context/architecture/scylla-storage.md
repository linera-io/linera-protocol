# Storage: views, ScyllaDB & RocksDB

## Purpose
Persistent state is modeled with **views** (an ORM-like layer over a key-value store) and
backed by a pluggable KV backend: **RocksDB** (embedded, dev/test) or **ScyllaDB**
(distributed, prod). Read this for storage backends, view types, key layout, caching,
and storage-cost/metering questions.

## Entry points
- `linera-views/src/backends/scylla_db.rs` — `ScyllaDbStore` (CQL/Cassandra driver).
- `linera-views/src/backends/rocks_db.rs` — `RocksDbStore` (embedded).
- `linera-views/src/backends/{metering,lru_caching,dual}.rs` — composable layers
  (cost metering, LRU cache, dual read/shadow — see #6111).
- `linera-views/src/store.rs` — store trait surface.
- `linera-views/src/views/key_value_store_view.rs` — `KeyValueStoreView`.
- `linera-storage/src/db_storage.rs` — `DbStorage`, the high-level polymorphic API.
- `linera-storage-service/src/{server,client}.rs` — optional remote storage over gRPC.
- Docs: linera.dev → Views guide; `docs.rs/linera-views`.

## How it works
Application/chain state is declared with view types (`MapView`, `CollectionView`,
`QueueView`, …) that serialize onto KV pairs. `DbStorage` wraps a chosen backend; backends
compose: e.g. `metering(lru_caching(scylla_db))`. Backend is selected at compile time via
Cargo features (`rocksdb`, `scylladb`, `storage-service`).

## Invariants & gotchas
- **Backend behaviour differs.** RocksDB is single-process and **locks** the DB file
  (multi-wallet/multi-process setups break — see linera-agents `rocksdb-locking.md`).
  Scylla is distributed and has its own consistency/partition semantics.
- **Key layout matters.** RootKey/partition-key encoding is non-trivial; there's active
  work on Scylla UDFs to decode RootKey partition keys (#6298). Don't hand-roll key bytes.
- **Metering layer** tracks I/O cost; performance-sensitive changes should preserve it.
- Integrity/validation cadence and "error-throwing successful writes" are open hardening
  areas (#6110, #6052).

## Related
- Packs: `validator.md`, `benchmarking.md`, `observability.md`.
- Metrics: `key_value_store_view_*_latency` family, `load_view_latency`,
  `save_view_latency`, `blob_count`.
- Pitfalls: `../known-pitfalls/storage.md`, `../known-pitfalls/performance.md`.

## How agents should use this
Decide backend first (feature flags), then read the matching `backends/*.rs`. Use
`search_code(query="ScyllaDbStore")` / `find_tests_for` rather than reading the 50KB
scylla file end-to-end. For prod storage latency, `metrics_summary(area="storage")`.

## Freshness
- Depends on: `linera-views/src/backends/{scylla_db,rocks_db,metering,lru_caching,dual}.rs`,
  `linera-views/src/store.rs`, `linera-storage/src/db_storage.rs`.
- verified_commit: `cfa6e9e`
- verified_at: 2026-06-03
