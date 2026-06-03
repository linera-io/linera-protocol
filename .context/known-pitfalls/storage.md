# Storage pitfalls

## RocksDB single-process lock
- **Symptom:** Second wallet/process fails to open the DB; "lock held" errors.
- **Cause:** RocksDB locks its directory to one process.
- **Fix:** One process per RocksDB path; for multi-wallet dev use separate paths or the
  storage-service. See linera-agents `rocksdb-locking.md` and `multi-wallet-setup.md`.

## Hand-rolling key bytes / RootKey partition keys
- **Symptom:** Corrupt reads, partition mismatches on Scylla.
- **Cause:** RootKey/partition-key encoding is non-trivial; bypassing the view layer.
- **Fix:** Go through view types and `store.rs`; don't construct raw keys. Decoding tooling
  is in flight (#6298).

## "Successful" writes that actually errored
- **Symptom:** Write reported OK but state diverges later.
- **Cause:** Error-throwing-yet-successful DB writes (open hardening area, #6052).
- **Fix:** Treat partial-failure on writes as real; check `#6110` integrity-hardening work
  before assuming write atomicity.

## Backend selected at compile time
- **Symptom:** Behaviour differs between local (RocksDB) and prod (Scylla); a test passes
  locally, fails in prod.
- **Cause:** Backend chosen via Cargo features (`rocksdb`/`scylladb`/`storage-service`).
- **Fix:** Build/test with the feature matching the target; don't assume parity.

## Freshness
- Depends on: `linera-views/src/backends/*.rs`, `linera-views/src/store.rs`.
  verified_commit: `cfa6e9e`; verified_at: 2026-06-03.
