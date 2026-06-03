# Async / concurrency pitfalls

## Cancellation safety in the chain-worker channel
- **Symptom:** Dropped/stuck work or inconsistent state when a future is cancelled mid-flight.
- **Cause:** `await` points in the chain-worker actor/channel code that aren't
  cancellation-safe.
- **Fix:** Treat `chain_worker/{handle,state}.rs` and `linera-cache` channel code as
  cancellation-sensitive. Proptests / loom / MIRI pilots are explicitly planned (#6109,
  #6112) — mirror their assumptions.

## `let _ = ...` hiding unawaited futures / errors
- **Symptom:** Silent dropped errors or never-awaited futures.
- **Cause:** `let _ = expr;` discards values, masking unhandled `Result`/`Future`.
- **Fix:** Per `CONTRIBUTING.md`, avoid `let _ =`; use `let _x =` only for RAII guards.

## Per-chain serialization assumptions
- **Symptom:** Races assumed impossible actually happen across chains.
- **Cause:** Assuming global ordering; only **per-chain** processing is serialized.
- **Fix:** Reason per-chain; cross-chain ordering comes only from inbox cursors
  (see `cross-chain.md`).

## Freshness
- Depends on: `linera-core/src/chain_worker/*.rs`, `linera-cache/`, `CONTRIBUTING.md`.
  verified_commit: `cfa6e9e`; verified_at: 2026-06-03.
