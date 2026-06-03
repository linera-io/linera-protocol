# Testing conventions

Source of truth: `CONTRIBUTING.md`, `docs/developers/backend/testing.md`, CI under
`.github/workflows/`.

## Where tests live
- Unit tests: `<crate>/src/unit_tests/*.rs` (e.g. `linera-chain/src/unit_tests/inbox_tests.rs`,
  `linera-core/src/unit_tests/worker_tests.rs`).
- Integration/e2e: `linera-service` (network spin-up), `remote-net-test.yml`,
  `docker-compose.yml` CI.
- Benches: `<crate>/benches/*.rs` (`harness = false`). See `architecture/benchmarking.md`.
- Proptests planned for chain-worker / views / cache (#6109).

## Running
- `cargo test` for the workspace; scope with `-p <crate> <test_filter>`.
- Wasm examples need extra flags — see the Wasm section in `CONTRIBUTING.md`.
- Required before PR: `cargo +nightly fmt && cargo clippy --all-targets --all-features && cargo test`.
- Test-runner migration to `cargo-nextest` is proposed (#5983) — check before assuming the
  runner.

## How agents should use this
Use `find_tests_for(target=<path|symbol>)` to get the exact test + a copy-pasteable
`run_cmd` instead of grepping. Add tests for new behavior in the matching `unit_tests/`
module.

## Freshness
- Depends on: `CONTRIBUTING.md`, `docs/developers/backend/testing.md`,
  `.github/workflows/rust.yml`. verified_commit: `cfa6e9e`; verified_at: 2026-06-03.
