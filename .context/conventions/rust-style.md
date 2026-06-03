# Rust style & contribution conventions

Source of truth: `CONTRIBUTING.md` (read it for the full text). Highlights agents get wrong:

## Naming
- Types (struct/enum/trait): CamelCase. Locals: snake_case, mirroring the type name,
  shortened to the salient tail when unambiguous (`let option = GarageDoorOption::default()`).
- Abbreviations are rare and never single-letter (allowed: `database` → `db`). Single
  letters `i`, `x`, `n` only for short-lived loop integers.
- Prefer plural names for collections (`let values = vec![...]`).

## Code style
- Type annotations only when the compiler requires them (no gratuitous `Vec::<usize>::new()`).
- Re-exports limited to otherwise-private definitions.
- **Avoid `let _ = ...`** to discard values (hides unawaited futures / unhandled errors);
  `let _x =` only for RAII guards.
- Follow the Rust API guidelines where reasonable.

## Hash-consed types
- Content-addressed immutable data (`Block`, `Blob`, `ConfirmedBlockCertificate`, …) is
  passed as `linera_cache::Arc<T>` (re-exported `linera_storage::Arc`). No public
  constructor — obtain via `ValueCache::insert`. Don't build a plain `std::sync::Arc`.

## Formatting & lints
- `cargo +nightly fmt` (config in `rustfmt.toml`); CI enforces it.
- `cargo clippy --all-targets --all-features` must pass. A clippy-hardening program is in
  progress (tracker #6230; lint groups #6224–#6229) — prefer
  `#[expect(..., reason="...")]` over `#[allow(...)]`.
- Copyright header required on new files (checked by `scripts/check_copyright_header`).

## Commits & PRs
- Fork/branch from `main`; **linear history** (no merge commits).
- Commits landing on `main` must compile and contain the PR number.
- Rebasing/squashing (`git rebase -i`, force-push to your PR branch) is encouraged for a
  clean history.
- Add tests for new code; update docs for API changes.

## How agents should use this
Before opening a PR, self-check against this list and run
`cargo +nightly fmt && cargo clippy --all-targets --all-features && cargo test`.

## Freshness
- Depends on: `CONTRIBUTING.md`, `rustfmt.toml`, `clippy.toml`,
  `scripts/check_copyright_header/`. verified_commit: `cfa6e9e`; verified_at: 2026-06-03.
