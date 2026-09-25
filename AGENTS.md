# AGENTS.md

Instructions for AI agents working on `linera-protocol`.

This file routes; it is not a second source of truth. **On any conflict, the linked file wins.**

- **Coding conventions, panics policy, cargo features, Wasm support, reviewer checklist** — [CONTRIBUTING.md](CONTRIBUTING.md). Read it before changing code.
- **Before every commit** — `cargo clippy --all-targets --all-features` and `cargo +nightly fmt`. CI enforces both; nightly is required for `fmt`.
- **After changing CLI flags** — `linera help-markdown > CLI.md`. CI fails on a stale `CLI.md`.
- **Building and running locally** — [INSTALL.md](INSTALL.md).
- **Concepts, tutorials, SDK guides** — [`docs/`](docs/), published at <https://linera.dev>.
