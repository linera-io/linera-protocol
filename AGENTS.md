# Agent guide for `linera-protocol`

Read this first. It tells coding agents how to orient in this repo cheaply and where the
shared context lives. (Tool-neutral; Claude Code also reads `CLAUDE.md`, which points here.)

## TL;DR
- This is a Rust monorepo (~30 crates) for the Linera L1 (microchains, validators,
  cross-chain messaging, RocksDB/ScyllaDB storage).
- **Don't blind-grep the whole tree to orient.** Use the curated context in `.context/`
  and (when available) the `company-context` MCP. Then read the *right* 2–3 source files.
- Curated context is a **navigation aid**, not a substitute for reading source. For exact
  current behaviour and line numbers, read the file.

## Where context lives
| Need | Go to |
|---|---|
| "How does subsystem X work / what invariants?" | `.context/architecture/*.md` (validator, proxy, shard-workers, cross-chain-messaging, scylla-storage, benchmarking, deployment, observability) |
| "What trap will bite me here?" | `.context/known-pitfalls/*.md` |
| "How do I style / test / add a metric the project way?" | `.context/conventions/*.md` |
| Repo structure | `README.md` (crate table), `.context/architecture/_index.md` |
| Live metrics/logs, PR history, prod state | `company-context` MCP (read-only) |

## Using the `company-context` MCP (when configured)
One read-only facade. Prefer high-level tools:
- `context_packet(topic)` — start here for a task; returns a token-budgeted, cited bundle.
- `search_code` (exact symbol/string) · `get_architecture_context(area)` ·
  `recent_changes(path)` · `find_tests_for(path)` · `search_prs(query)` ·
  `metrics_summary` / `logs_summary` (instead of raw PromQL/LogQL).

**When to call it vs. grep:** call `company-context` for *repo-shape / where-does-X-live /
how-does-X-work / what-changed-and-why / how-is-X-behaving* questions. Do **not** call it
for exact line numbers or current file contents — read the file for those. If a packet's
`warnings` mention staleness or drift, trust the source file over the packet.

See `.context/mcp/company-context/README.md` and the design doc `.context/DESIGN.md`.

## Before opening a PR
- Follow `.context/conventions/rust-style.md`.
- Run: `cargo +nightly fmt && cargo clippy --all-targets --all-features && cargo test`.
- Linear history, PR number in the commit, tests for new code, docs for API changes
  (see `CONTRIBUTING.md`).

## Keeping context fresh
If you change a subsystem, update the matching `.context/architecture/*.md`
**Freshness** block (or confirm the drift-detector prompt). Humans confirm; agents don't
silently rewrite the shared map. Delete fixed pitfalls — stale context is worse than none.
