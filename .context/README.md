# `.context/` — shared agent-context for `linera-protocol`

Curated, token-budgeted, **read-only** context that helps coding agents orient in this
repo without burning ~50k tokens re-exploring it every session. This is the in-tree half
of the system proposed in issue **#6216** (and widened in [`DESIGN.md`](./DESIGN.md) to the
full `company-context` MCP vision).

> Navigation aids, not source-of-truth. Packs tell you *which files to read* and *what
> invariants hold* — read the source for exact behaviour. Imprecise context is worse than
> none; if a pack's Freshness is stale, trust the code.

## Layout
```
.context/
├── README.md                 # this file
├── DESIGN.md                 # the full engineering proposal (12 sections)
├── architecture/             # subsystem packs (validator, proxy, shard-workers,
│   ├── _index.md             #   cross-chain-messaging, scylla-storage, benchmarking,
│   └── *.md                  #   deployment-gcp-k8s, observability) + pack contract
├── known-pitfalls/           # performance / storage / async-concurrency / cross-chain
├── conventions/              # rust-style / metrics / testing (distilled from CONTRIBUTING)
├── eval/                     # the ship-gate: 10 tasks, ON vs OFF, ≥20% token cut
└── mcp/company-context/      # the MCP facade: README + tool & packet JSON schemas + example
```

## Start here
- **Agents:** read [`/AGENTS.md`](../AGENTS.md) (and `/CLAUDE.md`). Use
  `get_architecture_context(area)` / `context_packet(topic)` when the `company-context` MCP
  is configured; otherwise read the relevant pack here directly.
- **Maintainers:** read [`DESIGN.md`](./DESIGN.md). Keep packs' **Freshness** blocks honest;
  the PR-time drift detector relies on them.

## Naming note
#6216 proposes `.kb/`; this scaffold uses `.context/`. They're the same thing — the team
should pick one (a rename). Structure is stable either way.
