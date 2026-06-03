# `company-context` MCP

> One logical, **read-only** MCP facade that coding agents call for code knowledge,
> architecture, GitHub history, tests, metrics/logs, and read-only production state —
> instead of blindly exploring the repo or hand-writing PromQL/`kubectl`.

This is the umbrella interface described in the design doc at
[`.context/DESIGN.md`](../../DESIGN.md). It **subsumes and extends** the `linera-kb`
proposal in issue **#6216**: `linera-kb` becomes the *code-knowledge backend* of this
facade; `company-context` adds GitHub-metadata, Grafana, and read-only prod-state
backends behind the same door.

## Design principles (non-negotiable)

1. **Read-only by default.** No tool mutates anything. Write-capable actions live in a
   *separate, future* MCP gated by human approval — never here.
2. **Facade over a giant server.** One interface, many small backends, each with its own
   **scoped, short-lived** token. No single process holds all credentials.
3. **Precision over volume.** Imprecise retrieval is worse than none (#6216). Every
   response is token-budgeted and every claim is cited. Truncation is explicit.
4. **OSS-first, cash-conscious.** ripgrep + `indxr` + git + the GitHub API + Grafana MCP.
   No vector DB or knowledge graph in v0.x until the eval harness proves they're needed.
5. **Wrap, don't expose.** Agents get `metrics_summary` / `logs_summary`, not raw
   PromQL/LogQL or `kubectl`.

## Tool surface (8 tools — MVP)

| Tool | What it answers | Backend(s) |
|---|---|---|
| `context_packet` | "Give me everything I need for task X" (entry point) | fan-out + dedup |
| `search_code` | "Where is this symbol/string/error?" | ripgrep + `indxr` |
| `get_architecture_context` | "How does subsystem X work / what invariants?" | `.context/architecture` |
| `recent_changes` | "What changed in this path lately, and why?" | GitHub + git log |
| `find_tests_for` | "Which tests cover this path? How do I run them?" | ripgrep + cargo metadata |
| `search_prs` | "Find the PR/issue where X was decided" | GitHub metadata |
| `metrics_summary` | "How is metric/area X behaving vs baseline?" | Grafana MCP (read-only) |
| `logs_summary` | "What errors is service X emitting?" | Grafana Loki (read-only) |

Full schemas: [`tools.schema.json`](./tools.schema.json).
Output format for `context_packet`: [`context-packet.schema.json`](./context-packet.schema.json),
example: [`examples/context_packet.add-metric.json`](./examples/context_packet.add-metric.json).

## Architecture (facade)

```
                    ┌─────────────────────────────────────────────┐
   coding agent ───▶│  company-context gateway (stateless, MCP)   │
   (OIDC subject)   │  • identity + scope check                   │
                    │  • per-call token budget                    │
                    │  • audit log  • prompt-injection sanitizer  │
                    │  • secret/PII redactor on egress            │
                    └───┬───────┬───────┬───────┬─────────┬────────┘
                        │       │       │       │         │
            scoped,short-lived tokens (one per backend, least privilege)
                        │       │       │       │         │
                ┌───────▼┐ ┌────▼────┐ ┌▼──────┐ ┌▼──────┐ ┌▼───────────┐
                │linera- │ │ GitHub  │ │Grafana│ │ripgrep│ │ prod-state │
                │kb (.ctx│ │ metadata│ │  MCP  │ │ + git │ │ read-only  │
                │ripgrep)│ │ (RO)    │ │ (RO)  │ │       │ │(k8s/helm RO)│
                └────────┘ └─────────┘ └───────┘ └───────┘ └────────────┘
```

The gateway is the only component agents talk to. Backends never receive agent text
directly except as sanitized, templated queries.

## What this is NOT

- Not a write path (no deploys, no `kubectl apply`, no PR merges, no edits).
- Not a credential vault — it holds no long-lived secrets; it mints/uses scoped tokens.
- Not a vector search engine (deferred; see DESIGN.md §5).
- Not a replacement for `indxr`/Serena or for reading source — it's a **navigation aid**.

## Status

Design + schemas only (this directory). No server implementation has landed yet.
See the phased plan in [`.context/DESIGN.md`](../../DESIGN.md) §10 and the ship-gate
in [`.context/eval/`](../../eval/).
