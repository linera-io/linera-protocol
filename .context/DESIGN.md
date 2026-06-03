# Agent-context system for `linera-protocol` — engineering proposal

> Status: design proposal. Author: agent-context working group. Companion to issues
> **#6216** (agent-shaped KB) and **#6179** (agent-friendly public docs).
> Cash-conscious, OSS-first, read-only-by-default.

---

## 0. Executive summary

Fresh coding agents currently burn ~50k tokens orienting in this repo every session
(per #6216): walking 30 crates, re-reading READMEs, grepping for symbols, re-deriving
conventions, and re-discovering operational gotchas that live in nobody's docs. The cost
is paid every session, by every agent, and the result is still incomplete because the
highest-value context — cross-crate wiring, design rationale, prod/operational state — is
not in code or in public docs.

**Proposal:** build one logical, **read-only** MCP facade — `company-context` — that
composes small, scoped, OSS backends behind a single door, plus an in-repo curated
context layer (`.context/`). This is **not** a giant credentialed write-capable server.
It is a thin gateway over: curated markdown (`.context/` ≈ the `linera-kb` of #6216),
ripgrep + `indxr`, GitHub metadata, **Grafana MCP** (wrapped, not exposed), and a
**read-only** Kubernetes/Helm prod-state reader.

**Posture (opinionated):**
- **Build first:** `.context/` packs + the code/docs/GitHub MCP (Phases 0–2). Highest
  value, lowest risk, no production blast radius.
- **Then:** wrap Grafana (Phase 3).
- **Then, carefully:** read-only prod-state (Phase 4).
- **Don't build (now):** vector DB, knowledge graph, any write path, a single
  all-credentials server.
- **Ship-gate everything** against the #6216 eval harness: ≥20% session-token reduction on
  ≥6/10 tasks, no correctness regression. If we miss it, fix content before adding layers.

This document delivers: current-state findings, the proposed architecture, the 8-tool MCP
surface, the context-packet schema, the architecture-pack plan, the security model, the
phased plan, a TODO checklist, and the concrete files already scaffolded in `.context/`.

---

## 1. Current-state findings

Inspected at commit `cfa6e9e` (HEAD of `main` mirror).

### 1.1 What exists for agents today
- **No `AGENTS.md`, `CLAUDE.md`, `.cursorrules`, `.context/`, or committed MCP config** in
  the repo before this proposal. (This proposal adds them.)
- **`docs/llms.txt`** — a well-curated, external-facing LLM index of linera.dev, example
  apps, and the `linera-agents` skills repo. *Useful, external-only.* It points to a
  separate `linera-io/linera-agents` repo with `SKILL.md` files (linera-dev, linera-markets)
  and reference docs (cross-chain messaging, RocksDB locking, multi-wallet setup). Those
  skills target **external builders**, not internal codebase work.
- **`indxr`** generates an untracked `INDEX.md` (per #6216: 33 crates, 744 files, 218k LOC,
  ~38k lines) — a per-machine workspace/symbol map. Plus **Serena** for symbol lookups.
- **28× per-crate `README.md`** — variable depth, no cross-crate pointers.
- **`README.md`** has a good crate dependency table; **`CONTRIBUTING.md`** has real,
  enforced conventions (naming, `let _ =` ban, hash-consed `linera_cache::Arc`, linear
  history, fmt/clippy gates).
- **Issue #6216** already proposes a `.kb/` agent KB + a stateless ripgrep-backed
  `linera-kb` MCP, with an explicit eval ship-gate and "no vectors/graph in v0.1". Issue
  **#6179** covers the external-docs side. **This proposal is the implementation arm of
  #6216, widened to the full `company-context` vision.**

### 1.2 Observability (real)
- ~**310 `register_*` metric call-sites**. Registration helpers in
  `linera-base/src/prometheus_util.rs`; per-subsystem statics (e.g.
  `linera-views/src/metrics.rs`). Exposed via `linera-metrics/src/monitoring_server.rs`
  on `:21100`.
- Stack: `docker/prometheus.yml` (jobs `proxy`, `shards`), `docker/alloy-config.river`
  (Grafana **Alloy** scrapes proxy+shards, forwards metrics/logs/traces via **OTel**),
  `docker/dashboards/linera-general.json` (single Grafana dashboard),
  `docker/docker-compose.yml` (proxy, shard-0..3, scylla, prometheus, grafana, watchtower,
  caddy). Real metric names: `execute_block_latency`, `inbox_size`, `cross_chain_batch_size`,
  `linera_proxy_request_latency`, `outbox_counters_size` (added #6440),
  `key_value_store_view_*_latency`, faucet metrics, etc.

### 1.3 Deployment (important)
- **No Kubernetes/Helm/Terraform in this repo.** `docs/operators.md` redirects to
  `docs.infra.linera.net`; charts/scripts live in **`linera-io/linera-artifacts`**. In-repo
  there's only Docker images, the compose stack, and a few k8s-aware helper scripts
  (`scripts/{deploy-validator,wait-for-kubernetes-service,backup-validator-keys}.sh`). The
  prod-state backend (Phase 4) must therefore target the **external repo + live cluster**,
  not this repo.

### 1.4 Useful / stale / missing / duplicated / verbose
- **Useful, keep:** `README.md` crate table, `CONTRIBUTING.md`, `docs/llms.txt`,
  `indxr`/`INDEX.md`, Serena, per-crate READMEs, the `linera-agents` skills.
- **Missing (the gap this fills):** internal architecture packs, cross-crate wiring,
  operational gotchas, a shared MCP, agent instructions (`AGENTS.md`/`CLAUDE.md`).
- **Duplicated risk:** per-user `CLAUDE.md` files (per #6216) — not shared. Consolidate the
  shared parts into the committed `CLAUDE.md`/`AGENTS.md`.
- **Stale risk:** `INDEX.md` is huge (~38k lines) and per-machine — **do not commit it
  raw**; reference it conceptually and let `indxr` regenerate locally.
- **Too verbose for an agent:** `CLI.md` (82KB) — fine as reference, never load wholesale;
  expose via `search_code`/docs, not in a packet.

### 1.5 Naming note (resolve before merge)
#6216 proposes `.kb/`; this proposal scaffolds `.context/` (the brief's requested name).
**These are the same thing — pick one.** Recommendation: agree on a single directory in the
#6216 thread (I lean `.context/` for discoverability, but `.kb/` is fine); it's a rename.
Everything here is structure-stable across that choice.

---

## 2. Proposed architecture

One **read-only** logical facade, many small scoped backends. The agent only ever talks to
the gateway.

```
   coding agent (carries OIDC identity)
        │  MCP (stdio/http)
        ▼
   ┌──────────────────────────────────────────────┐
   │  company-context gateway (stateless)          │
   │  identity+scope · token budget · audit log ·  │
   │  prompt-injection sanitize · secret/PII redact│
   └──┬──────┬───────┬───────┬─────────┬───────────┘
      │ scoped, short-lived, least-privilege tokens (1/backend)
   ┌──▼──┐ ┌─▼────┐ ┌▼─────┐ ┌▼─────┐ ┌▼────────────┐
   │linera│ │GitHub│ │Grafana│ │ripgrep│ │ prod-state │
   │-kb   │ │ meta │ │  MCP  │ │ + git │ │ (k8s/helm   │
   │(.ctx)│ │ (RO) │ │ (RO)  │ │       │ │  read-only) │
   └──────┘ └──────┘ └───────┘ └───────┘ └─────────────┘
```

Design rules:
1. **Read-only by default.** No backend has write scope. Writes (deploys, merges, edits)
   are a *separate future MCP* gated by human approval — never folded in here.
2. **Facade, not monolith.** No single process holds all credentials; each backend gets its
   own short-lived scoped token.
3. **Precision > volume.** Token-budgeted, cited responses; explicit truncation; staleness
   warnings. Imprecise retrieval is worse than none.
4. **OSS-first.** ripgrep, git, `indxr`, the GitHub API, Grafana MCP. No paid vector DB.
5. **Wrap don't expose.** Agents get `metrics_summary`, not raw PromQL; never raw `kubectl`.
6. **Where each tool fits:** `indxr`/`.context/` for navigation+invariants; ripgrep for
   exact lexical/symbol/error search; Semble (see §5) for fast relevant-snippet retrieval if
   the eval shows ripgrep+packs miss; Grafana MCP wrapped for telemetry; GitHub for history.

---

## 3. MCP tool surface (MVP — 8 read-only tools)

Full machine-readable schemas: [`mcp/company-context/tools.schema.json`](./mcp/company-context/tools.schema.json).
Summary:

| Tool | Purpose | Backends | Budget behaviour | Key security note |
|---|---|---|---|---|
| `context_packet` | Entry point: assemble a cited, budgeted ContextPacket for a task | fan-out + dedup | allocates budget across sections; drops lowest-priority first; sets `truncated` | metrics/logs/deploy require scope, else omitted w/ warning |
| `search_code` | Exact symbol/string/error search | ripgrep + `indxr` | 1 line/match; truncate match list | deny-list `.env`/keystore/wallet; no path escape |
| `get_architecture_context` | Curated subsystem pack + invariants | `.context/architecture` | prioritize entry-points/invariants | static content, no live access |
| `recent_changes` | PRs/issues/commits touching a path | GitHub + git log | metadata only; diffs need follow-up | bodies sanitized for injection |
| `find_tests_for` | Tests covering a path/symbol + run cmd | ripgrep + cargo meta | names+cmds, not bodies | repo-scoped RO |
| `search_prs` | Query PRs/issues for rationale/history | GitHub | title+1-line snippet | RO scope; sanitized bodies |
| `metrics_summary` | Summarize metric/area vs baseline | Grafana MCP (RO) | aggregates+verdict, no raw series | `obs:read` scope; server-templated PromQL only |
| `logs_summary` | Cluster logs into patterns+counts | Grafana Loki (RO) | pattern+count+1 redacted example | `obs:read`; PII/secret redactor on egress |

Worked example response: [`mcp/company-context/examples/context_packet.add-metric.json`](./mcp/company-context/examples/context_packet.add-metric.json).

Deliberately **not** first-class: `read_file`, `kubectl`, raw PromQL/LogQL, `git` write,
PR merge. `read_file` stays a host-tool primitive (agents already have it); the MCP's job is
*navigation and synthesis*, not re-exposing primitives.

---

## 4. Context packet format

Full JSON Schema: [`mcp/company-context/context-packet.schema.json`](./mcp/company-context/context-packet.schema.json).

Fields (all present in the schema): `schema_version`, `topic`, `summary`, `architecture[]`,
`code_spans[]` (path/line/symbol/why/commit), `changes[]` (PR/issue/commit metadata),
`tests[]` (with `run_cmd`), `observability{metrics,logs,deploy}` (opt-in, scoped),
`pitfalls[]`, `next_steps[]`, `citations[]` (≥1 required), `confidence{level,rationale}`,
`warnings[]` (`stale_pack`/`drift_detected`/`budget_truncated`/`backend_unavailable`/
`low_recall`/`ambiguous_topic`), `token_budget{requested,used,truncated}`.

Design intent: **navigation-first**. `code_spans` is the most valuable field — it prunes the
search space. Every non-obvious claim must carry a citation with the resolving `commit` so
agents can detect drift. An empty `citations` array means the packet is rejected.

---

## 5. Code-knowledge retrieval strategy

Explicit division of labour (and what must **not** go into vector search):

| Layer | Tool | Use for | Don't use for |
|---|---|---|---|
| Persistent codebase wiki / architecture memory | **`indxr`** (`INDEX.md`) + `.context/` packs | workspace map, symbol locations, invariants, "where does X live" | exact current contents (regenerate/read source) |
| Exact lexical / symbol / error-string | **ripgrep** (`search_code`) | error strings, symbol defs/uses, config keys | "explain how X works" (no semantics) |
| Fast relevant-snippet retrieval | **Semble** (or similar) — *only if eval shows ripgrep+packs miss* | "find the code that does this fuzzy thing" when keyword search fails | as the *default*; #6216's evidence is that agentic keyword search beats RAG here |
| Change/design history | **GitHub metadata** (`recent_changes`/`search_prs`) | "what changed and why", design rationale | code understanding |
| Vector DB | **deferred** | nothing in v0.x | **never:** source code, READMEs, `INDEX.md`, mdbook chapters at this corpus size |

**What must NOT go into vector search (firm):** source code, the auto-generated `INDEX.md`,
per-crate READMEs, and mdbook chapters. Rationale (from #6216): the corpus is ~30 crates /
~80 chapters; ripgrep over a curated structure has higher precision; embeddings go stale on
every edit; Claude Code's own team found agentic search beat RAG "not narrowly," and Amazon
Science (Feb 2026) corroborates >90% of RAG performance via keyword tool use with no vector
DB. **Where Semble *would* help:** as a *fallback* relevance ranker for natural-language code
queries that keyword search fans out poorly on — gated behind eval evidence, not the default.

**Where `indxr` is enough:** "what crates exist / where is symbol S / file summary." Don't
build anything new for those — point agents at `indxr`/Serena via `search_code`.

---

## 6. Architecture-pack plan

Scaffolded in [`architecture/`](./architecture/) with the pack contract in
[`architecture/_index.md`](./architecture/_index.md). Each pack: Purpose · Entry points ·
How it works · Invariants & gotchas · Related · How agents use it · Freshness (drift-detector
inputs + `verified_commit`/`verified_at`). All grounded in real paths.

| Pack | Source files to inspect | Status |
|---|---|---|
| [`validator.md`](./architecture/validator.md) | `proxy/main.rs`, `server.rs`, `core/worker.rs`, `linera-rpc/config.rs` | drafted |
| [`proxy.md`](./architecture/proxy.md) | `service/src/proxy/{main,grpc}.rs`, `rpc/config.rs` | drafted |
| [`shard-workers.md`](./architecture/shard-workers.md) | `core/worker.rs`, `core/chain_worker/{mod,state,handle,config,delivery_notifier}.rs` | drafted |
| [`cross-chain-messaging.md`](./architecture/cross-chain-messaging.md) | `chain/{inbox,outbox}.rs`, `rpc/cross_chain_message_queue.rs` | drafted |
| [`scylla-storage.md`](./architecture/scylla-storage.md) | `views/backends/{scylla_db,rocks_db,metering,lru_caching,dual}.rs`, `views/store.rs`, `storage/db_storage.rs` | drafted |
| [`benchmarking.md`](./architecture/benchmarking.md) | `service/src/benchmark.rs`, `client/src/benchmark.rs`, `**/benches/*.rs` | drafted |
| [`deployment-gcp-k8s.md`](./architecture/deployment-gcp-k8s.md) | `docker/*`, workflows, **external `linera-artifacts`** | drafted |
| [`observability.md`](./architecture/observability.md) | `base/prometheus_util.rs`, `metrics/*.rs`, `views/metrics.rs`, `docker/{prometheus.yml,alloy-config.river,dashboards}` | drafted |

Pitfalls: [`known-pitfalls/`](./known-pitfalls/) (performance, storage, async-concurrency,
cross-chain). Conventions: [`conventions/`](./conventions/) (rust-style, metrics, testing),
distilled from `CONTRIBUTING.md`.

**How agents use packs:** `get_architecture_context(area)` for orientation, then jump to the
cited entry-point files. Packs are pointers, not paraphrase. **Freshness:** PR-time drift
detector flags a pack when any path in its Freshness block is touched; the PR author
confirms/updates. Humans confirm; agents don't silently rewrite the shared map. Delete fixed
pitfalls.

---

## 7. Observability integration (Grafana MCP, wrapped)

Use the OSS **Grafana MCP** as the backend, but expose only higher-level wrappers so agents
never hand-write PromQL/LogQL and never reach edit/admin endpoints.

| Wrapper tool | What it gives agents | Built on Grafana MCP |
|---|---|---|
| `metrics_summary(metric|area, env, window)` | p50/p95/p99 + baseline delta + trend verdict | server-templated PromQL over Prometheus |
| `logs_summary(service, level, window)` | clustered log patterns + counts + 1 redacted example | LogQL over Loki |
| `alert_context(alert|area)` *(Phase 3.1)* | firing/recent alerts + linked dashboard + owning code area | Grafana alerting API (RO) |
| `dashboard_summary(uid|area)` *(Phase 3.1)* | which panels/metrics exist for an area (maps to `linera-general.json`) | Grafana dashboards API (RO) |
| `incident_context(window|area)` *(Phase 3.2)* | perf-regression / incident bundle: anomalous metrics + log pattern diff + recent deploys + suspect PRs | metrics+logs+`recent_changes`+prod-state |
| code↔metric mapping | each metric carries `defined_at` (source file), resolved from the metric catalog (grep of the ~310 `register_*` sites) | `search_code` + `.context` catalog |

Rules: **read-only Grafana token, environment-scoped**; no dashboard edits, no alert
mutation, no arbitrary queries (templated only); **secret/PII redactor** on all log egress
(keys, addresses, tokens). Code↔metric mapping is feasible and cheap here because metric
names are literal strings in source.

---

## 8. Read-only production-state integration (Phase 4)

Target = the live GCP/Kubernetes cluster + `linera-io/linera-artifacts` (charts), **not** this
repo. Expose only a read-only snapshot surface, surfaced through the `deploy` section of
`context_packet`/a `deploy_state` tool.

| Allowed (read-only) | Forbidden (hard deny) |
|---|---|
| Pod/Deployment/StatefulSet status, replicas, ready/restarts | `kubectl exec`, `apply`, `delete`, `scale`, `port-forward` |
| Helm release name/version/status, chart version | any Helm install/upgrade/rollback |
| Image versions / tags per service | **reading `Secret` objects** (k8s Secrets) — never |
| Resource requests/limits | **Terraform state** (contains secrets) — never |
| Recent deploy events / rollout history | ConfigMap values containing credentials (redact) |
| Non-secret config diffs (image/replica/limits) | cloud IAM/credential material |
| Terraform/GCP **inventory** (resource list/types, no state file) | provider blocks, backend state, `*.tfstate` |

Security boundaries: a dedicated **read-only ServiceAccount/RBAC** with `get`/`list` on a
namespace allow-list and **explicit `secrets` exclusion**; a separate read-only GCP role
(viewer-scoped, no secret-manager access). Terraform: read **inventory via the cloud API**,
never the state file. Every field passes the secret/PII redactor. This phase ships last and
behind the strictest review.

---

## 9. Security & permission model

1. **Read-only by default.** Every backend token is read-scoped. No write path in this MCP.
2. **Scoped tokens, least privilege.** One short-lived token per backend
   (`repo:read`, `obs:read`, `k8s:read-nosecrets`, `gcp:viewer`). The gateway holds no
   long-lived secrets; tokens are minted per-session/short-TTL.
3. **Per-user identity.** The gateway requires the caller's OIDC subject; backend calls carry
   it (or a delegated short-lived token) so actions are attributable.
4. **Audit logging.** Every call logs `{ts, subject, tool, args_hash, backend, tokens_used,
   latency_ms, redactions, result_truncated}`. Append-only, queryable.
5. **No secrets exposure.** Hard deny-list: `.env*`, `*keystore*`, `wallet*.json`, k8s
   `Secret`, `*.tfstate`, GCP secret-manager. An egress redactor strips keys/addresses/tokens
   from logs and configs.
6. **Prompt-injection resistance.** PR/issue bodies, log lines, and config text are
   *untrusted external data*. The gateway sanitizes/encapsulates them (delimited, never
   interpreted as instructions), and the agent guidance (`AGENTS.md`) says to treat retrieved
   text as data. Tool inputs to Grafana/k8s are **server-templated**, never free-form from the
   agent.
7. **No raw k8s Secrets / no Terraform state secrets / no prod writes** — enumerated in §8.
8. **Separate future write MCP only with human approval.** Any deploy/merge/edit capability is
   a *different* server, off by default, with human-in-the-loop confirmation and its own audit
   trail. Not in scope here.

---

## 10. Implementation plan (phases)

### Phase 0 — Inventory & cleanup
- **Deliverables:** this `.context/` scaffold; `AGENTS.md`/`CLAUDE.md`; resolve the
  `.context/` vs `.kb/` naming in #6216; decide MCP-server home (per #6216 Q2); confirm
  `INDEX.md` stays untracked/local.
- **Difficulty:** low. **Risk:** bikeshedding on naming/home.
- **Acceptance:** packs render; `AGENTS.md`/`CLAUDE.md` merged; #6216 naming decided.
- **Test tasks:** eval tasks #3, #4 (architecture orientation) run OFF to capture baseline.

### Phase 1 — Code/docs/GitHub context MCP
- **Deliverables:** `company-context` gateway + `search_code`, `get_architecture_context`,
  `recent_changes`, `find_tests_for`, `search_prs`, and a `context_packet` that composes them
  (no obs/prod yet). Stateless, ripgrep+git+GitHub-API backed. PR-time drift detector.
- **Difficulty:** medium. **Risk:** packet too verbose (mitigate w/ budget); MCP sits idle
  without the `CLAUDE.md` hint (mitigated, §AGENTS).
- **Acceptance:** ship-gate met on the code/history subset (eval #1–5,7,8,10).
- **Test tasks:** eval #1, #7, #8, #10.

### Phase 2 — Architecture packs (content depth)
- **Deliverables:** flesh remaining packs to full depth; drift detector wired into CI;
  pitfalls/conventions complete.
- **Difficulty:** low-medium (curation effort, ~2–3 focused days per #6216).
- **Risk:** drift/staleness; reviewer load from drift pings (#6216 Q6).
- **Acceptance:** every subsystem has a verified pack; drift detector green.
- **Test tasks:** eval #3, #4, #5.

### Phase 3 — Grafana observability integration
- **Deliverables:** `metrics_summary`, `logs_summary` (3.0); `alert_context`,
  `dashboard_summary` (3.1); `incident_context` (3.2). Read-only env-scoped Grafana token;
  templated queries; egress redactor; metric→source catalog.
- **Difficulty:** medium. **Risk:** query cost; cardinality; leaking secrets in logs
  (mitigated by redactor + templating).
- **Acceptance:** eval #6 passes; no raw query path exposed; redaction verified on a seeded
  secret.
- **Test tasks:** eval #6.

### Phase 4 — Read-only production-state integration
- **Deliverables:** `deploy_state` / `context_packet.deploy`; read-only k8s SA (no secrets),
  GCP viewer; Helm/image/replica/limits/rollout reads; non-secret config diff; TF inventory.
- **Difficulty:** medium-high (touches prod). **Risk:** RBAC misconfig exposing secrets;
  cluster read load.
- **Acceptance:** eval #9 passes; pen-check confirms Secrets/`tfstate` unreachable; audit log
  shows every read.
- **Test tasks:** eval #9.

### Phase 5 — Evaluation & iteration
- **Deliverables:** run the full 10-task harness ON vs OFF; publish numbers; decide on
  Semble/vector fallback *only if* recall gaps appear.
- **Difficulty:** low. **Risk:** failing the gate (then: fix content, not add layers).
- **Acceptance:** ≥20% token reduction on ≥6/10, no correctness regression.
- **Test tasks:** all 10 (`.context/eval/tasks.md`).

---

## 11. Concrete TODO checklist

- [x] `.context/` scaffold (architecture, known-pitfalls, conventions, mcp, eval).
- [x] `context-packet.schema.json` + worked example.
- [x] `company-context` `tools.schema.json` (8 tools) + README.
- [x] 8 architecture packs (drafted, real paths) + pack contract.
- [x] 4 pitfalls packs + 3 conventions packs.
- [x] `AGENTS.md` + `CLAUDE.md` with retrieval hint.
- [x] Eval harness README + 10 tasks + ship-gate.
- [ ] **Decide `.context/` vs `.kb/` naming and MCP-server home** (#6216 Q1/Q2).
- [ ] Capture OFF-baseline token numbers for the 10 tasks.
- [ ] Implement Phase 1 gateway + 5 code/history tools (stateless, ripgrep/git/GitHub).
- [ ] PR-time drift detector (flags packs whose Freshness paths changed).
- [ ] Phase 3: Grafana MCP wrappers + egress redactor + metric→source catalog.
- [ ] Phase 4: read-only k8s SA / GCP viewer + `deploy_state`.
- [ ] Run full eval; publish; decide on Semble/vector fallback.

---

## 12. Opinionated recommendations

- **Build first:** `.context/` packs + Phase-1 code/docs/GitHub MCP. Biggest win/lowest risk.
- **Postpone:** Grafana wrappers (Phase 3) until packs prove out; prod-state (Phase 4) last.
- **Do not build:** a vector DB or knowledge graph (no evidence we need them at this corpus
  size); any write path in `company-context`; a single all-credentials server.
- **Keep:** `indxr`/`INDEX.md` (local), Serena, `docs/llms.txt`, `linera-agents` skills,
  `README` crate table, `CONTRIBUTING.md`, per-crate READMEs.
- **Consolidate/clean:** fold shared per-user `CLAUDE.md` guidance into the committed
  `CLAUDE.md`/`AGENTS.md`; **don't commit raw `INDEX.md`** (huge, per-machine); never load
  `CLI.md` (82KB) wholesale into context.
- **`indxr` is enough for:** crate/symbol/file-location questions — don't reinvent them.
- **Semble would help** only as a fallback relevance ranker for fuzzy NL code queries that
  ripgrep fans out poorly — gate on eval evidence.
- **Grafana MCP must be wrapped, never exposed directly:** agents get `metrics_summary`/
  `logs_summary`/`alert_context`, not raw PromQL/LogQL or Grafana edit/admin.
- **Above all:** honour the #6216 ship-gate. If a layer doesn't move the token/correctness
  numbers, don't ship it — fix the content first. Imprecise context is worse than none.
