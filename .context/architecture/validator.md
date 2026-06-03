# Validator

## Purpose
A Linera validator is the public node operators run. It is two binaries: a **proxy**
(public front door) and one or more **servers** (each hosting sharded chain workers),
backed by a shared key-value **store** (RocksDB for dev, ScyllaDB for prod). Read this
pack first when a task says "validator", "node", or spans proxy↔server↔storage.

## Entry points
- `linera-service/src/proxy/main.rs` — `linera-proxy` binary, `ProxyOptions`.
- `linera-service/src/server.rs` — `linera-server` binary, `ServerContext`.
- `linera-core/src/worker.rs` — `WorkerState<S>`, the per-shard state machine.
- `linera-rpc/` — wire types for client↔proxy↔server↔server cross-chain RPC.
- `configuration/compose/validator.toml` — example validator config.
- `docs/developers/advanced_topics/validators.md` — canonical conceptual doc.

## How it works
Client → `linera-proxy` (routes by chain id) → the `linera-server` owning that chain's
shard → `WorkerState` executes/validates blocks and certificates → reads/writes the
store via `linera-storage`/`linera-views`. Servers also exchange **cross-chain
requests** with each other to deliver messages between chains (see
`cross-chain-messaging.md`). Metrics are exposed per-process on `:21100`
(see `observability.md`).

## Invariants & gotchas
- The proxy is **stateless routing**; correctness/consensus lives in the worker, not the
  proxy. Don't add consensus logic to the proxy.
- Chains are sharded across workers by chain id; a single chain is owned by exactly one
  shard at a time. Cross-chain work crosses shard/server boundaries via RPC, not shared
  memory.
- Storage backend is chosen at compile time via Cargo features (`rocksdb`, `scylladb`,
  `storage-service`). Behaviour can differ between backends — see `scylla-storage.md`.

## Related
- Packs: `proxy.md`, `shard-workers.md`, `cross-chain-messaging.md`, `scylla-storage.md`.
- Docs: linera.dev → Advanced Topics → Validators.

## How agents should use this
`get_architecture_context(area="validator")` for orientation, then jump to the specific
sub-pack. For "where does X live in a validator", prefer this pack's entry points over
grepping the whole tree.

## Freshness
- Depends on: `linera-service/src/proxy/main.rs`, `linera-service/src/server.rs`,
  `linera-core/src/worker.rs`, `linera-rpc/src/config.rs`.
- verified_commit: `cfa6e9e`
- verified_at: 2026-06-03
