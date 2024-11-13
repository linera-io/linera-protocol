[package]
name = "linera-core"
description = "The core Linera protocol, including client and server logic, node synchronization, etc."
readme = "README.md"
documentation = "https://docs.rs/linera-core/latest/linera_core/"

authors.workspace = true
edition.workspace = true
homepage.workspace = true
license.workspace = true
repository.workspace = true
version.workspace = true

[features]
wasmer = ["linera-execution/wasmer", "linera-storage/wasmer"]
wasmtime = ["linera-execution/wasmtime", "linera-storage/wasmtime"]
test = [
    "anyhow",
    "linera-base/test",
    "linera-chain/test",
    "linera-execution/test",
    "linera-storage/test",
    "linera-storage-service/test",
    "linera-views/test",
    "proptest",
    "test-log",
    "test-strategy",
    "tokio/parking_lot",
]
rocksdb = ["linera-views/rocksdb"]
dynamodb = ["linera-views/dynamodb"]
scylladb = ["linera-views/scylladb"]
storage-service = ["linera-storage-service"]
unstable-oracles = []
metrics = [
    "prometheus",
    "linera-base/metrics",
    "linera-chain/metrics",
    "linera-execution/metrics",
    "linera-storage/metrics",
    "linera-views/metrics",
]
web = [
    "linera-base/web",
    "linera-chain/web",
    "linera-execution/web",
    "linera-storage/web",
    "linera-views/web",
    "wasm-bindgen-futures",
]

[dependencies]
anyhow = { workspace = true, optional = true }
async-graphql.workspace = true
async-trait.workspace = true
bcs.workspace = true
clap.workspace = true
custom_debug_derive.workspace = true
dashmap.workspace = true
futures.workspace = true
linera-base.workspace = true
linera-chain.workspace = true
linera-execution.workspace = true
linera-storage.workspace = true
linera-version.workspace = true
linera-views.workspace = true
lru.workspace = true
prometheus = { workspace = true, optional = true }
proptest = { workspace = true, optional = true }
rand = { workspace = true, features = ["std_rng"] }
serde.workspace = true
serde_json.workspace = true
test-log = { workspace = true, optional = true }
test-strategy = { workspace = true, optional = true }
thiserror.workspace = true
tokio.workspace = true
tokio-stream.workspace = true
tonic.workspace = true
tracing.workspace = true
trait-variant.workspace = true
wasm-bindgen-futures = { workspace = true, optional = true }

[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
linera-storage-service = { workspace = true, optional = true }

[build-dependencies]
cfg_aliases.workspace = true

[dev-dependencies]
assert_matches.workspace = true
counter.workspace = true
criterion.workspace = true
fungible.workspace = true
linera-core = { path = ".", default-features = false, features = ["test"] }
linera-views.workspace = true
meta-counter.workspace = true
serde_json.workspace = true
social.workspace = true
test-case.workspace = true
tracing-subscriber = { workspace = true, features = ["fmt"] }

[target.'cfg(not(target_arch = "wasm32"))'.dev-dependencies]
criterion = { workspace = true, default-features = true, features = ["async_tokio"] }

[package.metadata.cargo-machete]
ignored = ["async-graphql", "proptest"]

[[bench]]
name = "client_benchmarks"
harness = false
required-features = ["test"]
