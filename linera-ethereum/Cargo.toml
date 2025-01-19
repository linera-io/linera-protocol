[package]
name = "linera-ethereum"
description = "Oracle functionality for Ethereum"
readme = "README.md"
documentation = "https://docs.rs/linera-ethereum/latest/linera_ethereum/"

authors.workspace = true
edition.workspace = true
homepage.workspace = true
license.workspace = true
repository.workspace = true
version.workspace = true

[package.metadata.docs.rs]
features = ["ethereum"]

[features]
ethereum = []

[dependencies]
anyhow.workspace = true
async-lock.workspace = true
async-trait.workspace = true
linera-base.workspace = true
num-bigint.workspace = true
num-traits.workspace = true
serde.workspace = true
serde_json = { workspace = true, features = ["raw_value"] }
thiserror.workspace = true

[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
alloy = { workspace = true, default-features = false, features = [ "rpc-types-eth", "provider-http", "json-rpc", "node-bindings", "network", "signers", "contract" ] }
alloy-signer-local = { workspace = true }
url.workspace = true
tokio = { workspace = true, features = ["full"] }

[target.'cfg(target_arch = "wasm32")'.dependencies]
alloy = { workspace = true, default-features = false, features = [ "rpc-types-eth" ] }

[build-dependencies]
cfg_aliases.workspace = true
