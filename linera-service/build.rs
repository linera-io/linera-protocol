// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    cfg_aliases::cfg_aliases! {
        with_revm: { feature = "revm" },
        with_testing: { any(test, feature = "test") },
        with_metrics: { all(not(target_arch = "wasm32"), feature = "metrics") },
    };

    tonic_build::compile_protos("src/exporter/proto/indexer.proto")?;

    Ok(())
}
