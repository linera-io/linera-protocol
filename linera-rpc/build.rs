// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let no_includes: &[&str] = &[];
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["proto/rpc.proto"], no_includes)?;

    cfg_aliases::cfg_aliases! {
        with_metrics: { all(not(target_arch = "wasm32"), feature = "metrics") },
    };

    Ok(())
}
