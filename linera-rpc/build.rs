// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir: std::path::PathBuf = std::env::var("OUT_DIR")?.into();

    let no_includes: &[&str] = &[];
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("file_descriptor_set.bin"))
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/rpc.proto"], no_includes)?;

    cfg_aliases::cfg_aliases! {
        with_testing: { any(test, feature = "test") },
        web: { all(target_arch = "wasm32", target_os = "unknown") },
        with_metrics: { all(not(web), feature = "metrics") },
        with_server: { all(not(web), feature = "server") },
        with_simple_network: { all(not(web), feature = "simple-network") },
    };

    Ok(())
}
