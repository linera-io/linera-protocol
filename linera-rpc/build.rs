// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let no_includes: &[&str] = &[];
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["proto/rpc.proto"], no_includes)?;

    cfg_aliases::cfg_aliases! {
        web: { all(target_arch = "wasm32", target_os = "unknown") },
        with_metrics: { all(not(web), feature = "metrics") },
        with_simple_network: { all(not(web), feature = "simple_network") },
        with_tcp: { all(not(web), feature = "tcp") },
        with_udp: { all(not(web), feature = "udp") },
    };

    Ok(())
}
