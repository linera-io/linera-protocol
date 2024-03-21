// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    cfg_aliases::cfg_aliases! {
        with_testing: { any(test, feature = "test") },
    };
    let no_includes: &[&str] = &[];
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["proto/key_value_store.proto"], no_includes)?;
    Ok(())
}
