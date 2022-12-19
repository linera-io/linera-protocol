// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(tonic_build::configure()
        .type_attribute("rpc.v1.ChainId", "#[derive(Hash, Eq)]")
        .compile(&["proto/rpc.proto"], &["proto"])?)
}
