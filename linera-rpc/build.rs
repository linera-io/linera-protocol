// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(tonic_build::compile_protos("proto/rpc.proto")?)
}
