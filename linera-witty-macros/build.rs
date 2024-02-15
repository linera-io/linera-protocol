// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
        with_testing: { feature = "test" },
        with_wasmer: { feature = "wasmer" },
        with_wasmtime: { feature = "wasmtime" },
        with_wit_export: {
            any(feature = "test", feature = "wasmer", feature = "wasmtime")
        },
    }
}
