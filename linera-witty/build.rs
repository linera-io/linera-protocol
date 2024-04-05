// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
        with_log: { feature = "log" },
        with_testing: { any(test, feature = "test") },
        with_wasmer: { feature = "wasmer" },
        with_wasmtime: { feature = "wasmtime" },
        with_macros: { feature = "macros" },
        with_wit_export: {
            all(
                feature = "macros",
                any(feature = "test", feature = "wasmer", feature = "wasmtime")
            )
        },
    }
}
