// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

fn main() {
    cfg_aliases::cfg_aliases! {
        with_testing: { any(test, feature = "test") },
        with_wasm_runtime: { any(feature = "wasmer", feature = "wasmtime") },
        with_integration_testing: {
            all(not(target_arch = "wasm32"), with_testing, with_wasm_runtime)
        },
    };
}
